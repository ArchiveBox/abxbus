defmodule AbxBus.EventWorker do
  @moduledoc """
  Per-event handler execution process.

  Each event dispatch spawns one EventWorker (a bare process, not a GenServer).
  The worker:

  1. Sets up handler context in the process dictionary (current event ID,
     current bus name — used for automatic parent tracking in nested emits)
  2. Resolves effective concurrency/completion/timeout config
  3. Runs handlers (serial or parallel via Tasks)
  4. Enforces handler-level and event-level timeouts
  5. Sends results back to BusServer

  ## Handler context (process dictionary)

  When a handler calls `AbxBus.emit(bus, child_event)`, the child's
  `event_parent_id` is automatically set to `Process.get(:abx_current_event_id)`.
  This replaces Python's ContextVar-based parent tracking.

  When a handler calls `AbxBus.current_bus!()`, it reads
  `Process.get(:abx_current_bus)` — replacing Python's `event.bus` property.

  ## Queue-jump flow

  When a handler does `AbxBus.await(child)`:

  1. Handler process sends `:jump_queue` to each bus in `child.event_path`
  2. Handler blocks on `receive` waiting for `{:event_completed, ref, event}`
  3. BusServer removes child from queue and spawns a new EventWorker for it
  4. That worker runs the child's handlers and notifies completion
  5. EventStore sends completion message back to the waiting handler process
  6. Handler unblocks and continues

  The parent handler's process is blocked in `receive` — it holds NO locks,
  NO GenServer state. The child processes independently in its own worker.
  """

  require Logger

  alias AbxBus.{EventStore, LockManager, HandlerResult}

  @doc """
  Main entry point — runs in a spawned process.
  Executes all handlers for the event, respecting concurrency and timeout config.
  """
  def run(event, handlers, bus_config, bus_pid, bus_name) do
    # Set handler context in process dictionary
    Process.put(:abx_current_event_id, event.event_id)
    Process.put(:abx_current_bus, bus_name)
    Process.put(:abx_current_bus_pid, bus_pid)

    # Notify bus that processing has started
    send(bus_pid, {:event_worker_started, event.event_id})

    # Resolve effective config
    handler_concurrency = LockManager.resolve_handler_concurrency(event, bus_config)
    handler_completion = LockManager.resolve_handler_completion(event, bus_config)
    event_timeout = LockManager.resolve_event_timeout(event, bus_config)

    # Create handler result placeholders
    results =
      handlers
      |> Enum.map(fn entry ->
        {entry.id, HandlerResult.new(entry.id,
          handler_name: entry.handler_name,
          timeout: LockManager.resolve_handler_timeout(entry, event, bus_config),
          handler_registered_at: entry.registered_at,
          eventbus_name: bus_name
        )}
      end)
      |> Map.new()

    # Store initial results on event
    EventStore.update(event.event_id, %{event_results: results})

    # Store event_id for result tracking during timeout
    Process.put(:abx_event_results_key, nil)

    # Execute with event-level timeout
    results =
      maybe_with_event_timeout(event_timeout, fn ->
        result =
          case handler_concurrency do
            :parallel ->
              run_handlers_parallel(event, handlers, results, handler_completion, bus_config, bus_name, bus_pid)

            :serial ->
              run_handlers_serial(event, handlers, results, handler_completion, bus_config, bus_name, bus_pid)
          end

        result
      end, results)

    # Send results back to bus
    send(bus_pid, {:event_worker_done, event.event_id, results})
  end

  # ── Parallel handler execution ─────────────────────────────────────────────

  defp run_handlers_parallel(event, handlers, results, completion_mode, bus_config, bus_name, bus_pid) do
    # Capture context values BEFORE spawning tasks
    current_event_id = event.event_id

    # Spawn a task per handler
    tasks =
      Enum.map(handlers, fn entry ->
        task =
          Task.async(fn ->
            # Propagate handler context to child task
            Process.put(:abx_current_event_id, current_event_id)
            Process.put(:abx_current_bus, bus_name)
            Process.put(:abx_current_bus_pid, bus_pid)
            Process.put(:abx_current_handler_id, entry.id)

            run_single_handler(entry, event, bus_config)
          end)

        {entry.id, task}
      end)

    case completion_mode do
      :all ->
        # Wait for all tasks
        Enum.reduce(tasks, results, fn {handler_id, task}, acc ->
          result_entry = Map.get(acc, handler_id) |> HandlerResult.mark_started()

          case Task.yield(task, :infinity) do
            {:ok, {:ok, value}} ->
              Map.put(acc, handler_id, HandlerResult.mark_completed(result_entry, value))

            {:ok, {:error, error}} ->
              Map.put(acc, handler_id, HandlerResult.mark_error(result_entry, error))

            {:exit, reason} ->
              Map.put(acc, handler_id, HandlerResult.mark_error(result_entry, reason))

            nil ->
              Task.shutdown(task, :brutal_kill)
              Map.put(acc, handler_id, HandlerResult.mark_aborted(result_entry))
          end
        end)

      :first ->
        # Wait for first non-nil, non-event result using Task.yield_many
        await_first_parallel(tasks, results)
    end
  end

  defp await_first_parallel(tasks, results) do
    # Mark all as started
    results =
      Enum.reduce(tasks, results, fn {handler_id, _task}, acc ->
        Map.update!(acc, handler_id, &HandlerResult.mark_started/1)
      end)

    # Use a spawned collector to race tasks properly
    task_map = Map.new(tasks, fn {handler_id, task} -> {task.ref, handler_id} end)
    all_tasks = Enum.map(tasks, fn {_, task} -> task end)

    # Yield with a short timeout repeatedly, checking for first valid result
    do_await_first(all_tasks, task_map, results, tasks)
  end

  defp do_await_first([], _task_map, results, _original_tasks), do: results

  defp do_await_first(remaining_tasks, task_map, results, original_tasks) do
    # Yield on all remaining tasks with a short timeout
    yielded = Task.yield_many(remaining_tasks, 1)

    {results, found_first, still_remaining} =
      Enum.reduce(yielded, {results, nil, []}, fn {task, result}, {acc, first, rem} ->
        handler_id = Map.get(task_map, task.ref)

        case result do
          {:ok, {:ok, value}} when not is_nil(value) and is_nil(first) ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_completed(Map.get(acc, handler_id), value))
            {acc, value, rem}

          {:ok, {:ok, value}} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_completed(Map.get(acc, handler_id), value))
            {acc, first, rem}

          {:ok, {:error, error}} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_error(Map.get(acc, handler_id), error))
            {acc, first, rem}

          {:exit, reason} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_error(Map.get(acc, handler_id), reason))
            {acc, first, rem}

          nil ->
            # Not done yet
            {acc, first, [task | rem]}
        end
      end)

    if found_first != nil do
      # Cancel remaining tasks
      for task <- still_remaining do
        Task.shutdown(task, :brutal_kill)
        handler_id = Map.get(task_map, task.ref)
        if handler_id do
          results = Map.put(results, handler_id, HandlerResult.mark_cancelled(Map.get(results, handler_id)))
        end
      end
      results
    else
      if still_remaining == [] do
        results
      else
        do_await_first(still_remaining, task_map, results, original_tasks)
      end
    end
  end

  # ── Serial handler execution ───────────────────────────────────────────────

  defp run_handlers_serial(event, handlers, results, completion_mode, bus_config, bus_name, bus_pid) do
    {final_results, _done} =
      Enum.reduce_while(handlers, {results, false}, fn entry, {acc, _} ->
        # Set handler context for serial execution (we're in the worker process)
        Process.put(:abx_current_event_id, event.event_id)
        Process.put(:abx_current_bus, bus_name)
        Process.put(:abx_current_bus_pid, bus_pid)
        Process.put(:abx_current_handler_id, entry.id)

        result_entry = Map.get(acc, entry.id) |> HandlerResult.mark_started()

        case run_single_handler(entry, event, bus_config) do
          {:ok, value} ->
            updated = HandlerResult.mark_completed(result_entry, value)
            acc = Map.put(acc, entry.id, updated)

            # Update shared ETS for timeout monitoring
            update_shared_results(acc)

            case completion_mode do
              :first when not is_nil(value) ->
                acc = cancel_remaining_serial(handlers, entry.id, acc)
                {:halt, {acc, true}}

              _ ->
                {:cont, {acc, false}}
            end

          {:error, error} ->
            updated = HandlerResult.mark_error(result_entry, error)
            acc = Map.put(acc, entry.id, updated)
            update_shared_results(acc)
            {:cont, {acc, false}}
        end
      end)

    final_results
  end

  defp update_shared_results(results) do
    # Update the shared ETS entry so the timeout monitor can see partial progress
    case Process.get(:abx_event_results_key) do
      nil -> :ok
      key ->
        case :ets.info(:abx_worker_results) do
          :undefined -> :ok
          _ -> :ets.insert(:abx_worker_results, {key, results})
        end
    end
  end

  defp cancel_remaining_serial(handlers, completed_id, results) do
    handlers
    |> Enum.drop_while(fn e -> e.id != completed_id end)
    |> Enum.drop(1)
    |> Enum.reduce(results, fn entry, acc ->
      case Map.get(acc, entry.id) do
        nil -> acc
        result -> Map.put(acc, entry.id, HandlerResult.mark_cancelled(result))
      end
    end)
  end

  # ── Single handler execution ───────────────────────────────────────────────

  defp run_single_handler(entry, event, bus_config) do
    # Resolve handler-level timeout (tightest wins)
    timeout = LockManager.resolve_handler_timeout(entry, event, bus_config)

    # Apply semaphore if configured
    maybe_with_semaphore(entry, fn ->
      maybe_with_handler_timeout(timeout, fn ->
        # Retry loop
        run_with_retries(entry, event, entry.max_attempts)
      end)
    end)
  end

  defp run_with_retries(entry, event, attempts_left) do
    try do
      value = entry.handler_fn.(event)
      {:ok, value}
    rescue
      e ->
        if attempts_left > 1 do
          run_with_retries(entry, event, attempts_left - 1)
        else
          {:error, e}
        end
    catch
      :exit, reason ->
        if attempts_left > 1 do
          run_with_retries(entry, event, attempts_left - 1)
        else
          {:error, reason}
        end
    end
  end

  # ── Timeout enforcement ────────────────────────────────────────────────────

  defp maybe_with_event_timeout(nil, fun, _results), do: fun.()

  defp maybe_with_event_timeout(timeout_s, fun, results) do
    timeout_ms = trunc(timeout_s * 1000)
    caller = self()

    # Capture process dictionary values before spawning
    event_id = Process.get(:abx_current_event_id)
    bus_name = Process.get(:abx_current_bus)
    bus_pid = Process.get(:abx_current_bus_pid)

    # Use an ETS table to share partial results between worker and timeout monitor
    results_ref = :erlang.make_ref()
    results_key = {__MODULE__, results_ref}

    case :ets.info(:abx_worker_results) do
      :undefined -> :ets.new(:abx_worker_results, [:set, :public, :named_table])
      _ -> :ok
    end

    :ets.insert(:abx_worker_results, {results_key, results})

    pid = spawn_link(fn ->
      Process.put(:abx_current_event_id, event_id)
      Process.put(:abx_current_bus, bus_name)
      Process.put(:abx_current_bus_pid, bus_pid)
      Process.put(:abx_event_results_key, results_key)

      result = fun.()

      # Store final results for the parent
      :ets.insert(:abx_worker_results, {results_key, result})
      send(caller, {:event_timeout_result, result})
    end)

    # Trap exit so spawn_link doesn't kill us when we kill the child
    Process.flag(:trap_exit, true)

    result =
      receive do
        {:event_timeout_result, result} ->
          result
      after
        timeout_ms ->
          Process.exit(pid, :kill)
          receive do
            {:EXIT, ^pid, _} -> :ok
          after
            10 -> :ok
          end

          # Read the latest partial results from ETS
          latest =
            case :ets.lookup(:abx_worker_results, results_key) do
              [{_, r}] -> r
              [] -> results
            end

          # Mark all incomplete results as error
          Enum.reduce(latest, latest, fn {handler_id, result}, acc ->
            updated =
              case result.status do
                s when s in [:pending, :started] ->
                  HandlerResult.mark_error(result, %AbxBus.EventHandlerAbortedError{})
                _ ->
                  result
              end

            Map.put(acc, handler_id, updated)
          end)
      end

    # Drain any EXIT message from the linked child
    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end

    Process.flag(:trap_exit, false)
    :ets.delete(:abx_worker_results, results_key)
    result
  end

  defp maybe_with_handler_timeout(nil, fun), do: fun.()

  defp maybe_with_handler_timeout(timeout_s, fun) do
    timeout_ms = trunc(timeout_s * 1000)
    caller = self()

    # Capture context
    event_id = Process.get(:abx_current_event_id)
    bus_name = Process.get(:abx_current_bus)
    bus_pid = Process.get(:abx_current_bus_pid)

    pid = spawn(fn ->
      Process.put(:abx_current_event_id, event_id)
      Process.put(:abx_current_bus, bus_name)
      Process.put(:abx_current_bus_pid, bus_pid)

      result = fun.()
      send(caller, {:handler_timeout_result, result})
    end)

    receive do
      {:handler_timeout_result, result} ->
        result
    after
      timeout_ms ->
        Process.exit(pid, :kill)
        {:error, %AbxBus.EventHandlerTimeoutError{}}
    end
  end

  defp maybe_with_semaphore(%{semaphore_scope: :none}, fun), do: fun.()

  defp maybe_with_semaphore(%{semaphore_scope: scope, semaphore_name: name, semaphore_limit: limit}, fun)
       when scope in [:global, :bus] do
    sem_name = name || "default"
    LockManager.acquire_semaphore(sem_name, limit)

    try do
      fun.()
    after
      LockManager.release_semaphore(sem_name)
    end
  end

  defp maybe_with_semaphore(_, fun), do: fun.()
end

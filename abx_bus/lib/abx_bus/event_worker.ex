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

    # Execute with event-level timeout
    results =
      maybe_with_event_timeout(event_timeout, fn ->
        case handler_concurrency do
          :parallel ->
            run_handlers_parallel(event, handlers, results, handler_completion, bus_config)

          :serial ->
            run_handlers_serial(event, handlers, results, handler_completion, bus_config)
        end
      end, results)

    # Send results back to bus
    send(bus_pid, {:event_worker_done, event.event_id, results})
  end

  # ── Parallel handler execution ─────────────────────────────────────────────

  defp run_handlers_parallel(event, handlers, results, completion_mode, bus_config) do
    parent_pid = self()

    # Spawn a task per handler
    tasks =
      Enum.map(handlers, fn entry ->
        task =
          Task.async(fn ->
            # Propagate handler context to child task
            Process.put(:abx_current_event_id, event.event_id)
            Process.put(:abx_current_bus, Process.get(:abx_current_bus))
            Process.put(:abx_current_bus_pid, Process.get(:abx_current_bus_pid))
            Process.put(:abx_current_handler_id, entry.id)

            run_single_handler(entry, event, bus_config)
          end)

        {entry.id, task}
      end)

    case completion_mode do
      :all ->
        # Wait for all tasks
        collected =
          Enum.reduce(tasks, results, fn {handler_id, task}, acc ->
            result_entry = Map.get(acc, handler_id)
            result_entry = HandlerResult.mark_started(result_entry)

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

        collected

      :first ->
        # Wait for first non-nil, non-event result
        await_first_parallel(tasks, results)
    end
  end

  defp await_first_parallel(tasks, results) do
    # Mark all as started
    results =
      Enum.reduce(tasks, results, fn {handler_id, _task}, acc ->
        Map.update!(acc, handler_id, &HandlerResult.mark_started/1)
      end)

    # Collect results as they come in, stop on first valid result
    {final_results, _} =
      Enum.reduce_while(tasks, {results, nil}, fn {handler_id, task}, {acc, _first} ->
        case Task.yield(task, :infinity) do
          {:ok, {:ok, value}} when not is_nil(value) ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_completed(Map.get(acc, handler_id), value))
            # Cancel remaining tasks
            cancel_remaining_tasks(tasks, handler_id)
            {:halt, {acc, value}}

          {:ok, {:ok, nil}} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_completed(Map.get(acc, handler_id), nil))
            {:cont, {acc, nil}}

          {:ok, {:error, error}} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_error(Map.get(acc, handler_id), error))
            {:cont, {acc, nil}}

          {:exit, reason} ->
            acc = Map.put(acc, handler_id, HandlerResult.mark_error(Map.get(acc, handler_id), reason))
            {:cont, {acc, nil}}

          nil ->
            Task.shutdown(task, :brutal_kill)
            acc = Map.put(acc, handler_id, HandlerResult.mark_aborted(Map.get(acc, handler_id)))
            {:cont, {acc, nil}}
        end
      end)

    final_results
  end

  defp cancel_remaining_tasks(tasks, completed_id) do
    for {handler_id, task} <- tasks, handler_id != completed_id do
      Task.shutdown(task, :brutal_kill)
    end
  end

  # ── Serial handler execution ───────────────────────────────────────────────

  defp run_handlers_serial(event, handlers, results, completion_mode, bus_config) do
    Enum.reduce_while(handlers, results, fn entry, acc ->
      result_entry = Map.get(acc, entry.id) |> HandlerResult.mark_started()

      case run_single_handler(entry, event, bus_config) do
        {:ok, value} ->
          updated = HandlerResult.mark_completed(result_entry, value)
          acc = Map.put(acc, entry.id, updated)

          case completion_mode do
            :first when not is_nil(value) ->
              # Mark remaining handlers as cancelled
              acc = cancel_remaining_serial(handlers, entry.id, acc)
              {:halt, acc}

            _ ->
              {:cont, acc}
          end

        {:error, error} ->
          updated = HandlerResult.mark_error(result_entry, error)
          acc = Map.put(acc, entry.id, updated)
          {:cont, acc}
      end
    end)
  end

  defp cancel_remaining_serial(handlers, completed_id, results) do
    found_completed = Enum.reduce_while(handlers, false, fn entry, _acc ->
      if entry.id == completed_id, do: {:halt, true}, else: {:cont, false}
    end)

    if found_completed do
      handlers
      |> Enum.drop_while(fn e -> e.id != completed_id end)
      |> Enum.drop(1)
      |> Enum.reduce(results, fn entry, acc ->
        case Map.get(acc, entry.id) do
          nil -> acc
          result -> Map.put(acc, entry.id, HandlerResult.mark_cancelled(result))
        end
      end)
    else
      results
    end
  end

  # ── Single handler execution ───────────────────────────────────────────────

  defp run_single_handler(entry, event, bus_config) do
    # Resolve handler-level timeout (tightest wins)
    timeout = LockManager.resolve_handler_timeout(entry, event, bus_config)

    # Apply semaphore if configured
    result =
      maybe_with_semaphore(entry, fn ->
        maybe_with_handler_timeout(timeout, fn ->
          # Retry loop
          run_with_retries(entry, event, entry.max_attempts)
        end)
      end)

    result
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

    task = Task.async(fn -> fun.() end)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)

        # Mark all pending/started results as aborted/cancelled
        Enum.reduce(results, results, fn {handler_id, result}, acc ->
          updated =
            case result.status do
              :pending -> HandlerResult.mark_cancelled(result)
              :started -> HandlerResult.mark_aborted(result)
              _ -> result
            end

          Map.put(acc, handler_id, updated)
        end)
    end
  end

  defp maybe_with_handler_timeout(nil, fun), do: fun.()

  defp maybe_with_handler_timeout(timeout_s, fun) do
    timeout_ms = trunc(timeout_s * 1000)

    task = Task.async(fun)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
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

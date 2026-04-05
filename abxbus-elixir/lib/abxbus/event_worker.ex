defmodule Abxbus.EventWorker do
  @moduledoc """
  Per-event handler execution process.

  Each event dispatch spawns one EventWorker (a bare process, not a GenServer).
  The worker:

  1. Sets up handler context in the process dictionary
  2. Resolves effective concurrency/completion/timeout config
  3. Runs handlers (serial or parallel via Tasks)
  4. Enforces handler-level and event-level timeouts
  5. Monitors for slow events/handlers
  6. Sends results back to BusServer
  """

  require Logger

  alias Abxbus.{EventStore, LockManager, EventResult}

  @doc """
  Main entry point — runs in a spawned process.
  """
  def run(event, handlers, bus_config, bus_pid, bus_name) do
    # Track handler recursion depth across nested emit→await chains.
    # The caller's depth is stored per-event in ETS by the BusServer.
    caller_depth =
      case :ets.lookup(:abxbus_event_depth, event.event_id) do
        [{_, d}] -> d
        [] -> 0
      end
    max_depth = Map.get(bus_config, :max_handler_recursion_depth, 3)
    Process.put(:abxbus_handler_depth, caller_depth + 1)

    Process.put(:abxbus_current_event_id, event.event_id)
    Process.put(:abxbus_current_bus, bus_name)
    Process.put(:abxbus_current_bus_pid, bus_pid)

    # Write pending handler results to ETS BEFORE signaling started,
    # so middleware can see them in on_event_result_change(:started)
    pre_populate_pending_results(event, handlers, bus_config, bus_name)

    send(bus_pid, {:event_worker_started, event.event_id})

    results =
      if caller_depth >= max_depth do
        # Recursion depth exceeded — mark all handlers as error
        handlers
        |> Enum.map(fn entry ->
          result = EventResult.new(entry.id,
            event_id: event.event_id,
            handler_name: entry.handler_name,
            handler_file_path: entry.handler_file_path,
            timeout: LockManager.resolve_handler_timeout(entry, event, bus_config),
            handler_registered_at: entry.handler_registered_at,
            eventbus_name: bus_name
          ) |> EventResult.mark_started() |> EventResult.mark_error(
            %RuntimeError{message: "Infinite loop detected: handler recursion depth #{caller_depth + 1} exceeds max #{max_depth}"}
          )
          {entry.id, result}
        end)
        |> Map.new()
      else
        run_handlers(event, handlers, bus_config, bus_pid, bus_name)
      end

    send(bus_pid, {:event_worker_done, event.event_id, results})
  end

  defp pre_populate_pending_results(_event, [], _bus_config, _bus_name), do: :ok
  defp pre_populate_pending_results(event, handlers, bus_config, bus_name) do
    pending =
      handlers
      |> Enum.map(fn entry ->
        {entry.id, EventResult.new(entry.id,
          event_id: event.event_id,
          handler_name: entry.handler_name,
          handler_file_path: entry.handler_file_path,
          timeout: LockManager.resolve_handler_timeout(entry, event, bus_config),
          handler_registered_at: entry.handler_registered_at,
          eventbus_name: bus_name
        )}
      end)
      |> Map.new()

    EventStore.update_fun(event.event_id, fn existing ->
      %{existing | event_results: Map.merge(existing.event_results, pending)}
    end)
  end

  # Fast path: no handlers, no work to do
  defp run_handlers(_event, [], _bus_config, _bus_pid, _bus_name), do: %{}

  defp run_handlers(event, handlers, bus_config, bus_pid, bus_name) do
    handler_concurrency = LockManager.resolve_handler_concurrency(event, bus_config)
    handler_completion = LockManager.resolve_handler_completion(event, bus_config)
    event_timeout = LockManager.resolve_event_timeout(event, bus_config)

    # Resolve slow warning thresholds
    event_slow = Map.get(event, :event_slow_timeout) || Map.get(bus_config, :event_slow_timeout)
    handler_slow = Map.get(event, :event_handler_slow_timeout) || Map.get(bus_config, :event_handler_slow_timeout)

    # Pending results already written to ETS by pre_populate_pending_results.
    # Reconstruct local results map for handler execution tracking.
    results =
      handlers
      |> Enum.map(fn entry ->
        {entry.id, EventResult.new(entry.id,
          event_id: event.event_id,
          handler_name: entry.handler_name,
          handler_file_path: entry.handler_file_path,
          timeout: LockManager.resolve_handler_timeout(entry, event, bus_config),
          handler_registered_at: entry.handler_registered_at,
          eventbus_name: bus_name
        )}
      end)
      |> Map.new()

    Process.put(:abxbus_event_results_key, nil)

    # Start slow event monitor
    slow_monitor = maybe_start_slow_monitor(event_slow, event, bus_name, :event)

    results =
      maybe_with_event_timeout(event_timeout, fn ->
        case handler_concurrency do
          :parallel ->
            run_handlers_parallel(event, handlers, results, handler_completion, bus_config, bus_name, bus_pid, handler_slow)

          :serial ->
            run_handlers_serial(event, handlers, results, handler_completion, bus_config, bus_name, bus_pid, handler_slow)
        end
      end, results)

    # Stop slow event monitor
    maybe_stop_slow_monitor(slow_monitor)

    results
  end

  # ── Slow monitoring ────────────────────────────────────────────────────────

  defp maybe_start_slow_monitor(nil, _event, _bus_name, _kind), do: nil

  defp maybe_start_slow_monitor(timeout_s, event, bus_name, kind) do
    timeout_ms = trunc(timeout_s * 1000)
    _self = self()

    _pid = spawn(fn ->
      Process.sleep(timeout_ms)

      label = case kind do
        :event -> "Slow event processing: #{inspect(event.event_type)} (#{event.event_id}) on #{bus_name} exceeded #{timeout_s}s"
        {:handler, name} -> "Slow event handler: #{name} for #{inspect(event.event_type)} (#{event.event_id}) on #{bus_name} exceeded #{timeout_s}s"
      end

      Logger.warning(label)
    end)
  end

  defp maybe_stop_slow_monitor(nil), do: :ok
  defp maybe_stop_slow_monitor(pid), do: Process.exit(pid, :kill)

  # ── Parallel handler execution ─────────────────────────────────────────────

  # Fast path: single handler — run directly without Task.async overhead
  defp run_handlers_parallel(event, [entry], results, _completion_mode, bus_config, bus_name, _bus_pid, handler_slow) do
    Process.put(:abxbus_current_handler_id, entry.id)

    result_entry = Map.get(results, entry.id) |> EventResult.mark_started()
    update_result_in_ets(event.event_id, entry.id, result_entry)

    slow_t = entry.handler_slow_timeout || handler_slow
    monitor = maybe_start_slow_monitor(slow_t, event, bus_name, {:handler, entry.handler_name})
    outcome = run_single_handler(entry, event, bus_config)
    maybe_stop_slow_monitor(monitor)

    case outcome do
      {:ok, value} ->
        updated = EventResult.mark_completed(result_entry, value)
        update_result_in_ets(event.event_id, entry.id, updated)
        Map.put(results, entry.id, updated)
      {:error, error} ->
        updated = EventResult.mark_error(result_entry, error)
        update_result_in_ets(event.event_id, entry.id, updated)
        Map.put(results, entry.id, updated)
    end
  end

  defp run_handlers_parallel(event, handlers, results, completion_mode, bus_config, bus_name, bus_pid, handler_slow) do
    current_event_id = event.event_id
    handler_depth = Process.get(:abxbus_handler_depth, 0)

    tasks =
      Enum.map(handlers, fn entry ->
        task =
          Task.async(fn ->
            Process.put(:abxbus_current_event_id, current_event_id)
            Process.put(:abxbus_current_bus, bus_name)
            Process.put(:abxbus_current_bus_pid, bus_pid)
            Process.put(:abxbus_current_handler_id, entry.id)
            Process.put(:abxbus_handler_depth, handler_depth)

            slow_t = entry.handler_slow_timeout || handler_slow
            monitor = maybe_start_slow_monitor(slow_t, event, bus_name, {:handler, entry.handler_name})
            result = run_single_handler(entry, event, bus_config)
            maybe_stop_slow_monitor(monitor)
            result
          end)

        {entry.id, task}
      end)

    case completion_mode do
      :all ->
        Enum.reduce(tasks, results, fn {handler_id, task}, acc ->
          result_entry = Map.get(acc, handler_id) |> EventResult.mark_started()

          case Task.yield(task, :infinity) do
            {:ok, {:ok, value}} ->
              Map.put(acc, handler_id, EventResult.mark_completed(result_entry, value))
            {:ok, {:error, error}} ->
              Map.put(acc, handler_id, EventResult.mark_error(result_entry, error))
            {:exit, reason} ->
              Map.put(acc, handler_id, EventResult.mark_error(result_entry, reason))
            nil ->
              Task.shutdown(task, :brutal_kill)
              Map.put(acc, handler_id, EventResult.mark_aborted(result_entry))
          end
        end)

      :first ->
        await_first_parallel(tasks, results)
    end
  end

  defp await_first_parallel(tasks, results) do
    results = Enum.reduce(tasks, results, fn {handler_id, _}, acc ->
      Map.update!(acc, handler_id, &EventResult.mark_started/1)
    end)

    task_map = Map.new(tasks, fn {handler_id, task} -> {task.ref, handler_id} end)
    all_tasks = Enum.map(tasks, fn {_, task} -> task end)
    do_await_first(all_tasks, task_map, results, tasks)
  end

  defp do_await_first([], _task_map, results, _), do: results

  defp do_await_first(remaining, task_map, results, original) do
    yielded = Task.yield_many(remaining, 1)

    {results, found, still} =
      Enum.reduce(yielded, {results, nil, []}, fn {task, result}, {acc, first, rem} ->
        handler_id = Map.get(task_map, task.ref)

        case result do
          {:ok, {:ok, value}} when not is_nil(value) and is_nil(first) ->
            acc = Map.put(acc, handler_id, EventResult.mark_completed(Map.get(acc, handler_id), value))
            {acc, value, rem}
          {:ok, {:ok, value}} ->
            acc = Map.put(acc, handler_id, EventResult.mark_completed(Map.get(acc, handler_id), value))
            {acc, first, rem}
          {:ok, {:error, error}} ->
            acc = Map.put(acc, handler_id, EventResult.mark_error(Map.get(acc, handler_id), error))
            {acc, first, rem}
          {:exit, reason} ->
            acc = Map.put(acc, handler_id, EventResult.mark_error(Map.get(acc, handler_id), reason))
            {acc, first, rem}
          nil ->
            {acc, first, [task | rem]}
        end
      end)

    if found != nil do
      results = Enum.reduce(still, results, fn task, acc ->
        Task.shutdown(task, :brutal_kill)
        handler_id = Map.get(task_map, task.ref)
        if handler_id do
          Map.put(acc, handler_id, EventResult.mark_cancelled(Map.get(acc, handler_id)))
        else
          acc
        end
      end)
      results
    else
      if still == [], do: results, else: do_await_first(still, task_map, results, original)
    end
  end

  # ── Serial handler execution ───────────────────────────────────────────────

  defp run_handlers_serial(event, handlers, results, completion_mode, bus_config, bus_name, bus_pid, handler_slow) do
    {final_results, _} =
      Enum.reduce_while(handlers, {results, false}, fn entry, {acc, _} ->
        Process.put(:abxbus_current_event_id, event.event_id)
        Process.put(:abxbus_current_bus, bus_name)
        Process.put(:abxbus_current_bus_pid, bus_pid)
        Process.put(:abxbus_current_handler_id, entry.id)

        result_entry = Map.get(acc, entry.id) |> EventResult.mark_started()
        update_result_in_ets(event.event_id, entry.id, result_entry)

        slow_t = entry.handler_slow_timeout || handler_slow
        monitor = maybe_start_slow_monitor(slow_t, event, bus_name, {:handler, entry.handler_name})

        outcome = run_single_handler(entry, event, bus_config)

        maybe_stop_slow_monitor(monitor)

        case outcome do
          {:ok, value} ->
            updated = EventResult.mark_completed(result_entry, value)
            update_result_in_ets(event.event_id, entry.id, updated)
            acc = Map.put(acc, entry.id, updated)
            update_shared_results(acc)

            case completion_mode do
              :first when not is_nil(value) ->
                acc = cancel_remaining_serial(handlers, entry.id, acc)
                {:halt, {acc, true}}
              _ ->
                {:cont, {acc, false}}
            end

          {:error, error} ->
            updated = EventResult.mark_error(result_entry, error)
            update_result_in_ets(event.event_id, entry.id, updated)
            acc = Map.put(acc, entry.id, updated)
            update_shared_results(acc)
            {:cont, {acc, false}}
        end
      end)

    final_results
  end

  # Write individual handler result to ETS for real-time observability
  defp update_result_in_ets(event_id, handler_id, result) do
    EventStore.update_fun(event_id, fn event ->
      %{event | event_results: Map.put(event.event_results, handler_id, result)}
    end)
  end

  defp update_shared_results(results) do
    case Process.get(:abxbus_event_results_key) do
      nil -> :ok
      key ->
        case :ets.info(:abxbus_worker_results) do
          :undefined -> :ok
          _ -> :ets.insert(:abxbus_worker_results, {key, results})
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
        result -> Map.put(acc, entry.id, EventResult.mark_cancelled(result))
      end
    end)
  end

  # ── Single handler execution ───────────────────────────────────────────────

  defp run_single_handler(entry, event, bus_config) do
    timeout = LockManager.resolve_handler_timeout(entry, event, bus_config)

    maybe_with_semaphore(entry, fn ->
      maybe_with_handler_timeout(timeout, fn ->
        run_with_retries(entry, event, entry.max_attempts, 1)
      end)
    end)
  end

  defp run_with_retries(entry, event, attempts_left, attempt_index) do
    try do
      value = entry.handler.(event)
      {:ok, value}
    rescue
      e ->
        if attempts_left > 1 and should_retry?(e, entry.retry_on_errors) do
          delay = compute_retry_delay(entry, attempt_index)
          if delay > 0, do: Process.sleep(trunc(delay * 1000))
          run_with_retries(entry, event, attempts_left - 1, attempt_index + 1)
        else
          {:error, e}
        end
    catch
      :exit, reason ->
        if attempts_left > 1 do
          delay = compute_retry_delay(entry, attempt_index)
          if delay > 0, do: Process.sleep(trunc(delay * 1000))
          run_with_retries(entry, event, attempts_left - 1, attempt_index + 1)
        else
          {:error, reason}
        end
    end
  end

  defp should_retry?(_error, nil), do: true

  defp should_retry?(error, matchers) do
    Enum.any?(matchers, fn matcher ->
      cond do
        is_atom(matcher) -> is_struct(error, matcher)
        is_function(matcher, 1) -> matcher.(error)
        true -> false
      end
    end)
  end

  defp compute_retry_delay(entry, attempt_index) do
    entry.retry_after * :math.pow(entry.retry_backoff_factor, attempt_index - 1)
  end

  # ── Timeout enforcement ────────────────────────────────────────────────────

  defp maybe_with_event_timeout(nil, fun, _results), do: fun.()

  defp maybe_with_event_timeout(timeout_s, fun, results) do
    timeout_ms = trunc(timeout_s * 1000)
    caller = self()

    event_id = Process.get(:abxbus_current_event_id)
    bus_name = Process.get(:abxbus_current_bus)
    bus_pid = Process.get(:abxbus_current_bus_pid)
    handler_depth = Process.get(:abxbus_handler_depth, 0)

    results_ref = :erlang.make_ref()
    results_key = {__MODULE__, results_ref}

    :ets.insert(:abxbus_worker_results, {results_key, results})

    # Trap exits BEFORE spawn_link to avoid race window
    old_trap = Process.flag(:trap_exit, true)

    pid = spawn_link(fn ->
      Process.put(:abxbus_current_event_id, event_id)
      Process.put(:abxbus_current_bus, bus_name)
      Process.put(:abxbus_current_bus_pid, bus_pid)
      Process.put(:abxbus_handler_depth, handler_depth)
      Process.put(:abxbus_event_results_key, results_key)

      result = fun.()
      :ets.insert(:abxbus_worker_results, {results_key, result})
      send(caller, {:event_timeout_result, result})
    end)

    result =
      receive do
        {:event_timeout_result, result} -> result
      after
        timeout_ms ->
          Process.exit(pid, :kill)
          receive do
            {:EXIT, ^pid, _} -> :ok
          after
            10 -> :ok
          end

          latest =
            case :ets.lookup(:abxbus_worker_results, results_key) do
              [{_, r}] -> r
              [] -> results
            end

          Enum.reduce(latest, latest, fn {handler_id, result}, acc ->
            updated =
              case result.status do
                s when s in [:pending, :started] ->
                  EventResult.mark_error(result, %Abxbus.EventHandlerAbortedError{})
                _ -> result
              end
            Map.put(acc, handler_id, updated)
          end)
      end

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end

    Process.flag(:trap_exit, old_trap)
    :ets.delete(:abxbus_worker_results, results_key)
    result
  end

  defp maybe_with_handler_timeout(nil, fun), do: fun.()

  defp maybe_with_handler_timeout(timeout_s, fun) do
    timeout_ms = trunc(timeout_s * 1000)
    caller = self()
    ref = make_ref()

    event_id = Process.get(:abxbus_current_event_id)
    bus_name = Process.get(:abxbus_current_bus)
    bus_pid = Process.get(:abxbus_current_bus_pid)
    handler_depth = Process.get(:abxbus_handler_depth, 0)

    # Trap exits BEFORE spawn_link to avoid race window
    old_trap = Process.flag(:trap_exit, true)

    pid = spawn_link(fn ->
      Process.put(:abxbus_current_event_id, event_id)
      Process.put(:abxbus_current_bus, bus_name)
      Process.put(:abxbus_current_bus_pid, bus_pid)
      Process.put(:abxbus_handler_depth, handler_depth)

      result = fun.()
      send(caller, {:handler_timeout_result, ref, result})
    end)

    result =
      receive do
        {:handler_timeout_result, ^ref, result} -> result
      after
        timeout_ms ->
          Process.exit(pid, :kill)
          {:error, %Abxbus.EventHandlerTimeoutError{}}
      end

    # Drain EXIT from the linked child
    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end

    # Drain any stale result message
    receive do
      {:handler_timeout_result, ^ref, _} -> :ok
    after
      0 -> :ok
    end

    Process.flag(:trap_exit, old_trap)
    result
  end

  defp maybe_with_semaphore(%{semaphore_scope: :none}, fun), do: fun.()

  defp maybe_with_semaphore(%{semaphore_scope: scope, semaphore_name: name, semaphore_limit: limit, eventbus_name: bus}, fun)
       when scope in [:global, :bus] do
    # Bus-scoped semaphores include bus name in key to isolate across buses
    base_name = name || "default"
    sem_name = if scope == :bus, do: "#{bus}:#{base_name}", else: base_name
    LockManager.acquire_semaphore(sem_name, limit)

    try do
      fun.()
    after
      LockManager.release_semaphore(sem_name)
    end
  end

  defp maybe_with_semaphore(_, fun), do: fun.()
end

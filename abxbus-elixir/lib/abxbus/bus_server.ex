defmodule Abxbus.BusServer do
  @moduledoc """
  The brain of each event bus. A GenServer that manages:

    * The pending event queue (Erlang `:queue` — O(1) both ends)
    * Event concurrency enforcement (parallel / bus-serial / global-serial)
    * Queue-jump for awaited child events
    * In-flight and processing event tracking
    * Idle detection and `wait_until_idle/1`
    * History management with backpressure and trimming
    * Middleware lifecycle dispatch
    * Handler registration and unregistration
  """

  use GenServer
  require Logger

  alias Abxbus.{EventStore, EventWorker, LockManager, EventHandler, Middleware}

  defstruct [
    :name,
    :label,
    :id,
    # config defaults
    event_concurrency: :bus_serial,
    event_handler_concurrency: :parallel,
    event_handler_completion: :all,
    event_timeout: nil,
    event_handler_timeout: nil,
    event_slow_timeout: nil,
    event_handler_slow_timeout: nil,
    event_handler_detect_file_paths: true,
    # cached config map (rebuilt on config change)
    cached_config: %{},
    # runtime state
    pending_event_queue: :queue.new(),
    in_flight_event_ids: MapSet.new(),
    processing_event_ids: MapSet.new(),
    worker_monitors: %{},
    event_history: [],
    event_history_count: 0,
    max_history_size: 1000,
    max_history_drop: true,
    idle_waiters: [],
    handlers: %{},
    middlewares: [],
    started: false
  ]

  # ── Client API ──────────────────────────────────────────────────────────────

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  def via(name) when is_atom(name), do: name
  def via(name), do: {:via, Registry, {Abxbus.BusRegistry, name}}

  @doc "Emit an event onto this bus. Non-blocking."
  def emit(bus, event) do
    GenServer.call(lookup(bus), {:emit, event})
  end

  @doc "Alias for emit (matches Python's dispatch)."
  def dispatch(bus, event), do: emit(bus, event)

  @doc "Register a handler for an event pattern. Returns the EventHandler entry."
  def on(bus, event_pattern, handler, opts \\ []) do
    GenServer.call(lookup(bus), {:register_handler, event_pattern, handler, opts})
  end

  @doc "Unregister handlers by pattern, function ref, or handler ID."
  def off(bus, event_pattern_or_opts) do
    GenServer.call(lookup(bus), {:unregister_handler, event_pattern_or_opts, nil})
  end

  def off(bus, event_pattern, handler) do
    GenServer.call(lookup(bus), {:unregister_handler, event_pattern, handler})
  end

  @doc "Wait until the bus has no pending or in-flight events."
  def wait_until_idle(bus, timeout \\ :infinity) do
    GenServer.call(lookup(bus), :wait_until_idle, timeout)
  end

  @doc "Stop the bus, optionally clearing all state."
  def stop(bus, opts \\ []), do: GenServer.call(lookup(bus), {:stop, opts})

  @doc "Get the bus label (name#short_id)."
  def label(bus), do: GenServer.call(lookup(bus), :label)

  @doc "Get current in-flight event IDs."
  def in_flight_event_ids(bus), do: GenServer.call(lookup(bus), :in_flight_event_ids)

  @doc "Get current processing event IDs."
  def processing_event_ids(bus), do: GenServer.call(lookup(bus), :processing_event_ids)

  @doc "Get the pending queue size."
  def queue_size(bus), do: GenServer.call(lookup(bus), :queue_size)

  @doc "List pending events."
  def events_pending(bus), do: GenServer.call(lookup(bus), :events_pending)

  @doc "List started events."
  def events_started(bus), do: GenServer.call(lookup(bus), :events_started)

  @doc "List completed events."
  def events_completed(bus), do: GenServer.call(lookup(bus), :events_completed)

  defp lookup(bus) when is_pid(bus), do: bus
  defp lookup(bus), do: via(bus)

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    short_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    bus_id = Abxbus.Event.generate_id()

    state = %__MODULE__{
      name: name,
      id: bus_id,
      label: "#{name}##{short_id}",
      event_concurrency: Keyword.get(opts, :event_concurrency, :bus_serial),
      event_handler_concurrency: Keyword.get(opts, :event_handler_concurrency, :parallel),
      event_handler_completion: Keyword.get(opts, :event_handler_completion, :all),
      event_timeout: Keyword.get(opts, :event_timeout),
      event_handler_timeout: Keyword.get(opts, :event_handler_timeout),
      event_slow_timeout: Keyword.get(opts, :event_slow_timeout),
      event_handler_slow_timeout: Keyword.get(opts, :event_handler_slow_timeout),
      event_handler_detect_file_paths: Keyword.get(opts, :event_handler_detect_file_paths, true),
      max_history_size: Keyword.get(opts, :max_history_size, 1000),
      max_history_drop: Keyword.get(opts, :max_history_drop, true),
      middlewares: Keyword.get(opts, :middlewares, []),
      started: true
    }

    state = %{state | cached_config: build_config(state)}

    ensure_bus_pid_table()
    :ets.insert(:abxbus_bus_pids, {self(), state.name})

    {:ok, state}
  end

  defp ensure_bus_pid_table do
    try do
      :ets.new(:abxbus_bus_pids, [:set, :public, :named_table])
    rescue
      ArgumentError -> :ok
    end
  end

  # ── Emit ────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:emit, _event}, _from, %{started: false} = state) do
    {:reply, {:error, :stopped}, state}
  end

  def handle_call({:emit, event}, _from, state) do
    event = prepare_event(event, state.label)

    case check_backpressure(state) do
      :ok ->
        # Store in ETS and index — needed for find, await, etc.
        EventStore.put_or_merge(event)
        EventStore.index_to_bus(state.name, event.event_id)
        EventStore.resolve_find_waiters(event)

        state = %{state |
          pending_event_queue: :queue.in(event, state.pending_event_queue),
          in_flight_event_ids: MapSet.put(state.in_flight_event_ids, event.event_id),
          event_history: [event.event_id | state.event_history],
          event_history_count: state.event_history_count + 1
        }

        if state.middlewares != [] do
          Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :pending])
        end
        state = maybe_process_next(state)
        {:reply, event, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # ── Handler registration ────────────────────────────────────────────────────

  def handle_call({:register_handler, event_pattern, handler, opts}, _from, state) do
    event_pattern = normalize_event_pattern(event_pattern)

    detect = Keyword.get(opts, :detect_file_paths, state.event_handler_detect_file_paths)
    opts = Keyword.put(opts, :detect_file_paths, detect)
    entry = EventHandler.new(event_pattern, handler,
      Keyword.merge(opts, eventbus_name: state.name, eventbus_id: state.id))

    handlers =
      Map.update(state.handlers, event_pattern, [entry], fn existing ->
        existing ++ [entry]
      end)

    Middleware.dispatch(state.middlewares, :on_bus_handlers_change, [state.name, entry, true])

    {:reply, entry, %{state | handlers: handlers}}
  end

  # ── Handler unregistration ─────────────────────────────────────────────────

  def handle_call({:unregister_handler, pattern_or_opts, handler}, _from, state) do
    {removed, handlers} =
      case {pattern_or_opts, handler} do
        {[handler_id: id], _} ->
          remove_handler_by_id(state.handlers, id)

        {event_pattern, nil} ->
          et = normalize_event_pattern(event_pattern)
          removed = Map.get(state.handlers, et, [])
          {removed, Map.delete(state.handlers, et)}

        {event_pattern, handler} ->
          et = normalize_event_pattern(event_pattern)
          remove_handler_by_fn(state.handlers, et, handler)
      end

    for entry <- List.wrap(removed) do
      Middleware.dispatch(state.middlewares, :on_bus_handlers_change, [state.name, entry, false])
    end

    {:reply, :ok, %{state | handlers: handlers}}
  end

  # ── Idle ────────────────────────────────────────────────────────────────────

  def handle_call(:wait_until_idle, from, state) do
    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | idle_waiters: [from | state.idle_waiters]}}
    end
  end

  # ── Introspection ──────────────────────────────────────────────────────────

  def handle_call(:label, _from, state), do: {:reply, state.label, state}
  def handle_call(:in_flight_event_ids, _from, state), do: {:reply, state.in_flight_event_ids, state}
  def handle_call(:processing_event_ids, _from, state), do: {:reply, state.processing_event_ids, state}
  def handle_call(:queue_size, _from, state), do: {:reply, :queue.len(state.pending_event_queue), state}

  def handle_call(:events_pending, _from, state) do
    events = :queue.to_list(state.pending_event_queue)
    {:reply, events, state}
  end

  def handle_call(:events_started, _from, state) do
    events =
      state.processing_event_ids
      |> MapSet.to_list()
      |> Enum.map(&EventStore.get/1)
      |> Enum.reject(&is_nil/1)
    {:reply, events, state}
  end

  def handle_call(:events_completed, _from, state) do
    events =
      state.event_history
      |> Enum.map(&EventStore.get/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&(&1.event_status == :completed))
    {:reply, events, state}
  end

  # ── Stop ────────────────────────────────────────────────────────────────────

  def handle_call({:stop, opts}, _from, state) do
    if Keyword.get(opts, :clear, false) do
      {:reply, :ok, %{state |
        pending_event_queue: :queue.new(),
        in_flight_event_ids: MapSet.new(),
        processing_event_ids: MapSet.new(),
        event_history: [],
        event_history_count: 0,
        worker_monitors: %{},
        started: false
      }}
    else
      {:reply, :ok, %{state | started: false}}
    end
  end

  # ── Queue jump ──────────────────────────────────────────────────────────────

  @impl true
  def handle_cast({:jump_queue, event_id, _notify_pid, _notify_ref}, state) do
    case remove_from_queue(state.pending_event_queue, event_id) do
      {:ok, event, new_queue} ->
        state = %{state | pending_event_queue: new_queue}
        state = spawn_event_worker(state, event, jump: true)
        {:noreply, state}

      :not_found ->
        {:noreply, state}
    end
  end

  # ── Event worker completion ─────────────────────────────────────────────────

  @impl true
  def handle_info({:event_worker_done, event_id, results}, state) do
    # Use update_fun for atomic read-modify-write (safe for forwarded events
    # completing on multiple buses concurrently)
    EventStore.update_fun(event_id, fn event ->
      now = System.monotonic_time(:nanosecond)
      completed_ats = for({_, r} <- results, r.completed_at != nil, do: r.completed_at)
      completed_at = Enum.max([now | completed_ats])
      new_status = if event.event_status == :error, do: :error, else: :completed

      %{event |
        event_results: Map.merge(event.event_results, results),
        event_status: new_status,
        event_completed_at: completed_at,
        event_pending_bus_count: max((event.event_pending_bus_count || 1) - 1, 0)
      }
    end)

    state = %{state |
      processing_event_ids: MapSet.delete(state.processing_event_ids, event_id),
      in_flight_event_ids: MapSet.delete(state.in_flight_event_ids, event_id)
    }

    event = EventStore.get(event_id)
    effective_concurrency = LockManager.resolve_event_concurrency(event || %{}, config(state))

    if effective_concurrency == :global_serial do
      LockManager.release_global()
      notify_all_buses_check_pending()
    end

    if event do
      # Dispatch per-handler result change notifications
      if state.middlewares != [] do
        for {_handler_id, result} <- results do
          Middleware.dispatch(state.middlewares, :on_event_result_change, [state.name, event, result, result.status])
        end
      end

      Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :completed])
      maybe_mark_tree_complete(event)
    end

    state = maybe_trim_history(state)
    state = maybe_process_next(state)
    state = maybe_notify_idle(state)

    {:noreply, state}
  end

  def handle_info({:event_worker_started, event_id}, state) do
    EventStore.update(event_id, %{event_status: :started, event_started_at: System.monotonic_time(:nanosecond)})

    if event = EventStore.get(event_id) do
      Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :started])

      # Dispatch :started for each handler result
      if state.middlewares != [] do
        for {_handler_id, result} <- event.event_results do
          Middleware.dispatch(state.middlewares, :on_event_result_change, [state.name, event, result, :started])
        end
      end
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.worker_monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {event_id, monitors} ->
        state = %{state | worker_monitors: monitors}

        if reason != :normal do
          Logger.warning("EventWorker for #{event_id} crashed: #{inspect(reason)}")
          EventStore.update(event_id, %{event_status: :error})
          state = %{state |
            processing_event_ids: MapSet.delete(state.processing_event_ids, event_id),
            in_flight_event_ids: MapSet.delete(state.in_flight_event_ids, event_id)
          }

          # Same cleanup as normal completion path — release locks, notify waiters
          event = EventStore.get(event_id)
          effective_concurrency = LockManager.resolve_event_concurrency(event || %{}, config(state))

          if effective_concurrency == :global_serial do
            LockManager.release_global()
            notify_all_buses_check_pending()
          end

          if event do
            Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :completed])
            maybe_mark_tree_complete(event)
          end

          state = maybe_process_next(state)
          state = maybe_notify_idle(state)
          {:noreply, state}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info(:check_pending, state) do
    state = maybe_process_next(state)
    state = maybe_notify_idle(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Internal: queue processing ──────────────────────────────────────────────

  defp maybe_process_next(state) do
    if :queue.is_empty(state.pending_event_queue) do
      state
    else
      {:value, event} = :queue.peek(state.pending_event_queue)
      effective_concurrency = LockManager.resolve_event_concurrency(event, config(state))

      case can_proceed?(effective_concurrency, state) do
        true ->
          {{:value, event}, new_queue} = :queue.out(state.pending_event_queue)
          state = %{state | pending_event_queue: new_queue}
          state = spawn_event_worker(state, event, jump: false)

          if effective_concurrency == :parallel do
            maybe_process_next(state)
          else
            state
          end

        false ->
          state
      end
    end
  end

  defp can_proceed?(concurrency_mode, state) do
    case concurrency_mode do
      :parallel -> true
      :bus_serial -> MapSet.size(state.processing_event_ids) == 0
      :global_serial ->
        MapSet.size(state.processing_event_ids) == 0 and
          LockManager.try_acquire_global() == :ok
    end
  end

  defp spawn_event_worker(state, event, _opts) do
    bus_pid = self()
    bus_name = state.name
    handlers = applicable_handlers(state, event)
    bus_config = config(state)

    pid = spawn(fn ->
      EventWorker.run(event, handlers, bus_config, bus_pid, bus_name)
    end)

    ref = Process.monitor(pid)

    %{state |
      processing_event_ids: MapSet.put(state.processing_event_ids, event.event_id),
      worker_monitors: Map.put(state.worker_monitors, ref, event.event_id)
    }
  end

  defp applicable_handlers(%{handlers: handlers}, _event) when map_size(handlers) == 0, do: []

  defp applicable_handlers(state, event) do
    event_type = event.event_type

    type_handlers = Map.get(state.handlers, event_type, [])
    wildcard_handlers = Map.get(state.handlers, :wildcard, [])

    # Only do Module.split for string-based handler lookup if needed
    string_handlers =
      if map_size(state.handlers) > length(type_handlers) + length(wildcard_handlers) and is_atom(event_type) do
        type_name = event_type |> Module.split() |> List.last()
        Map.get(state.handlers, type_name, [])
      else
        []
      end

    all = type_handlers ++ string_handlers ++ wildcard_handlers

    if wildcard_handlers == [] do
      all
    else
      Enum.reject(all, fn entry ->
        entry.event_pattern == :wildcard and
          entry.eventbus_name != state.name and
          Enum.any?(event.event_path, fn p -> String.starts_with?(p, "#{entry.eventbus_name}#") end)
      end)
    end
  end

  # ── Internal: handler removal ───────────────────────────────────────────────

  defp remove_handler_by_id(handlers, id) do
    Enum.reduce(handlers, {[], handlers}, fn {type, entries}, {removed_acc, h} ->
      {matching, remaining} = Enum.split_with(entries, fn e -> e.id == id end)
      {removed_acc ++ matching, Map.put(h, type, remaining)}
    end)
  end

  defp remove_handler_by_fn(handlers, event_pattern, handler) do
    entries = Map.get(handlers, event_pattern, [])
    {matching, remaining} = Enum.split_with(entries, fn e -> e.handler == handler end)
    {matching, Map.put(handlers, event_pattern, remaining)}
  end

  # ── Internal: history trimming ──────────────────────────────────────────────

  defp maybe_trim_history(%{max_history_drop: false} = state), do: state
  defp maybe_trim_history(%{max_history_size: max} = state) when max < 0, do: state

  defp maybe_trim_history(state) do
    # Trim at 2x max to amortize the O(n) Enum.take cost.
    # Each trim costs O(max), but only runs every max events → amortized O(1).
    if state.event_history_count > state.max_history_size * 2 do
      trimmed = Enum.take(state.event_history, state.max_history_size)
      %{state | event_history: trimmed, event_history_count: state.max_history_size}
    else
      state
    end
  end

  # ── Internal: helpers ───────────────────────────────────────────────────────

  defp normalize_event_pattern("*"), do: :wildcard
  defp normalize_event_pattern(type), do: type

  # Prepare event metadata in a single pass (avoids multiple struct copies)
  defp prepare_event(event, label) do
    id = event.event_id || Abxbus.Event.generate_id()
    type = event.event_type || event.__struct__
    created_at = event.event_created_at || System.monotonic_time(:nanosecond)
    path = if label in event.event_path do
      Logger.warning("Event #{id} already has bus #{label} in path — possible circular forwarding")
      event.event_path
    else
      event.event_path ++ [label]
    end

    %{event |
      event_id: id,
      event_type: type,
      event_created_at: created_at,
      event_path: path
    }
  end

  defp check_backpressure(state) do
    if state.event_history_count >= state.max_history_size and not state.max_history_drop do
      {:error, %Abxbus.HistoryFullError{}}
    else
      :ok
    end
  end

  defp config(state), do: state.cached_config

  defp build_config(state) do
    %{
      event_concurrency: state.event_concurrency,
      event_handler_concurrency: state.event_handler_concurrency,
      event_handler_completion: state.event_handler_completion,
      event_timeout: state.event_timeout,
      event_handler_timeout: state.event_handler_timeout,
      event_slow_timeout: state.event_slow_timeout,
      event_handler_slow_timeout: state.event_handler_slow_timeout
    }
  end

  defp idle?(state) do
    :queue.is_empty(state.pending_event_queue) and MapSet.size(state.in_flight_event_ids) == 0
  end

  defp maybe_notify_idle(%{idle_waiters: []} = state), do: state
  defp maybe_notify_idle(state) do
    if idle?(state) do
      for waiter <- state.idle_waiters, do: GenServer.reply(waiter, :ok)
      %{state | idle_waiters: []}
    else
      state
    end
  end

  defp remove_from_queue(queue, event_id) do
    remove_from_queue(queue, event_id, :queue.new())
  end

  defp remove_from_queue(queue, event_id, acc) do
    case :queue.out(queue) do
      {:empty, _} -> :not_found
      {{:value, event}, rest} ->
        if event.event_id == event_id do
          {:ok, event, :queue.join(acc, rest)}
        else
          remove_from_queue(rest, event_id, :queue.in(event, acc))
        end
    end
  end

  defp notify_all_buses_check_pending do
    case :ets.info(:abxbus_bus_pids) do
      :undefined -> :ok
      _ ->
        :ets.tab2list(:abxbus_bus_pids)
        |> Enum.each(fn {pid, _} ->
          if Process.alive?(pid), do: send(pid, :check_pending)
        end)
    end
  end

  defp maybe_mark_tree_complete(event) do
    # Skip tree check if pending_bus_count > 0 (common for forwarded events)
    if (event.event_pending_bus_count || 0) > 0, do: :ok, else: do_mark_tree_complete(event)
  end

  defp do_mark_tree_complete(event) do
    # Only proceed if this event's own handler has finished (status is terminal).
    # This prevents premature tree-completion when a child finishes but the parent
    # handler is still running (e.g., handler called await(child) and continues after).
    unless event.event_status in [:completed, :error] do
      :ok
    else
      children_ids = EventStore.children_of(event.event_id)
      all_children_complete =
        Enum.all?(children_ids, fn child_id ->
          case EventStore.get(child_id) do
            nil -> true
            child -> child.event_status in [:completed, :error]
          end
        end)

      if all_children_complete do
        EventStore.notify_waiters(event.event_id, event)

        if event.event_parent_id do
          case EventStore.get(event.event_parent_id) do
            nil -> :ok
            parent -> do_mark_tree_complete(parent)
          end
        end
      end
    end
  end
end

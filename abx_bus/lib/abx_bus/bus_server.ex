defmodule AbxBus.BusServer do
  @moduledoc """
  The brain of each event bus. A GenServer that manages:

    * The pending event queue (Erlang `:queue` — O(1) both ends)
    * Event concurrency enforcement (parallel / bus-serial / global-serial)
    * Queue-jump for awaited child events
    * In-flight and processing event tracking
    * Forwarding to other buses
    * Idle detection and `wait_until_idle/1`
    * History management with backpressure and trimming
    * Middleware lifecycle dispatch
    * Handler registration and unregistration
  """

  use GenServer
  require Logger

  alias AbxBus.{EventStore, EventWorker, LockManager, HandlerEntry, Middleware}

  defstruct [
    :name,
    :label,
    :event_supervisor,
    # config defaults
    event_concurrency: :bus_serial,
    event_handler_concurrency: :parallel,
    event_handler_completion: :all,
    event_timeout: nil,
    event_handler_timeout: nil,
    event_slow_timeout: nil,
    event_handler_slow_timeout: nil,
    event_handler_detect_file_paths: true,
    # runtime state
    pending_queue: :queue.new(),
    in_flight: MapSet.new(),
    processing: MapSet.new(),
    worker_monitors: %{},
    history: [],
    history_count: 0,
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
  def via(name), do: {:via, Registry, {AbxBus.BusRegistry, name}}

  @doc "Emit an event onto this bus. Non-blocking."
  def emit(bus, event) do
    GenServer.call(lookup(bus), {:emit, event})
  end

  @doc "Register a handler for an event type. Returns the handler entry."
  def on(bus, event_type, handler_fn, opts \\ []) do
    GenServer.call(lookup(bus), {:register_handler, event_type, handler_fn, opts})
  end

  @doc """
  Unregister handlers.

  ## Patterns

    * `off(bus, event_type)` — remove ALL handlers for this event type
    * `off(bus, event_type, handler_fn)` — remove handler matching this function ref
    * `off(bus, handler_id: id)` — remove handler by its unique ID
  """
  def off(bus, event_type_or_opts) do
    GenServer.call(lookup(bus), {:unregister_handler, event_type_or_opts, nil})
  end

  def off(bus, event_type, handler_fn) do
    GenServer.call(lookup(bus), {:unregister_handler, event_type, handler_fn})
  end

  @doc "Wait until the bus has no pending or in-flight events."
  def wait_until_idle(bus, timeout \\ :infinity) do
    GenServer.call(lookup(bus), :wait_until_idle, timeout)
  end

  @doc "Stop the bus, optionally clearing all state."
  def stop(bus, opts \\ []) do
    GenServer.call(lookup(bus), {:stop, opts})
  end

  @doc "Get the bus label (name#short_id)."
  def label(bus), do: GenServer.call(lookup(bus), :label)

  @doc "Get current in-flight event IDs."
  def in_flight_event_ids(bus), do: GenServer.call(lookup(bus), :in_flight_event_ids)

  @doc "Get current processing event IDs."
  def processing_event_ids(bus), do: GenServer.call(lookup(bus), :processing_event_ids)

  @doc "Get the pending queue size."
  def queue_size(bus), do: GenServer.call(lookup(bus), :queue_size)

  @doc "Get handler count for an event type (or all if :all)."
  def handler_count(bus, event_type \\ :all) do
    GenServer.call(lookup(bus), {:handler_count, event_type})
  end

  @doc "Get bus config as a map."
  def get_config(bus), do: GenServer.call(lookup(bus), :get_config)

  defp lookup(bus) when is_pid(bus), do: bus
  defp lookup(bus), do: via(bus)

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    short_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)

    state = %__MODULE__{
      name: name,
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

    ensure_bus_pid_table()
    :ets.insert(:abx_bus_pids, {self(), state.name})

    {:ok, state}
  end

  defp ensure_bus_pid_table do
    try do
      :ets.new(:abx_bus_pids, [:set, :public, :named_table])
    rescue
      ArgumentError -> :ok
    end
  end

  # ── Emit ────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:emit, event}, _from, state) do
    event =
      event
      |> ensure_event_id()
      |> ensure_event_type()
      |> ensure_timestamps()
      |> append_bus_to_path(state.label)

    case check_backpressure(state) do
      :ok ->
        new_queue = :queue.in(event, state.pending_queue)
        new_in_flight = MapSet.put(state.in_flight, event.event_id)

        EventStore.put(event)
        EventStore.index_to_bus(state.name, event.event_id)
        EventStore.update(event.event_id, %{
          event_pending_bus_count: (event.event_pending_bus_count || 0) + 1
        })
        EventStore.resolve_find_waiters(event)

        state = %{state |
          pending_queue: new_queue,
          in_flight: new_in_flight,
          history: [event.event_id | state.history],
          history_count: state.history_count + 1
        }

        # Middleware: event pending
        Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :pending])

        state = maybe_process_next(state)

        {:reply, event, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # ── Handler registration ────────────────────────────────────────────────────

  def handle_call({:register_handler, event_type, handler_fn, opts}, _from, state) do
    event_type = normalize_event_type(event_type)

    detect = Keyword.get(opts, :detect_file_paths, state.event_handler_detect_file_paths)
    opts = Keyword.put(opts, :detect_file_paths, detect)
    entry = HandlerEntry.new(event_type, handler_fn, Keyword.put(opts, :bus_name, state.name))

    handlers =
      Map.update(state.handlers, event_type, [entry], fn existing ->
        existing ++ [entry]
      end)

    # Middleware: handler registered
    Middleware.dispatch(state.middlewares, :on_bus_handlers_change, [state.name, entry, true])

    {:reply, entry, %{state | handlers: handlers}}
  end

  # ── Handler unregistration ─────────────────────────────────────────────────

  def handle_call({:unregister_handler, event_type_or_opts, handler_fn}, _from, state) do
    {removed, handlers} =
      case {event_type_or_opts, handler_fn} do
        {[handler_id: id], _} ->
          # Remove by handler ID across all types
          remove_handler_by_id(state.handlers, id)

        {event_type, nil} ->
          # Remove all handlers for this event type
          et = normalize_event_type(event_type)
          removed = Map.get(state.handlers, et, [])
          {removed, Map.delete(state.handlers, et)}

        {event_type, handler_fn} ->
          # Remove specific handler function for this event type
          et = normalize_event_type(event_type)
          remove_handler_by_fn(state.handlers, et, handler_fn)
      end

    # Middleware: handler unregistered
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
  def handle_call(:in_flight_event_ids, _from, state), do: {:reply, state.in_flight, state}
  def handle_call(:processing_event_ids, _from, state), do: {:reply, state.processing, state}
  def handle_call(:queue_size, _from, state), do: {:reply, :queue.len(state.pending_queue), state}
  def handle_call(:get_config, _from, state), do: {:reply, config(state), state}

  def handle_call({:handler_count, :all}, _from, state) do
    count = state.handlers |> Map.values() |> List.flatten() |> length()
    {:reply, count, state}
  end

  def handle_call({:handler_count, event_type}, _from, state) do
    et = normalize_event_type(event_type)
    {:reply, length(Map.get(state.handlers, et, [])), state}
  end

  # ── Stop ────────────────────────────────────────────────────────────────────

  def handle_call({:stop, opts}, _from, state) do
    if Keyword.get(opts, :clear, false) do
      {:reply, :ok, %{state | pending_queue: :queue.new(), in_flight: MapSet.new(),
                       processing: MapSet.new(), history: [], started: false}}
    else
      {:reply, :ok, %{state | started: false}}
    end
  end

  # ── Queue jump ──────────────────────────────────────────────────────────────

  @impl true
  def handle_cast({:jump_queue, event_id, _notify_pid, _notify_ref}, state) do
    case remove_from_queue(state.pending_queue, event_id) do
      {:ok, event, new_queue} ->
        state = %{state | pending_queue: new_queue}
        state = spawn_event_worker(state, event, jump: true)
        {:noreply, state}

      :not_found ->
        {:noreply, state}
    end
  end

  # ── Event worker completion ─────────────────────────────────────────────────

  @impl true
  def handle_info({:event_worker_done, event_id, results}, state) do
    EventStore.update_fun(event_id, fn event ->
      now = System.monotonic_time(:nanosecond)
      completed_ats = for({_, r} <- results, r.completed_at != nil, do: r.completed_at)
      completed_at = Enum.max([now | completed_ats])

      %{event |
        event_results: results,
        event_status: :completed,
        event_completed_at: completed_at,
        event_pending_bus_count: max((event.event_pending_bus_count || 1) - 1, 0)
      }
    end)

    state = %{state |
      processing: MapSet.delete(state.processing, event_id),
      in_flight: MapSet.delete(state.in_flight, event_id)
    }

    event = EventStore.get(event_id)
    effective_concurrency = LockManager.resolve_event_concurrency(event || %{}, config(state))

    if effective_concurrency == :global_serial do
      LockManager.release_global()
      notify_all_buses_check_pending()
    end

    # Middleware: event completed
    if event do
      Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :completed])
      maybe_mark_tree_complete(event)
    end

    # Trim history if needed
    state = maybe_trim_history(state)

    state = maybe_process_next(state)
    state = maybe_notify_idle(state)

    {:noreply, state}
  end

  @impl true
  def handle_info({:event_worker_started, event_id}, state) do
    EventStore.update(event_id, %{event_status: :started, event_started_at: System.monotonic_time(:nanosecond)})

    # Middleware: event started
    if event = EventStore.get(event_id) do
      Middleware.dispatch(state.middlewares, :on_event_change, [state.name, event, :started])
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.worker_monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {event_id, monitors} ->
        state = %{state | worker_monitors: monitors}

        if reason != :normal do
          Logger.warning("EventWorker for #{event_id} crashed: #{inspect(reason)}")
          # Clean up as if worker completed with error
          EventStore.update(event_id, %{event_status: :error})
          state = %{state |
            processing: MapSet.delete(state.processing, event_id),
            in_flight: MapSet.delete(state.in_flight, event_id)
          }
          state = maybe_process_next(state)
          state = maybe_notify_idle(state)
          {:noreply, state}
        else
          {:noreply, state}
        end
    end
  end

  @impl true
  def handle_info(:check_pending, state) do
    state = maybe_process_next(state)
    state = maybe_notify_idle(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  # ── Internal: queue processing ──────────────────────────────────────────────

  defp maybe_process_next(state) do
    if :queue.is_empty(state.pending_queue) do
      state
    else
      {:value, event} = :queue.peek(state.pending_queue)
      effective_concurrency = LockManager.resolve_event_concurrency(event, config(state))

      case can_proceed?(effective_concurrency, state) do
        true ->
          {{:value, event}, new_queue} = :queue.out(state.pending_queue)
          state = %{state | pending_queue: new_queue}
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
      :bus_serial -> MapSet.size(state.processing) == 0
      :global_serial ->
        MapSet.size(state.processing) == 0 and
          LockManager.try_acquire_global() == :ok
    end
  end

  defp spawn_event_worker(state, event, opts) do
    bus_pid = self()
    bus_name = state.name
    handlers = applicable_handlers(state, event)
    bus_config = config(state)
    _jump = Keyword.get(opts, :jump, false)

    pid = spawn(fn ->
      EventWorker.run(event, handlers, bus_config, bus_pid, bus_name)
    end)

    ref = Process.monitor(pid)

    %{state |
      processing: MapSet.put(state.processing, event.event_id),
      worker_monitors: Map.put(state.worker_monitors, ref, event.event_id)
    }
  end

  defp applicable_handlers(state, event) do
    event_type = event.event_type

    type_handlers = Map.get(state.handlers, event_type, [])
    wildcard_handlers = Map.get(state.handlers, :wildcard, [])

    # Also match string-registered handlers
    type_name = if is_atom(event_type), do: event_type |> Module.split() |> List.last(), else: nil
    string_handlers = if type_name, do: Map.get(state.handlers, type_name, []), else: []

    (type_handlers ++ string_handlers ++ wildcard_handlers)
    |> Enum.reject(fn entry ->
      entry.event_type == :wildcard and
        entry.bus_name != state.name and
        Enum.any?(event.event_path, fn p -> String.starts_with?(p, "#{entry.bus_name}#") end)
    end)
  end

  # ── Internal: handler removal ───────────────────────────────────────────────

  defp remove_handler_by_id(handlers, id) do
    Enum.reduce(handlers, {[], handlers}, fn {type, entries}, {removed_acc, h} ->
      {matching, remaining} = Enum.split_with(entries, fn e -> e.id == id end)
      {removed_acc ++ matching, Map.put(h, type, remaining)}
    end)
  end

  defp remove_handler_by_fn(handlers, event_type, handler_fn) do
    entries = Map.get(handlers, event_type, [])
    {matching, remaining} = Enum.split_with(entries, fn e -> e.handler_fn == handler_fn end)
    {matching, Map.put(handlers, event_type, remaining)}
  end

  # ── Internal: history trimming ──────────────────────────────────────────────

  defp maybe_trim_history(%{max_history_drop: false} = state), do: state
  defp maybe_trim_history(%{max_history_size: max} = state) when max <= 0, do: state

  defp maybe_trim_history(state) do
    if state.history_count > state.max_history_size do
      trimmed = Enum.take(state.history, state.max_history_size)
      %{state | history: trimmed, history_count: state.max_history_size}
    else
      state
    end
  end

  # ── Internal: helpers ───────────────────────────────────────────────────────

  defp normalize_event_type("*"), do: :wildcard
  defp normalize_event_type(type), do: type

  defp ensure_event_id(%{event_id: nil} = event) do
    %{event | event_id: AbxBus.Event.generate_id()}
  end
  defp ensure_event_id(event), do: event

  defp ensure_event_type(%{event_type: nil} = event) do
    %{event | event_type: event.__struct__}
  end
  defp ensure_event_type(event), do: event

  defp ensure_timestamps(%{event_created_at: nil} = event) do
    %{event | event_created_at: System.monotonic_time(:nanosecond)}
  end
  defp ensure_timestamps(event), do: event

  defp append_bus_to_path(event, label) do
    if label in event.event_path do
      Logger.warning("Event #{event.event_id} already has bus #{label} in path — possible circular forwarding")
      event
    else
      %{event | event_path: event.event_path ++ [label]}
    end
  end

  defp check_backpressure(state) do
    if state.history_count >= state.max_history_size and not state.max_history_drop do
      {:error, %AbxBus.HistoryFullError{}}
    else
      :ok
    end
  end

  defp config(state) do
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
    :queue.is_empty(state.pending_queue) and MapSet.size(state.in_flight) == 0
  end

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
    case :ets.info(:abx_bus_pids) do
      :undefined -> :ok
      _ ->
        :ets.tab2list(:abx_bus_pids)
        |> Enum.each(fn {pid, _} ->
          if Process.alive?(pid), do: send(pid, :check_pending)
        end)
    end
  end

  defp maybe_mark_tree_complete(event) do
    children_ids = EventStore.children_of(event.event_id)
    all_children_complete =
      Enum.all?(children_ids, fn child_id ->
        case EventStore.get(child_id) do
          nil -> true
          child -> child.event_status in [:completed, :error]
        end
      end)

    if all_children_complete and event.event_pending_bus_count <= 0 do
      EventStore.update(event.event_id, %{event_status: :completed})
      updated = EventStore.get(event.event_id)
      EventStore.notify_waiters(event.event_id, updated)

      if event.event_parent_id do
        case EventStore.get(event.event_parent_id) do
          nil -> :ok
          parent -> maybe_mark_tree_complete(parent)
        end
      end
    end
  end
end

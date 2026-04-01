defmodule AbxBus.BusServer do
  @moduledoc """
  The brain of each event bus. A GenServer that manages:

    * The pending event queue (Erlang `:queue` — O(1) both ends)
    * Event concurrency enforcement (parallel / bus-serial / global-serial)
    * Queue-jump for awaited child events
    * In-flight and processing event tracking
    * Forwarding to other buses
    * Idle detection and `wait_until_idle/1`
    * History management with backpressure

  ## Concurrency model

  In **bus-serial** mode (default), the GenServer processes one event at a time.
  When the current event's EventWorker completes, it sends `{:event_worker_done, ...}`
  back, and only then does `maybe_process_next/1` dequeue the next event.

  In **parallel** mode, the GenServer drains the entire queue — every event gets
  its own EventWorker immediately.

  In **global-serial** mode, the GenServer coordinates with `AbxBus.LockManager`
  to ensure only ONE event runs across ALL buses.

  ## Queue-jump mechanics

  When a handler blocks on `AbxBus.await(child_event)`:

  1. The handler process sends `{:jump_queue, event_id}` to each bus where the
     child is queued.
  2. BusServer removes the event from its pending queue (if found).
  3. BusServer spawns an EventWorker for it directly — bypassing the concurrency
     policy check. This is safe because the parent handler is blocked in `receive`
     (not holding any GenServer state).
  4. When the child worker completes, the EventStore notifies the waiting handler
     process, which unblocks and continues.

  No re-entrant locks are needed. The BEAM's process model gives us this for free.
  """

  use GenServer
  require Logger

  alias AbxBus.{EventStore, EventWorker, LockManager, HandlerEntry, HandlerResult}

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
    history: [],
    max_history_size: 1000,
    max_history_drop: true,
    idle_waiters: [],
    forwarding_targets: [],
    handlers: %{},
    started: false
  ]

  # ── Client API ──────────────────────────────────────────────────────────────

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  def via(name) when is_atom(name), do: name
  def via(name), do: {:via, Registry, {AbxBus.BusRegistry, name}}

  @doc """
  Emit an event onto this bus. Non-blocking — returns the event immediately
  with `event_id` and metadata populated. The event will be processed
  asynchronously.

  If called from within a handler process, automatically sets `event_parent_id`.
  """
  def emit(bus, event) do
    GenServer.call(lookup(bus), {:emit, event})
  end

  @doc """
  Register a handler for an event type. Returns the handler entry.

  `event_type` can be a module atom or `:wildcard` (matches all events).

  ## Options

    * `:timeout` — per-handler timeout in seconds
    * `:max_attempts` — retry count
    * `:semaphore_scope` — `:none` | `:global` | `:bus`
    * `:semaphore_name` — name for the semaphore
    * `:semaphore_limit` — max concurrent invocations
    * `:handler_name` — human-readable name
  """
  def on(bus, event_type, handler_fn, opts \\ []) do
    GenServer.call(lookup(bus), {:register_handler, event_type, handler_fn, opts})
  end

  @doc """
  Add a forwarding target. All events emitted on this bus will also be
  emitted (same event_id, not a child) on the target bus.
  """
  def forward_to(source_bus, target_bus) do
    GenServer.call(lookup(source_bus), {:add_forward, target_bus})
  end

  @doc """
  Wait until the bus has no pending or in-flight events.
  Blocks the calling process.
  """
  def wait_until_idle(bus, timeout \\ :infinity) do
    GenServer.call(lookup(bus), :wait_until_idle, timeout)
  end

  @doc "Stop the bus, optionally clearing all state."
  def stop(bus, opts \\ []) do
    GenServer.call(lookup(bus), {:stop, opts})
  end

  @doc "Get the bus label (name#short_id)."
  def label(bus) do
    GenServer.call(lookup(bus), :label)
  end

  @doc "Get current in-flight event IDs."
  def in_flight_event_ids(bus) do
    GenServer.call(lookup(bus), :in_flight_event_ids)
  end

  @doc "Get current processing event IDs."
  def processing_event_ids(bus) do
    GenServer.call(lookup(bus), :processing_event_ids)
  end

  @doc "Get the pending queue size."
  def queue_size(bus) do
    GenServer.call(lookup(bus), :queue_size)
  end

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
      started: true
    }

    {:ok, state}
  end

  # ── Emit ────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:emit, event}, _from, state) do
    # Ensure event has ID and metadata
    event =
      event
      |> ensure_event_id()
      |> ensure_event_type()
      |> ensure_timestamps()
      |> append_bus_to_path(state.label)

    # Set parent from calling context (process dictionary of caller)
    # The caller's process dictionary won't be visible here (we're in the
    # GenServer process), so parent_id must be set by the caller before calling emit.

    # Check history backpressure
    case check_backpressure(state) do
      :ok ->
        # Enqueue
        new_queue = :queue.in(event, state.pending_queue)
        new_in_flight = MapSet.put(state.in_flight, event.event_id)

        # Store in ETS
        EventStore.put(event)
        EventStore.index_to_bus(state.name, event.event_id)

        # Update event's pending bus count
        EventStore.update(event.event_id, %{
          event_pending_bus_count: (event.event_pending_bus_count || 0) + 1
        })

        # Resolve find-waiters
        EventStore.resolve_find_waiters(event)

        state = %{state |
          pending_queue: new_queue,
          in_flight: new_in_flight,
          history: [event.event_id | state.history]
        }

        # Forward to target buses (same event_id — NOT a child)
        forward_event(event, state)

        # Try to process next event
        state = maybe_process_next(state)

        {:reply, event, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # ── Handler registration ────────────────────────────────────────────────────

  def handle_call({:register_handler, event_type, handler_fn, opts}, _from, state) do
    # Normalize event type: "*" or :wildcard both map to :wildcard
    event_type =
      case event_type do
        "*" -> :wildcard
        other -> other
      end

    entry = HandlerEntry.new(event_type, handler_fn, Keyword.put(opts, :bus_name, state.name))

    handlers =
      Map.update(state.handlers, event_type, [entry], fn existing ->
        existing ++ [entry]
      end)

    {:reply, entry, %{state | handlers: handlers}}
  end

  # ── Forwarding ──────────────────────────────────────────────────────────────

  def handle_call({:add_forward, target_bus}, _from, state) do
    targets = [target_bus | state.forwarding_targets] |> Enum.uniq()
    {:reply, :ok, %{state | forwarding_targets: targets}}
  end

  # ── Idle detection ──────────────────────────────────────────────────────────

  def handle_call(:wait_until_idle, from, state) do
    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | idle_waiters: [from | state.idle_waiters]}}
    end
  end

  # ── Introspection ──────────────────────────────────────────────────────────

  def handle_call(:label, _from, state) do
    {:reply, state.label, state}
  end

  def handle_call(:in_flight_event_ids, _from, state) do
    {:reply, state.in_flight, state}
  end

  def handle_call(:processing_event_ids, _from, state) do
    {:reply, state.processing, state}
  end

  def handle_call(:queue_size, _from, state) do
    {:reply, :queue.len(state.pending_queue), state}
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
  def handle_cast({:jump_queue, event_id, notify_pid, notify_ref}, state) do
    case remove_from_queue(state.pending_queue, event_id) do
      {:ok, event, new_queue} ->
        # Found in queue — process immediately, bypassing concurrency policy
        state = %{state | pending_queue: new_queue}
        state = spawn_event_worker(state, event, jump: true)
        {:noreply, state}

      :not_found ->
        # Already dequeued by normal processing. The EventWorker will handle it.
        # Nothing to do — the waiter will be notified when the worker completes.
        {:noreply, state}
    end
  end

  # ── Event worker completion ─────────────────────────────────────────────────

  def handle_info({:event_worker_done, event_id, results}, state) do
    # Update event in store
    EventStore.update_fun(event_id, fn event ->
      now = System.monotonic_time(:nanosecond)
      completed_at = Enum.max([now | for {_, r} <- results, r.completed_at, do: r.completed_at])

      %{event |
        event_results: results,
        event_status: :completed,
        event_completed_at: completed_at,
        event_pending_bus_count: max((event.event_pending_bus_count || 1) - 1, 0)
      }
    end)

    # Remove from processing and in-flight
    state = %{state |
      processing: MapSet.delete(state.processing, event_id),
      in_flight: MapSet.delete(state.in_flight, event_id)
    }

    # Release global lock if we held it
    event = EventStore.get(event_id)
    effective_concurrency = LockManager.resolve_event_concurrency(event || %{}, config(state))

    if effective_concurrency == :global_serial do
      LockManager.release_global()
    end

    # Notify completion waiters
    if event do
      maybe_mark_tree_complete(event)
    end

    # Try to process next event
    state = maybe_process_next(state)

    # Check idle
    state = maybe_notify_idle(state)

    {:noreply, state}
  end

  def handle_info({:event_worker_started, event_id}, state) do
    EventStore.update(event_id, %{event_status: :started, event_started_at: System.monotonic_time(:nanosecond)})
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ── Internal: queue processing ──────────────────────────────────────────────

  defp maybe_process_next(state) do
    if :queue.is_empty(state.pending_queue) do
      state
    else
      {{:value, event}, _rest} = :queue.peek(state.pending_queue)
      effective_concurrency = LockManager.resolve_event_concurrency(event, config(state))

      case can_proceed?(effective_concurrency, state) do
        true ->
          {{:value, event}, new_queue} = :queue.out(state.pending_queue)
          state = %{state | pending_queue: new_queue}
          state = spawn_event_worker(state, event, jump: false)

          # In parallel mode, keep draining
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
      :parallel ->
        true

      :bus_serial ->
        MapSet.size(state.processing) == 0

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

    # Spawn a linked process for this event
    spawn_link(fn ->
      EventWorker.run(event, handlers, bus_config, bus_pid, bus_name)
    end)

    %{state | processing: MapSet.put(state.processing, event.event_id)}
  end

  defp applicable_handlers(state, event) do
    event_type = event.event_type

    type_handlers = Map.get(state.handlers, event_type, [])
    wildcard_handlers = Map.get(state.handlers, :wildcard, [])

    # Filter out handlers that would create forwarding loops
    (type_handlers ++ wildcard_handlers)
    |> Enum.reject(fn entry ->
      # If this handler's bus is already in the event path (besides the current bus),
      # and the handler is a wildcard (forwarding), skip to prevent loops
      entry.event_type == :wildcard and
        entry.bus_name != state.name and
        Enum.any?(event.event_path, fn p -> String.starts_with?(p, "#{entry.bus_name}#") end)
    end)
  end

  # ── Internal: forwarding ────────────────────────────────────────────────────

  defp forward_event(event, state) do
    for target <- state.forwarding_targets do
      # Forward is NOT a child — same event_id, just adding to path
      # The target bus's emit handler will append its label to event_path
      Task.start(fn ->
        try do
          AbxBus.BusServer.emit(target, event)
        rescue
          _ -> :ok
        end
      end)
    end
  end

  # ── Internal: helpers ───────────────────────────────────────────────────────

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
    if length(state.history) >= state.max_history_size and not state.max_history_drop do
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
    :queue.is_empty(state.pending_queue) and
      MapSet.size(state.in_flight) == 0
  end

  defp maybe_notify_idle(state) do
    if idle?(state) do
      for waiter <- state.idle_waiters do
        GenServer.reply(waiter, :ok)
      end

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
      {:empty, _} ->
        :not_found

      {{:value, event}, rest} ->
        if event.event_id == event_id do
          # Found it — rebuild queue without this event
          remaining = :queue.join(acc, rest)
          {:ok, event, remaining}
        else
          remove_from_queue(rest, event_id, :queue.in(event, acc))
        end
    end
  end

  defp maybe_mark_tree_complete(event) do
    # Check if all children are complete
    children_ids = EventStore.children_of(event.event_id)
    all_children_complete =
      Enum.all?(children_ids, fn child_id ->
        case EventStore.get(child_id) do
          nil -> true
          child -> child.event_status in [:completed, :error]
        end
      end)

    if all_children_complete and event.event_pending_bus_count <= 0 do
      # Mark this event fully complete and notify waiters
      EventStore.update(event.event_id, %{event_status: :completed})
      updated = EventStore.get(event.event_id)
      EventStore.notify_waiters(event.event_id, updated)

      # Propagate up to parent
      if event.event_parent_id do
        case EventStore.get(event.event_parent_id) do
          nil -> :ok
          parent -> maybe_mark_tree_complete(parent)
        end
      end
    end
  end
end

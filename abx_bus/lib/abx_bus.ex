defmodule AbxBus do
  @moduledoc """
  OTP-native event bus with queue-jump, multi-bus forwarding, and lineage tracking.

  ## Key concepts

    * **Event concurrency** — controls how many events process simultaneously:
      `:parallel` (unlimited), `:bus_serial` (one per bus), `:global_serial` (one globally)
    * **Handler concurrency** — controls how many handlers run per event:
      `:parallel` (all at once), `:serial` (one at a time)
    * **Handler completion** — when an event is "done":
      `:all` (wait for every handler), `:first` (first non-nil result wins)
    * **Queue-jump** — when a handler awaits a child event, that child bypasses
      the pending queue and processes immediately, preventing deadlock
    * **Forwarding** — events can be forwarded to other buses (same `event_id`,
      NOT a parent-child relationship)
    * **Lineage** — parent-child tracking via `event_parent_id`, automatic when
      emitting from within a handler

  ## Quick start

      # Start a bus
      {:ok, _} = AbxBus.start_bus(:auth, event_concurrency: :bus_serial)

      # Register handlers
      AbxBus.on(:auth, MyApp.UserCreated, fn event ->
        IO.puts("User created: \#{event.username}")
        :ok
      end)

      # Emit events
      event = AbxBus.emit(:auth, MyApp.UserCreated.new(username: "alice"))

      # Wait for completion
      completed = AbxBus.await(event)

      # Or wait for the bus to be idle
      AbxBus.wait_until_idle(:auth)

  ## Queue-jump example

      AbxBus.on(:main, MyApp.ParentEvent, fn event ->
        # This child will jump the queue and process immediately
        child = AbxBus.emit(AbxBus.current_bus!(), MyApp.ChildEvent.new())
        completed_child = AbxBus.await(child)

        # Siblings stay in queue until parent handler finishes
        AbxBus.emit(AbxBus.current_bus!(), MyApp.SiblingEvent.new())
        :ok
      end)
  """

  alias AbxBus.{BusServer, EventStore}

  # ── Bus lifecycle ───────────────────────────────────────────────────────────

  @doc """
  Start a named event bus.

  ## Options

    * `:event_concurrency` — `:parallel` | `:bus_serial` (default) | `:global_serial`
    * `:event_handler_concurrency` — `:parallel` (default) | `:serial`
    * `:event_handler_completion` — `:all` (default) | `:first`
    * `:event_timeout` — default event timeout in seconds
    * `:event_handler_timeout` — default handler timeout in seconds
    * `:event_slow_timeout` — slow event warning threshold in seconds
    * `:max_history_size` — max completed events to keep (default 1000)
    * `:max_history_drop` — whether to drop old history (default true)
  """
  def start_bus(name, opts \\ []) do
    opts = Keyword.put(opts, :name, name)

    DynamicSupervisor.start_child(
      AbxBus.BusSupervisor,
      {BusServer, opts}
    )
  end

  @doc "Stop a bus."
  def stop_bus(name, opts \\ []) do
    BusServer.stop(name, opts)
  end

  # ── Event operations ────────────────────────────────────────────────────────

  @doc """
  Emit an event on a bus. Non-blocking — returns the event with metadata populated.

  If called from within a handler, `event_parent_id` is automatically set
  to the current event's ID (unless explicitly provided).

  The event will be queued and processed asynchronously according to the bus's
  concurrency policy.
  """
  def emit(bus, event) do
    # Auto-set parent from handler context before calling BusServer
    event = maybe_set_parent(event)

    # Track child in parent's children list
    maybe_track_child(event)

    BusServer.emit(bus, event)
  end

  @doc """
  Await an event's completion. Blocks the calling process until the event
  (and all its children) are complete.

  **Queue-jump**: If called from within a handler, the awaited event will
  jump the queue and process immediately. Non-awaited siblings stay queued.
  This prevents deadlock in bus-serial mode.

  Returns the completed event.
  """
  def await(event, timeout \\ :infinity) do
    # Check if already complete
    case EventStore.get(event.event_id) do
      %{event_status: :completed} = completed ->
        completed

      _ ->
        # Register as waiter
        ref = EventStore.add_waiter(event.event_id)

        # If we're inside a handler, trigger queue-jump
        if in_handler_context?() do
          trigger_queue_jump(event)
        end

        # Block until completion
        timeout_ms =
          case timeout do
            :infinity -> :infinity
            s when is_number(s) -> trunc(s * 1000)
          end

        receive do
          {:event_completed, ^ref, completed_event} ->
            completed_event
        after
          timeout_ms ->
            {:error, :timeout}
        end
    end
  end

  @doc """
  Wait for event completion WITHOUT queue-jumping.

  Unlike `await/2`, this does NOT cause the event to jump the queue.
  It simply waits for the event's completion signal passively.

  Use this when you want to observe completion without affecting
  processing order (analogous to Python's `event.event_completed()`).
  """
  def wait_for_completion(event, timeout \\ :infinity) do
    case EventStore.get(event.event_id) do
      %{event_status: :completed} = completed ->
        completed

      _ ->
        ref = EventStore.add_waiter(event.event_id)

        timeout_ms =
          case timeout do
            :infinity -> :infinity
            s when is_number(s) -> trunc(s * 1000)
          end

        receive do
          {:event_completed, ^ref, completed_event} ->
            completed_event
        after
          timeout_ms ->
            {:error, :timeout}
        end
    end
  end

  @doc """
  Get the first non-nil result from an event's handlers.
  Blocks until the event completes.

  For `:first` completion mode events, this returns the winning result.
  """
  def first(event, timeout \\ :infinity) do
    completed = await(event, timeout)

    case completed do
      {:error, _} = err ->
        err

      %{event_results: results} ->
        results
        |> Map.values()
        |> Enum.find_value(fn
          %{status: :completed, result: value} when not is_nil(value) -> value
          _ -> nil
        end)
    end
  end

  # ── Handler registration ────────────────────────────────────────────────────

  @doc """
  Register a handler for an event type on a bus.

  `event_type` can be:
    * A module atom (e.g., `MyApp.UserCreated`)
    * `"*"` or `:wildcard` to match all events

  ## Options

    * `:timeout` — per-handler timeout in seconds
    * `:max_attempts` — retry count (default 1)
    * `:handler_name` — human-readable name
    * `:semaphore_scope` — `:none` | `:global` | `:bus`
    * `:semaphore_name` — shared semaphore name
    * `:semaphore_limit` — max concurrent invocations (default 1)

  Returns the handler entry.
  """
  def on(bus, event_type, handler_fn, opts \\ []) do
    BusServer.on(bus, event_type, handler_fn, opts)
  end

  # ── Forwarding ──────────────────────────────────────────────────────────────

  @doc """
  Forward all events from source bus to target bus.

  Forwarded events keep the same `event_id` (they are NOT children).
  The target bus appends its label to `event_path`.

  This is equivalent to `bus.on('*', other_bus.emit)` in the Python version.
  """
  def forward(source_bus, target_bus) do
    BusServer.forward_to(source_bus, target_bus)
  end

  # ── Bus queries ─────────────────────────────────────────────────────────────

  @doc "Wait until a bus has no pending or in-flight events."
  def wait_until_idle(bus, timeout \\ :infinity) do
    BusServer.wait_until_idle(bus, timeout)
  end

  @doc "Get the bus label (name#short_id)."
  def bus_label(bus) do
    BusServer.label(bus)
  end

  # ── Event queries ───────────────────────────────────────────────────────────

  @doc """
  Find an event matching criteria.

  ## Options

    * `:child_of` — scope to children of a parent event
    * `:where` — `fn event -> boolean` filter
    * `:past` — `true` | seconds to search history
    * `:future` — `false` | seconds to wait for future event
    * `:event_status` — filter by status
    * Other key-value pairs match event fields exactly

  Returns the matching event or `nil`.
  """
  def find(event_type, opts \\ []) do
    EventStore.find(event_type, opts)
  end

  # ── Handler context ─────────────────────────────────────────────────────────

  @doc """
  Get the current bus name from handler context.
  Raises if called outside of an event handler.
  """
  def current_bus! do
    case Process.get(:abx_current_bus) do
      nil ->
        raise RuntimeError, "bus can only be accessed from within an event handler"

      bus ->
        bus
    end
  end

  @doc "Get the current event ID from handler context, or nil."
  def current_event_id do
    Process.get(:abx_current_event_id)
  end

  @doc "Get the current handler ID from handler context, or nil."
  def current_handler_id do
    Process.get(:abx_current_handler_id)
  end

  @doc "Check whether we're inside an event handler."
  def in_handler_context? do
    Process.get(:abx_current_event_id) != nil
  end

  # ── Internals ───────────────────────────────────────────────────────────────

  defp maybe_set_parent(%{event_parent_id: nil} = event) do
    case Process.get(:abx_current_event_id) do
      nil -> event
      parent_id -> %{event | event_parent_id: parent_id}
    end
  end

  defp maybe_set_parent(event), do: event

  defp maybe_track_child(event) do
    case event.event_parent_id do
      nil -> :ok
      parent_id -> EventStore.add_child(parent_id, event.event_id)
    end
  end

  defp trigger_queue_jump(event) do
    # Send jump_queue to each bus where this event is queued
    notify_ref = make_ref()

    for bus_label <- event.event_path do
      # Extract bus name from label (format: "name#short_id")
      bus_name =
        case String.split(bus_label, "#") do
          [name | _] -> String.to_existing_atom(name)
          _ -> bus_label
        end

      GenServer.cast(
        BusServer.via(bus_name),
        {:jump_queue, event.event_id, self(), notify_ref}
      )
    end
  end
end

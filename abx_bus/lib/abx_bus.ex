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
    * **Forwarding** — register a wildcard handler that emits to another bus:
      `AbxBus.on(:bus_a, "*", fn e -> AbxBus.emit(:bus_b, e) end)`
      Forwarded events keep the same `event_id` (NOT parent-child).
    * **Lineage** — parent-child tracking via `event_parent_id`, automatic when
      emitting from within a handler

  ## Quick start

      {:ok, _} = AbxBus.start_bus(:auth, event_concurrency: :bus_serial)

      AbxBus.on(:auth, MyApp.UserCreated, fn event ->
        IO.puts("User created: \#{event.username}")
        :ok
      end)

      event = AbxBus.emit(:auth, MyApp.UserCreated.new(username: "alice"))
      completed = AbxBus.await(event)
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
    * `:event_handler_slow_timeout` — slow handler warning threshold in seconds
    * `:max_history_size` — max completed events to keep (default 1000)
    * `:max_history_drop` — whether to drop old history (default true)
    * `:middlewares` — list of middleware modules (default [])
  """
  def start_bus(name, opts \\ []) do
    opts = Keyword.put(opts, :name, name)

    DynamicSupervisor.start_child(
      AbxBus.BusSupervisor,
      {BusServer, opts}
    )
  end

  @doc "Stop a bus."
  def stop(name, opts \\ []) do
    BusServer.stop(name, opts)
  end

  # ── Event operations ────────────────────────────────────────────────────────

  @doc """
  Emit an event on a bus. Non-blocking — returns the event with metadata populated.

  If called from within a handler, `event_parent_id` is automatically set
  to the current event's ID (unless explicitly provided).
  """
  def emit(bus, event) do
    event = maybe_set_parent(event)
    maybe_track_child(event)
    BusServer.emit(bus, event)
  end

  @doc """
  Await an event's completion. Blocks the calling process until the event
  (and all its children) are complete.

  **Queue-jump**: If called from within a handler, the awaited event will
  jump the queue and process immediately. Non-awaited siblings stay queued.

  Returns the completed event.
  """
  def await(event, timeout \\ :infinity) do
    # Register waiter FIRST to prevent TOCTOU race
    ref = EventStore.add_waiter(event.event_id)

    case EventStore.get(event.event_id) do
      %{event_status: :completed} = completed ->
        # Already complete — drain any notification sent between registration and check
        receive do
          {:event_completed, ^ref, _} -> :ok
        after
          0 -> :ok
        end
        completed

      _ ->
        if in_handler_context?() do
          trigger_queue_jump(event)
        end

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
  Analogous to Python's `event.event_completed()`.
  """
  def event_completed(event, timeout \\ :infinity) do
    ref = EventStore.add_waiter(event.event_id)

    case EventStore.get(event.event_id) do
      %{event_status: :completed} = completed ->
        receive do
          {:event_completed, ^ref, _} -> :ok
        after
          0 -> :ok
        end
        completed

      _ ->
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
  """
  def first(event, timeout \\ :infinity) do
    completed = await(event, timeout)

    case completed do
      {:error, _} = err ->
        err

      %{event_results: results} ->
        found =
          results
          |> Map.values()
          |> Enum.find(fn
            %{status: :completed, result: value} when not is_nil(value) -> true
            _ -> false
          end)

        case found do
          %{result: value} -> value
          nil -> nil
        end
    end
  end

  @doc """
  Get the first handler result (value only). Blocks until complete.

  Unlike `first/2`, this returns `nil` results too — it's the value of
  the first completed handler regardless of the value.
  """
  def event_result(event, timeout \\ :infinity) do
    completed = await(event, timeout)

    case completed do
      {:error, _} = err -> err
      %{event_results: results} ->
        results
        |> Map.values()
        |> Enum.find(fn r -> r.status == :completed end)
        |> case do
          nil -> nil
          r -> r.result
        end
    end
  end

  @doc """
  Get all handler results as a list with optional filtering.

  ## Options

    * `:where` — `fn result -> boolean` custom filter
    * `:raise_if_any` — raise if any result has `:error` status
    * `:raise_if_none` — raise if no results pass the filter
    * `:include` — filter by status, one of `:all`, `:completed`, `:errors`
  """
  def event_results_list(event, opts \\ []) do
    stored = EventStore.get(event.event_id) || event
    results = Map.values(stored.event_results || %{})

    # Filter by include mode
    results =
      case Keyword.get(opts, :include, :completed) do
        :all -> results
        :completed -> Enum.filter(results, &(&1.status == :completed))
        :errors -> Enum.filter(results, &(&1.status == :error))
      end

    # Custom filter
    results =
      case Keyword.get(opts, :where) do
        nil -> results
        fun -> Enum.filter(results, fun)
      end

    # Raise checks
    if Keyword.get(opts, :raise_if_any, false) do
      errors = Enum.filter(results, &(&1.status == :error))
      if errors != [] do
        first_error = hd(errors)
        raise first_error.error || %RuntimeError{message: "Handler error"}
      end
    end

    if Keyword.get(opts, :raise_if_none, false) and results == [] do
      raise %RuntimeError{message: "No results after filtering"}
    end

    Enum.map(results, & &1.result)
  end

  @doc """
  Create a fresh pending copy of an event for re-emission.
  Preserves user payload fields; resets all runtime metadata.
  """
  def event_reset(event), do: AbxBus.Event.reset(event)

  # ── Handler registration ────────────────────────────────────────────────────

  @doc """
  Register a handler for an event type on a bus.

  `event_type` can be:
    * A module atom (e.g., `MyApp.UserCreated`)
    * A string type name (e.g., `"UserCreated"`)
    * `"*"` or `:wildcard` to match all events

  ## Options

    * `:timeout` — per-handler timeout in seconds
    * `:handler_slow_timeout` — slow handler warning threshold
    * `:max_attempts` — retry count (default 1)
    * `:retry_after` — base delay between retries in seconds (default 0)
    * `:retry_backoff_factor` — exponential multiplier (default 1.0)
    * `:retry_on_errors` — list of error modules/matchers to retry on
    * `:handler_name` — human-readable name
    * `:semaphore_scope` — `:none` | `:global` | `:bus`
    * `:semaphore_name` — shared semaphore name
    * `:semaphore_limit` — max concurrent invocations (default 1)

  Returns the handler entry.
  """
  def on(bus, event_type, handler_fn, opts \\ []) do
    BusServer.on(bus, event_type, handler_fn, opts)
  end

  @doc """
  Unregister handlers.

    * `off(bus, event_type)` — remove ALL handlers for this event type
    * `off(bus, event_type, handler_fn)` — remove handler matching this function
    * `off(bus, handler_id: id)` — remove handler by its unique ID
  """
  def off(bus, event_type_or_opts) do
    BusServer.off(bus, event_type_or_opts)
  end

  def off(bus, event_type, handler_fn) do
    BusServer.off(bus, event_type, handler_fn)
  end

  @doc "Alias for `emit/2` (matches Python's `bus.dispatch()`)."
  def dispatch(bus, event), do: emit(bus, event)

  # ── Bus queries ─────────────────────────────────────────────────────────────

  @doc "Wait until a bus has no pending or in-flight events."
  def wait_until_idle(bus, timeout \\ :infinity) do
    BusServer.wait_until_idle(bus, timeout)
  end

  @doc "Get the bus label (name#short_id)."
  def label(bus), do: BusServer.label(bus)

  @doc "List pending events."
  def events_pending(bus), do: BusServer.events_pending(bus)

  @doc "List started (in-progress) events."
  def events_started(bus), do: BusServer.events_started(bus)

  @doc "List completed events."
  def events_completed(bus), do: BusServer.events_completed(bus)

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
  """
  def find(event_type, opts \\ []) do
    EventStore.find(event_type, opts)
  end

  # ── Lineage queries ────────────────────────────────────────────────────────

  @doc "Check whether `child` is a descendant of `parent`."
  def event_is_child_of(child, parent), do: AbxBus.Tree.child_of?(child, parent)

  @doc "Check whether `parent` is an ancestor of `child`."
  def event_is_parent_of(parent, child), do: AbxBus.Tree.parent_of?(parent, child)

  @doc """
  Render an ASCII tree of the event hierarchy.

  ## Options

    * `:max_depth` — maximum depth (default: 10)
    * `:show_timing` — include timing info (default: true)
    * `:show_results` — include handler results (default: false)
  """
  def log_tree(event, opts \\ []), do: AbxBus.Tree.log_tree(event, opts)

  # ── Handler context ─────────────────────────────────────────────────────────

  @doc """
  Get the current bus name from handler context.
  Raises if called outside of an event handler.
  """
  def current_bus! do
    case Process.get(:abx_current_bus) do
      nil -> raise RuntimeError, "bus can only be accessed from within an event handler"
      bus -> bus
    end
  end

  @doc "Get the current event ID from handler context, or nil."
  def current_event_id, do: Process.get(:abx_current_event_id)

  @doc "Get the current handler ID from handler context, or nil."
  def current_handler_id, do: Process.get(:abx_current_handler_id)

  @doc "Check whether we're inside an event handler."
  def in_handler_context?, do: Process.get(:abx_current_event_id) != nil

  # ── Internals ───────────────────────────────────────────────────────────────

  defp maybe_set_parent(%{event_parent_id: nil} = event) do
    # If event already has entries in event_path, it's being forwarded
    # (same event re-emitted to another bus) — don't set parent.
    if event.event_path != [] do
      event
    else
      case Process.get(:abx_current_event_id) do
        nil -> event
        parent_id -> %{event | event_parent_id: parent_id}
      end
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
    notify_ref = make_ref()

    for bus_label <- event.event_path do
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

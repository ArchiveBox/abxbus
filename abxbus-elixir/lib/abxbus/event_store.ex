defmodule Abxbus.EventStore do
  @moduledoc """
  ETS-backed event registry.

  Performance-critical operations (put, get, index) go directly to ETS.
  Only true read-modify-write operations that need atomicity (update_fun,
  put_or_merge for forwarded events) serialize through the GenServer.

  ## Tables

    * `:abxbus_events`         — `{event_id, event_map}` (set)
    * `:abxbus_event_children`  — `{parent_id, child_id}` (bag)
    * `:abxbus_event_waiters`   — `{event_id, {pid, ref}}` (bag)
    * `:abxbus_find_waiters`    — `{event_type, {pid, ref, opts}}` (bag)
    * `:abxbus_bus_events`      — `{bus_name, event_id}` (bag)
    * `:abxbus_worker_results`  — `{key, results_map}` (set)
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ── Direct ETS operations (no serialization needed) ────────────────────────

  @doc "Retrieve an event by ID. Direct ETS read."
  def get(event_id) do
    case :ets.lookup(:abxbus_events, event_id) do
      [{^event_id, event}] -> event
      [] -> nil
    end
  end

  @doc "Get all child event IDs for a parent."
  def children_of(parent_id) do
    :ets.lookup(:abxbus_event_children, parent_id)
    |> Enum.map(fn {_, child_id} -> child_id end)
  end

  @doc "Get all event IDs for a bus."
  def events_for_bus(bus_name) do
    :ets.lookup(:abxbus_bus_events, bus_name)
    |> Enum.map(fn {_, event_id} -> event_id end)
  end

  @doc "Store an event. Direct ETS write (idempotent)."
  def put(event) do
    :ets.insert(:abxbus_events, {event.event_id, event})
    :ok
  end

  @doc """
  Fast-path upsert: tries insert_new (atomic, no GenServer hop).
  Falls back to GenServer only for the rare merge case (forwarded events).
  """
  def put_or_merge(event) do
    inserted = %{event | event_pending_bus_count: 1}

    case :ets.insert_new(:abxbus_events, {event.event_id, inserted}) do
      true ->
        # New event — fast path, no GenServer needed
        index_by_type(event.event_type, event.event_id)
        :ok

      false ->
        # Already exists (forwarded event) — need atomic merge
        GenServer.call(__MODULE__, {:merge_forwarded, event})
    end
  end

  defp index_by_type(nil, _event_id), do: :ok
  defp index_by_type(event_type, event_id) do
    :ets.insert(:abxbus_events_by_type, {event_type, event_id})
  end

  @doc "Update specific fields on a stored event. Direct ETS read-modify-write."
  def update(event_id, updates) when is_map(updates) do
    case :ets.lookup(:abxbus_events, event_id) do
      [{^event_id, event}] ->
        updated = Map.merge(event, updates)
        :ets.insert(:abxbus_events, {event_id, updated})
        {:ok, updated}
      [] ->
        :error
    end
  end

  @doc "Atomically update an event via a transform function. Through GenServer for safety."
  def update_fun(event_id, fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:update_fun, event_id, fun})
  end

  @doc "Record a parent-child relationship. Direct ETS."
  def add_child(parent_id, child_id) do
    :ets.insert(:abxbus_event_children, {parent_id, child_id})
    :ok
  end

  @doc "Index an event to a bus. Direct ETS."
  def index_to_bus(bus_name, event_id) do
    :ets.insert(:abxbus_bus_events, {bus_name, event_id})
    :ok
  end

  # ── Completion waiters (direct ETS — atomic via :ets.take) ─────────────────

  @doc "Register completion waiter. Returns ref for matching."
  def add_waiter(event_id, pid \\ self()) do
    ref = make_ref()
    :ets.insert(:abxbus_event_waiters, {event_id, {pid, ref}})
    ref
  end

  @doc "Atomically read and remove all waiters, then notify them."
  def notify_waiters(event_id, event) do
    waiters = :ets.take(:abxbus_event_waiters, event_id)

    for {_, {pid, ref}} <- waiters do
      send(pid, {:event_completed, ref, event})
    end

    length(waiters)
  end

  @doc "Remove a specific waiter by event_id and ref (cleanup on timeout)."
  def remove_waiter(event_id, ref) do
    :ets.match_delete(:abxbus_event_waiters, {event_id, {:_, ref}})
    :ok
  end

  # ── Find waiters ───────────────────────────────────────────────────────────

  def add_find_waiter(event_type, opts, pid \\ self()) do
    ref = make_ref()
    :ets.insert(:abxbus_find_waiters, {event_type, {pid, ref, opts}})
    ref
  end

  def remove_find_waiter(ref) do
    :ets.tab2list(:abxbus_find_waiters)
    |> Enum.each(fn {_, {_, r, _}} = entry ->
      if r == ref, do: :ets.delete_object(:abxbus_find_waiters, entry)
    end)
    :ok
  end

  @doc """
  Check find-waiters against a newly emitted event. Fast path: skip entirely
  if no waiters registered. Slow path through GenServer only when waiters exist.
  """
  def resolve_find_waiters(event) do
    # Fast path: check if ANY find waiters exist before doing expensive work
    case :ets.info(:abxbus_find_waiters, :size) do
      0 -> :ok
      _ -> GenServer.call(__MODULE__, {:resolve_find_waiters, event})
    end
  end

  # ── Find (past + future search) ────────────────────────────────────────────

  @doc "Search for events matching criteria. See Abxbus.find/2 for options."
  def find(event_type, opts \\ []) do
    past = Keyword.get(opts, :past, true)
    future = Keyword.get(opts, :future, false)

    # For future searches: register waiter FIRST to avoid TOCTOU race
    future_ref =
      if future != false do
        add_find_waiter(normalize_type(event_type), opts)
      end

    # Phase 1: search past events
    result =
      if past do
        cutoff =
          case past do
            true -> :no_cutoff
            seconds when is_number(seconds) ->
              System.monotonic_time(:nanosecond) - trunc(seconds * 1_000_000_000)
          end

        search_past(normalize_type(event_type), cutoff, opts)
      end

    case {result, future, future_ref} do
      {nil, false, _} ->
        nil

      {nil, _, ref} ->
        timeout_ms =
          case future do
            true -> :infinity
            s when is_number(s) -> trunc(s * 1000)
          end

        receive do
          {:find_match, ^ref, event} -> event
        after
          timeout_ms ->
            remove_find_waiter(ref)
            nil
        end

      {event, _, ref} ->
        if ref, do: remove_find_waiter(ref)
        event
    end
  end

  # ── Internals ───────────────────────────────────────────────────────────────

  defp search_past(event_type, cutoff, opts) do
    # Use ETS select with match spec to filter by event_type at the ETS level.
    # This avoids O(n) scans when the events table has many unrelated entries.
    candidates = select_by_type(event_type)

    candidates
    |> Enum.filter(fn event -> matches_all?(event, event_type, cutoff, opts) end)
    |> Enum.max_by(fn event -> event.event_created_at || 0 end, fn -> nil end)
  end

  # O(k) lookup by event_type using the secondary index, where k is the
  # number of events of that type (not the total number of events).
  defp select_by_type(:wildcard) do
    # Wildcard needs all events — use a full scan (rare case)
    :ets.select(:abxbus_events, [{{:_, :"$1"}, [], [:"$1"]}])
  end

  defp select_by_type(type) when is_atom(type) do
    # Lookup event_ids for this type, then fetch each event
    :abxbus_events_by_type
    |> :ets.lookup(type)
    |> Enum.flat_map(fn {_type, event_id} ->
      case :ets.lookup(:abxbus_events, event_id) do
        [{_, event}] -> [event]
        [] -> []
      end
    end)
  end

  defp select_by_type(type) when is_binary(type) do
    # String type — iterate all keys in the index (cheap — keys are atoms)
    # and match by Module short name. Much smaller than scanning all events.
    :abxbus_events_by_type
    |> :ets.tab2list()
    |> Enum.reduce(%{}, fn {k, _v}, acc -> Map.put(acc, k, true) end)
    |> Map.keys()
    |> Enum.filter(fn
      k when is_atom(k) ->
        case Code.ensure_loaded?(k) do
          true -> Module.split(k) |> List.last() == type
          false -> Atom.to_string(k) == type
        end
      _ -> false
    end)
    |> Enum.flat_map(fn matching_atom -> select_by_type(matching_atom) end)
  end

  defp matches_all?(event, event_type, cutoff, opts) do
    matches_type?(event, event_type) and
      matches_cutoff?(event, cutoff) and
      matches_child_of_event?(event, opts) and
      matches_status?(event, opts) and
      matches_where?(event, opts) and
      matches_metadata?(event, opts)
  end

  defp matches_type?(_event, :wildcard), do: true
  defp matches_type?(event, type) when is_atom(type), do: event.event_type == type

  defp matches_type?(event, type) when is_binary(type) do
    event_type_name = event.event_type |> Module.split() |> List.last()
    event_type_name == type
  end

  defp matches_cutoff?(_event, :no_cutoff), do: true
  defp matches_cutoff?(event, cutoff), do: (event.event_created_at || 0) >= cutoff

  defp matches_child_of_event?(event, opts) do
    case Keyword.get(opts, :child_of) do
      nil -> true
      parent when is_map(parent) -> is_descendant_of?(event, parent.event_id)
      parent_id when is_binary(parent_id) -> is_descendant_of?(event, parent_id)
    end
  end

  defp is_descendant_of?(%{event_parent_id: nil}, _ancestor_id), do: false
  defp is_descendant_of?(%{event_parent_id: pid}, ancestor_id) when pid == ancestor_id, do: true
  defp is_descendant_of?(%{event_parent_id: pid}, ancestor_id) do
    case :ets.lookup(:abxbus_events, pid) do
      [{_, parent}] -> is_descendant_of?(parent, ancestor_id)
      [] -> false
    end
  end

  defp matches_status?(event, opts) do
    case Keyword.get(opts, :event_status) do
      nil -> true
      status -> event.event_status == status
    end
  end

  defp matches_where?(event, opts) do
    case Keyword.get(opts, :where) do
      nil -> true
      fun when is_function(fun, 1) -> fun.(event)
    end
  end

  defp matches_metadata?(event, opts) do
    reserved = [:child_of, :where, :past, :future, :event_status, :bus_name]

    opts
    |> Keyword.drop(reserved)
    |> Enum.all?(fn {key, val} -> Map.get(event, key) == val end)
  end

  defp matches_find_criteria?(event, opts) do
    matches_child_of_event?(event, opts) and
      matches_status?(event, opts) and
      matches_where?(event, opts) and
      matches_metadata?(event, opts)
  end

  defp normalize_type(:wildcard), do: :wildcard
  defp normalize_type("*"), do: :wildcard
  defp normalize_type(type), do: type

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    :ets.new(:abxbus_events, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(:abxbus_events_by_type, [:duplicate_bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_event_children, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_event_waiters, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_find_waiters, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_bus_events, [:duplicate_bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_worker_results, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(:abxbus_event_depth, [:set, :public, :named_table, write_concurrency: true])
    {:ok, %{}}
  end

  # Only used for the forwarded-event merge path
  @impl true
  def handle_call({:merge_forwarded, event}, _from, state) do
    case :ets.lookup(:abxbus_events, event.event_id) do
      [{_, existing}] ->
        merged_path = Enum.uniq(existing.event_path ++ event.event_path)
        updated = %{existing |
          event_path: merged_path,
          event_pending_bus_count: (existing.event_pending_bus_count || 0) + 1
        }
        :ets.insert(:abxbus_events, {event.event_id, updated})
        {:reply, :ok, state}

      [] ->
        # Raced — insert now
        inserted = %{event | event_pending_bus_count: 1}
        :ets.insert(:abxbus_events, {event.event_id, inserted})
        {:reply, :ok, state}
    end
  end

  def handle_call({:put, event}, _from, state) do
    :ets.insert(:abxbus_events, {event.event_id, event})
    {:reply, :ok, state}
  end

  def handle_call({:update_fun, event_id, fun}, _from, state) do
    result =
      case :ets.lookup(:abxbus_events, event_id) do
        [{^event_id, event}] ->
          updated = fun.(event)
          :ets.insert(:abxbus_events, {event_id, updated})
          {:ok, updated}
        [] ->
          :error
      end
    {:reply, result, state}
  end

  def handle_call({:resolve_find_waiters, event}, _from, state) do
    event_type = event.event_type

    # Also check string short-name key (e.g. "UserCreated" for MyApp.UserCreated)
    # to match find waiters registered with string type names
    type_keys = [event_type, :wildcard]
    type_keys =
      if is_atom(event_type) and event_type != nil do
        type_keys ++ [event_type |> Module.split() |> List.last()]
      else
        type_keys
      end

    for type_key <- type_keys do
      waiters = :ets.lookup(:abxbus_find_waiters, type_key)

      for {_, {pid, ref, opts}} = entry <- waiters do
        matches? =
          try do
            matches_find_criteria?(event, opts)
          rescue
            _ -> false
          end

        if matches? do
          :ets.delete_object(:abxbus_find_waiters, entry)
          send(pid, {:find_match, ref, event})
        end
      end
    end

    {:reply, :ok, state}
  end
end

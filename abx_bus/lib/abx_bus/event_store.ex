defmodule AbxBus.EventStore do
  @moduledoc """
  ETS-backed event registry with serialized updates.

  All mutations go through the GenServer to prevent concurrent read-modify-write
  races. Reads are direct ETS lookups for performance.

  ## Tables

    * `:abx_events`         — `{event_id, event_map}` (set)
    * `:abx_event_children`  — `{parent_id, child_id}` (bag)
    * `:abx_event_waiters`   — `{event_id, {pid, ref}}` (bag)
    * `:abx_find_waiters`    — `{event_type, {pid, ref, opts}}` (bag)
    * `:abx_bus_events`      — `{bus_name, event_id}` (bag)
    * `:abx_worker_results`  — `{key, results_map}` (set) — shared timeout state
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ── Direct ETS reads (no serialization needed) ─────────────────────────────

  @doc "Retrieve an event by ID."
  def get(event_id) do
    case :ets.lookup(:abx_events, event_id) do
      [{^event_id, event}] -> event
      [] -> nil
    end
  end

  @doc "Get all child event IDs for a parent."
  def children_of(parent_id) do
    :ets.lookup(:abx_event_children, parent_id)
    |> Enum.map(fn {_, child_id} -> child_id end)
  end

  @doc "Get all event IDs for a bus."
  def events_for_bus(bus_name) do
    :ets.lookup(:abx_bus_events, bus_name)
    |> Enum.map(fn {_, event_id} -> event_id end)
  end

  # ── Serialized mutations (through GenServer) ───────────────────────────────

  @doc "Store or update an event in the registry."
  def put(event) do
    GenServer.call(__MODULE__, {:put, event})
  end

  @doc "Update specific fields on a stored event."
  def update(event_id, updates) when is_map(updates) do
    GenServer.call(__MODULE__, {:update, event_id, updates})
  end

  @doc "Atomically update an event via a transform function."
  def update_fun(event_id, fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:update_fun, event_id, fun})
  end

  @doc "Record a parent-child relationship."
  def add_child(parent_id, child_id) do
    :ets.insert(:abx_event_children, {parent_id, child_id})
    :ok
  end

  @doc "Index an event to a bus."
  def index_to_bus(bus_name, event_id) do
    :ets.insert(:abx_bus_events, {bus_name, event_id})
    :ok
  end

  # ── Completion waiters ──────────────────────────────────────────────────────

  @doc "Register completion waiter. Returns ref for matching."
  def add_waiter(event_id, pid \\ self()) do
    ref = make_ref()
    :ets.insert(:abx_event_waiters, {event_id, {pid, ref}})
    ref
  end

  @doc "Notify all waiters and remove their entries."
  def notify_waiters(event_id, event) do
    waiters = :ets.lookup(:abx_event_waiters, event_id)
    :ets.delete(:abx_event_waiters, event_id)

    for {_, {pid, ref}} <- waiters do
      send(pid, {:event_completed, ref, event})
    end

    length(waiters)
  end

  # ── Find waiters (future search) ───────────────────────────────────────────

  def add_find_waiter(event_type, opts, pid \\ self()) do
    ref = make_ref()
    :ets.insert(:abx_find_waiters, {event_type, {pid, ref, opts}})
    ref
  end

  def remove_find_waiter(ref) do
    # Scan and remove entry with this ref
    for type_key <- [:abx_find_waiters] do
      :ets.tab2list(:abx_find_waiters)
      |> Enum.each(fn {_, {_, r, _}} = entry ->
        if r == ref, do: :ets.delete_object(:abx_find_waiters, entry)
      end)
    end
    :ok
  end

  def resolve_find_waiters(event) do
    event_type = event.event_type

    for type_key <- [event_type, :wildcard] do
      waiters = :ets.lookup(:abx_find_waiters, type_key)

      for {_, {pid, ref, opts}} = entry <- waiters do
        if matches_find_criteria?(event, opts) do
          :ets.delete_object(:abx_find_waiters, entry)
          send(pid, {:find_match, ref, event})
        end
      end
    end

    :ok
  end

  # ── Find (past + future search) ────────────────────────────────────────────

  @doc "Search for events matching criteria. See AbxBus.find/2 for options."
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

        search_past(event_type, cutoff, opts)
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
        # Found in past — clean up future waiter if registered
        if ref, do: remove_find_waiter(ref)
        event
    end
  end

  # ── Internals ───────────────────────────────────────────────────────────────

  defp search_past(event_type, cutoff, opts) do
    :ets.tab2list(:abx_events)
    |> Enum.find_value(fn {_id, event} ->
      if matches_all?(event, event_type, cutoff, opts), do: event
    end)
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
      parent when is_map(parent) -> event.event_parent_id == parent.event_id
      parent_id when is_binary(parent_id) -> event.event_parent_id == parent_id
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
    :ets.new(:abx_events, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(:abx_event_children, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abx_event_waiters, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abx_find_waiters, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abx_bus_events, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(:abx_worker_results, [:set, :public, :named_table, write_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, event}, _from, state) do
    :ets.insert(:abx_events, {event.event_id, event})
    {:reply, :ok, state}
  end

  def handle_call({:update, event_id, updates}, _from, state) do
    result =
      case :ets.lookup(:abx_events, event_id) do
        [{^event_id, event}] ->
          updated = Map.merge(event, updates)
          :ets.insert(:abx_events, {event_id, updated})
          {:ok, updated}
        [] ->
          :error
      end
    {:reply, result, state}
  end

  def handle_call({:update_fun, event_id, fun}, _from, state) do
    result =
      case :ets.lookup(:abx_events, event_id) do
        [{^event_id, event}] ->
          updated = fun.(event)
          :ets.insert(:abx_events, {event_id, updated})
          {:ok, updated}
        [] ->
          :error
      end
    {:reply, result, state}
  end
end

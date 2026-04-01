defmodule Abxbus.CrossRuntimeFeaturesTest do
  @moduledoc """
  Tests for cross-runtime parity features: queue-jump with lineage,
  parallel events with serial handlers, timeout enforcement,
  zero-history backpressure, and context propagation.

  Port of tests/subtests/test_eventbus_cross_runtime_features.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(CRParentEvent, label: "root")
  defevent(CRChildEvent, label: "child")
  defevent(CRSiblingEvent, label: "sibling")
  defevent(CRTimeoutEvent, event_timeout: 0.1)
  defevent(CRParallelSerialEvent, label: nil)
  defevent(CRHistoryEvent)

  describe "queue-jump preserves lineage" do
    test "child dispatched inside handler has correct parent and runs first" do
      {:ok, _} = Abxbus.start_bus(:cr_lineage,
        event_concurrency: :bus_serial,
        event_handler_concurrency: :serial
      )

      log = Agent.start_link(fn -> [] end) |> elem(1)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:cr_lineage, CRParentEvent, fn event ->
        Agent.update(log, &(&1 ++ ["root:start"]))
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))

        child = Abxbus.emit(:cr_lineage, CRChildEvent.new())
        Abxbus.await(child)

        # Also emit a non-awaited sibling
        Abxbus.emit(:cr_lineage, CRSiblingEvent.new())

        Agent.update(log, &(&1 ++ ["root:end"]))
        :ok
      end)

      Abxbus.on(:cr_lineage, CRChildEvent, fn event ->
        Agent.update(log, &(&1 ++ ["child"]))
        Agent.update(ids, &Map.put(&1, :child, event.event_id))
        :ok
      end)

      Abxbus.on(:cr_lineage, CRSiblingEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["sibling"]))
        :ok
      end)

      Abxbus.emit(:cr_lineage, CRParentEvent.new())
      Abxbus.wait_until_idle(:cr_lineage)

      order = Agent.get(log, & &1)
      captured = Agent.get(ids, & &1)

      assert order == ["root:start", "child", "root:end", "sibling"]

      # Verify lineage
      child_stored = Abxbus.EventStore.get(captured.child)
      assert child_stored.event_parent_id == captured.parent
    end
  end

  describe "parallel events, serial handlers" do
    test "multiple events run concurrently but each event's handlers are serial" do
      {:ok, _} = Abxbus.start_bus(:cr_ps,
        event_concurrency: :parallel,
        event_handler_concurrency: :serial
      )

      # Track per-event concurrency using event_id-keyed ETS counters
      :ets.new(:cr_ps_per_event, [:set, :public, :named_table])
      global_counter = :counters.new(1, [:atomics])
      global_max = :atomics.new(1, [])

      handler = fn event ->
        eid = event.event_id

        # Per-event counter: increment
        per_event = case :ets.lookup(:cr_ps_per_event, {eid, :count}) do
          [] -> :ets.insert(:cr_ps_per_event, {{eid, :count}, 1}); 1
          [{_, c}] -> :ets.insert(:cr_ps_per_event, {{eid, :count}, c + 1}); c + 1
        end

        # Track per-event max
        case :ets.lookup(:cr_ps_per_event, {eid, :max}) do
          [] -> :ets.insert(:cr_ps_per_event, {{eid, :max}, per_event})
          [{_, m}] when per_event > m -> :ets.insert(:cr_ps_per_event, {{eid, :max}, per_event})
          _ -> :ok
        end

        # Global counter
        :counters.add(global_counter, 1, 1)
        g = :counters.get(global_counter, 1)
        old_g = :atomics.get(global_max, 1)
        if g > old_g, do: :atomics.put(global_max, 1, g)

        Process.sleep(10)

        # Per-event counter: decrement
        [{_, c}] = :ets.lookup(:cr_ps_per_event, {eid, :count})
        :ets.insert(:cr_ps_per_event, {{eid, :count}, c - 1})
        :counters.add(global_counter, 1, -1)
        :ok
      end

      Abxbus.on(:cr_ps, CRParallelSerialEvent, handler, handler_name: "h1")
      Abxbus.on(:cr_ps, CRParallelSerialEvent, handler, handler_name: "h2")

      e1 = Abxbus.emit(:cr_ps, CRParallelSerialEvent.new(label: "e1"))
      e2 = Abxbus.emit(:cr_ps, CRParallelSerialEvent.new(label: "e2"))

      Abxbus.wait_until_idle(:cr_ps)

      # Each event's handlers should run serially (max 1 handler at a time per event)
      e1_max = case :ets.lookup(:cr_ps_per_event, {e1.event_id, :max}) do
        [{_, m}] -> m
        [] -> 0
      end
      e2_max = case :ets.lookup(:cr_ps_per_event, {e2.event_id, :max}) do
        [{_, m}] -> m
        [] -> 0
      end

      assert e1_max == 1, "Event 1 handlers should run serially (max 1), got #{e1_max}"
      assert e2_max == 1, "Event 2 handlers should run serially (max 1), got #{e2_max}"

      # But globally, multiple events should overlap (parallel event concurrency)
      assert :atomics.get(global_max, 1) >= 2,
             "Parallel events should overlap globally"

      :ets.delete(:cr_ps_per_event)
    end
  end

  describe "timeout enforcement" do
    test "timed-out event completes with error results" do
      {:ok, _} = Abxbus.start_bus(:cr_to)

      Abxbus.on(:cr_to, CRTimeoutEvent, fn _event ->
        Process.sleep(100)
        "should_not_return"
      end, handler_name: "slow")

      event = Abxbus.emit(:cr_to, CRTimeoutEvent.new())
      Abxbus.wait_until_idle(:cr_to)

      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_status == :completed

      results = Map.values(stored.event_results)
      assert length(results) > 0

      for r <- results do
        assert r.status == :error
      end

      # Queue should be clean
      assert Abxbus.BusServer.queue_size(:cr_to) == 0
      assert MapSet.size(Abxbus.BusServer.in_flight_event_ids(:cr_to)) == 0
    end
  end

  describe "zero-history backpressure" do
    test "max_history_size=0 with drop=true still processes events" do
      {:ok, _} = Abxbus.start_bus(:cr_zh,
        max_history_size: 0,
        max_history_drop: true
      )

      counter = :counters.new(1, [:atomics])

      Abxbus.on(:cr_zh, CRHistoryEvent, fn _event ->
        :counters.add(counter, 1, 1)
        :ok
      end)

      Abxbus.emit(:cr_zh, CRHistoryEvent.new())
      Abxbus.emit(:cr_zh, CRHistoryEvent.new())
      Abxbus.emit(:cr_zh, CRHistoryEvent.new())

      Abxbus.wait_until_idle(:cr_zh)

      assert :counters.get(counter, 1) == 3
    end
  end
end

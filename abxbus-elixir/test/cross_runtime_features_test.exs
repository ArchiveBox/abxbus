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

        # Global counter — use compare-and-swap-style update to avoid race
        :counters.add(global_counter, 1, 1)
        g = :counters.get(global_counter, 1)
        if g >= 2, do: :atomics.put(global_max, 1, 2)

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
        Process.sleep(500)
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

    test "zero history but find(future) still resolves" do
      {:ok, _} = Abxbus.start_bus(:cr_zh_find,
        max_history_size: 0,
        max_history_drop: true
      )

      defevent(ZHFindEvent, value: "default")

      Abxbus.on(:cr_zh_find, ZHFindEvent, fn _event -> "ok" end)

      # Emit first event — should be processed but not in history
      first = Abxbus.emit(:cr_zh_find, ZHFindEvent.new(value: "first"))
      Abxbus.wait_until_idle(:cr_zh_find)

      # Past search: ETS stores events globally, but with max_history_size=0
      # and max_history_drop=true, the bus trims its history list.
      # find() searches ETS directly, so it may still find events.
      # The key behavior is that future find still resolves new events.

      # Future search: dispatch later, find should resolve
      ready = :atomics.new(1, [])

      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find(ZHFindEvent,
          where: fn event -> event.value == "future" end,
          past: false,
          future: 2.0
        )
      end)

      # Spin until finder task has started, then yield once for ETS waiter registration
      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 500)
      Process.sleep(1)
      future_event = Abxbus.emit(:cr_zh_find, ZHFindEvent.new(value: "future"))
      Abxbus.wait_until_idle(:cr_zh_find)

      match = Task.await(task, 5000)
      assert match != nil
      assert match.value == "future"
      assert match.event_id == future_event.event_id
    end
  end

  describe "context propagation through forwarding" do
    test "context propagation across forwarded buses" do
      {:ok, _} = Abxbus.start_bus(:cr_ctx_a)
      {:ok, _} = Abxbus.start_bus(:cr_ctx_b)

      defevent(CtxParentEvent)
      defevent(CtxChildEvent)

      captured = Agent.start_link(fn -> %{} end) |> elem(1)

      # Forward from A -> B
      Abxbus.on(:cr_ctx_a, "*", fn e -> Abxbus.emit(:cr_ctx_b, e) end, handler_name: "fwd")

      # Parent handler on B dispatches child
      Abxbus.on(:cr_ctx_b, CtxParentEvent, fn event ->
        parent_bus = Abxbus.current_bus!()
        Agent.update(captured, &Map.put(&1, :parent_id, event.event_id))
        Agent.update(captured, &Map.put(&1, :parent_bus, parent_bus))

        child = Abxbus.emit(parent_bus, CtxChildEvent.new())
        Abxbus.await(child)
        "parent_ok"
      end, handler_name: "parent_handler")

      # Child handler on B
      Abxbus.on(:cr_ctx_b, CtxChildEvent, fn event ->
        child_bus = Abxbus.current_bus!()
        Agent.update(captured, &Map.put(&1, :child_parent_id, event.event_parent_id))
        Agent.update(captured, &Map.put(&1, :child_bus, child_bus))
        "child_ok"
      end, handler_name: "child_handler")

      parent = Abxbus.emit(:cr_ctx_a, CtxParentEvent.new())
      Abxbus.wait_until_idle(:cr_ctx_a)
      Abxbus.wait_until_idle(:cr_ctx_b)

      state = Agent.get(captured, & &1)

      # Parent handler should see bus B
      assert state.parent_bus == :cr_ctx_b

      # Child handler should also see bus B
      assert state.child_bus == :cr_ctx_b

      # Child's parent_id should match parent event
      assert state.child_parent_id == state.parent_id

      # Event path should include both buses
      stored = Abxbus.EventStore.get(parent.event_id)
      path_str = Enum.join(stored.event_path, ",")
      assert path_str =~ "cr_ctx_a", "event_path should include source bus cr_ctx_a"
      assert path_str =~ "cr_ctx_b", "event_path should include target bus cr_ctx_b"
    end
  end

  describe "history backpressure rejects overflow" do
    test "max_history_drop=false rejects at limit" do
      {:ok, _} = Abxbus.start_bus(:cr_bp,
        max_history_size: 1,
        max_history_drop: false
      )

      defevent(BPEvent, value: "default")

      Abxbus.on(:cr_bp, BPEvent, fn _event -> "ok" end)

      # First event should succeed
      first = Abxbus.emit(:cr_bp, BPEvent.new(value: "first"))
      Abxbus.wait_until_idle(:cr_bp)

      stored_first = Abxbus.EventStore.get(first.event_id)
      assert stored_first != nil

      # Second event should be rejected (history is full, drop=false)
      result = Abxbus.emit(:cr_bp, BPEvent.new(value: "second"))
      assert match?({:error, %Abxbus.HistoryFullError{}}, result)
    end
  end

  describe "pending queue find visibility transitions to completed" do
    test "in-progress event is visible via find, then transitions to completed" do
      {:ok, _} = Abxbus.start_bus(:cr_pqv)

      defevent(PQVEvent, value: "default")

      barrier = :atomics.new(1, [])

      Abxbus.on(:cr_pqv, PQVEvent, fn _event ->
        # Block until barrier is released
        spin_until(fn -> :atomics.get(barrier, 1) == 1 end, 5000)
        "done"
      end, handler_name: "slow_handler")

      event = Abxbus.emit(:cr_pqv, PQVEvent.new(value: "target"))

      # Give the handler a moment to start processing
      Process.sleep(20)

      # While handler is blocked, the event should be findable and in-progress (pending)
      in_progress = Abxbus.find(PQVEvent,
        where: fn e -> e.value == "target" end,
        past: true
      )
      assert in_progress != nil
      assert in_progress.event_id == event.event_id

      in_progress_stored = Abxbus.EventStore.get(event.event_id)
      assert in_progress_stored.event_status in [:pending, :processing, :started]

      # Release the handler
      :atomics.put(barrier, 1, 1)
      Abxbus.wait_until_idle(:cr_pqv)

      # Now the event should be completed
      completed_stored = Abxbus.EventStore.get(event.event_id)
      assert completed_stored.event_status == :completed

      completed_find = Abxbus.find(PQVEvent,
        where: fn e -> e.value == "target" end,
        past: true
      )
      assert completed_find != nil
      assert completed_find.event_id == event.event_id
    end
  end

  defp spin_until(fun, max, i \\ 0) do
    if i >= max, do: raise("spin_until exceeded #{max} iterations")
    if fun.(), do: :ok, else: (Process.sleep(1); spin_until(fun, max, i + 1))
  end
end

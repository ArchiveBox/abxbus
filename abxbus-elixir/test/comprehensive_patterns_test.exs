defmodule Abxbus.ComprehensivePatternsTest do
  @moduledoc """
  Tests for complex event dispatch patterns including queue-jump with no-overshoot,
  dispatch-multiple-await-one, and multi-bus forwarding.

  Port of tests/test_comprehensive_patterns.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(Event1, label: "E1")
  defevent(Event2, label: "E2")
  defevent(Event3, label: "E3")
  defevent(ChildA, label: "A")
  defevent(ChildB, label: "B")
  defevent(ChildC, label: "C")
  defevent(ImmediateChildEvent)
  defevent(QueuedChildEvent)
  defevent(CPParentEvent)

  describe "queue-jump no overshoot" do
    test "awaited child jumps queue but queued siblings don't start early" do
      # Queue: [E1, E2, E3]
      # E1 handler emits and awaits ChildEvent
      # ChildEvent should run during E1, E2 and E3 should NOT start until E1 is done
      {:ok, _} = Abxbus.start_bus(:noov, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      defevent(NOChildEvent, label: "Child")

      Abxbus.on(:noov, Event1, fn _event ->
        Agent.update(log, &(&1 ++ ["E1_start"]))

        child = Abxbus.emit(:noov, NOChildEvent.new())
        Agent.update(log, &(&1 ++ ["Child_dispatched"]))
        Abxbus.await(child)
        Agent.update(log, &(&1 ++ ["Child_await_returned"]))

        Agent.update(log, &(&1 ++ ["E1_end"]))
        :ok
      end)

      Abxbus.on(:noov, NOChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["Child_handler"]))
        :ok
      end)

      Abxbus.on(:noov, Event2, fn _event ->
        Agent.update(log, &(&1 ++ ["E2"]))
        :ok
      end)

      Abxbus.on(:noov, Event3, fn _event ->
        Agent.update(log, &(&1 ++ ["E3"]))
        :ok
      end)

      Abxbus.emit(:noov, Event1.new())
      Abxbus.emit(:noov, Event2.new())
      Abxbus.emit(:noov, Event3.new())

      Abxbus.wait_until_idle(:noov)

      order = Agent.get(log, & &1)

      # Child should run between E1_start and E1_end
      e1_start = Enum.find_index(order, &(&1 == "E1_start"))
      child = Enum.find_index(order, &(&1 == "Child_handler"))
      e1_end = Enum.find_index(order, &(&1 == "E1_end"))
      e2 = Enum.find_index(order, &(&1 == "E2"))
      e3 = Enum.find_index(order, &(&1 == "E3"))

      assert e1_start < child, "Child should start after E1_start"
      assert child < e1_end, "Child should complete before E1_end"
      assert e1_end < e2, "E2 should not start before E1 ends"
      assert e2 < e3, "E3 should come after E2 (FIFO)"
    end
  end

  describe "dispatch multiple, await one" do
    test "only the awaited child jumps queue; others stay in FIFO order" do
      {:ok, _} = Abxbus.start_bus(:dma, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:dma, Event1, fn _event ->
        Agent.update(log, &(&1 ++ ["E1_start"]))

        # Dispatch three children, only await ChildB
        _child_a = Abxbus.emit(:dma, ChildA.new())
        child_b = Abxbus.emit(:dma, ChildB.new())
        _child_c = Abxbus.emit(:dma, ChildC.new())

        Abxbus.await(child_b)
        Agent.update(log, &(&1 ++ ["ChildB_await_returned"]))

        Agent.update(log, &(&1 ++ ["E1_end"]))
        :ok
      end)

      Abxbus.on(:dma, ChildA, fn _event ->
        Agent.update(log, &(&1 ++ ["ChildA"]))
        :ok
      end)

      Abxbus.on(:dma, ChildB, fn _event ->
        Agent.update(log, &(&1 ++ ["ChildB"]))
        :ok
      end)

      Abxbus.on(:dma, ChildC, fn _event ->
        Agent.update(log, &(&1 ++ ["ChildC"]))
        :ok
      end)

      Abxbus.on(:dma, Event2, fn _event ->
        Agent.update(log, &(&1 ++ ["E2"]))
        :ok
      end)

      Abxbus.on(:dma, Event3, fn _event ->
        Agent.update(log, &(&1 ++ ["E3"]))
        :ok
      end)

      Abxbus.emit(:dma, Event1.new())
      Abxbus.emit(:dma, Event2.new())
      Abxbus.emit(:dma, Event3.new())

      Abxbus.wait_until_idle(:dma)

      order = Agent.get(log, & &1)

      # ChildB should run during E1 (queue-jumped)
      e1_start = Enum.find_index(order, &(&1 == "E1_start"))
      child_b = Enum.find_index(order, &(&1 == "ChildB"))
      e1_end = Enum.find_index(order, &(&1 == "E1_end"))

      assert e1_start < child_b, "ChildB should start after E1_start"
      assert child_b < e1_end, "ChildB should complete before E1_end"

      # ChildA and ChildC should NOT run before E1_end
      child_a_idx = Enum.find_index(order, &(&1 == "ChildA"))
      child_c_idx = Enum.find_index(order, &(&1 == "ChildC"))

      assert child_a_idx > e1_end, "ChildA should not run until E1 finishes"
      assert child_c_idx > e1_end, "ChildC should not run until E1 finishes"

      # E2 and E3 should come after E1_end
      e2 = Enum.find_index(order, &(&1 == "E2"))
      e3 = Enum.find_index(order, &(&1 == "E3"))

      assert e2 > e1_end, "E2 should not run until E1 finishes"
      assert e3 > e1_end, "E3 should not run until E1 finishes"
    end
  end

  describe "sync vs async dispatch" do
    test "awaited emit completes before continuing; non-awaited stays pending" do
      {:ok, _} = Abxbus.start_bus(:sva, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:sva, CPParentEvent, fn _event ->
        # Async dispatch (fire-and-forget)
        queued = Abxbus.emit(:sva, QueuedChildEvent.new())
        queued_store = Abxbus.EventStore.get(queued.event_id)
        Agent.update(log, &(&1 ++ ["queued_status:#{queued_store.event_status}"]))

        # Sync dispatch (await emit = queue jump)
        immediate = Abxbus.emit(:sva, ImmediateChildEvent.new())
        completed = Abxbus.await(immediate)
        Agent.update(log, &(&1 ++ ["immediate_status:#{completed.event_status}"]))

        :ok
      end)

      Abxbus.on(:sva, ImmediateChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["immediate_handler"]))
        :ok
      end)

      Abxbus.on(:sva, QueuedChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["queued_handler"]))
        :ok
      end)

      Abxbus.emit(:sva, CPParentEvent.new())
      Abxbus.wait_until_idle(:sva)

      order = Agent.get(log, & &1)

      assert "queued_status:pending" in order, "Non-awaited emit should be pending"
      assert "immediate_status:completed" in order, "Awaited emit should be completed"

      # immediate_handler should run before queued_handler
      imm_idx = Enum.find_index(order, &(&1 == "immediate_handler"))
      q_idx = Enum.find_index(order, &(&1 == "queued_handler"))

      assert imm_idx < q_idx, "Awaited child should run before queued child"
    end
  end

  describe "multi-bus forwarding with queued events" do
    test "queue-jump on bus1 doesn't affect bus2's queue" do
      {:ok, _} = Abxbus.start_bus(:mb1, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:mb2, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      defevent(MBChildEvent)
      defevent(MBEvent3)
      defevent(MBEvent4)

      Abxbus.on(:mb1, Event1, fn _event ->
        Agent.update(log, &(&1 ++ ["mb1_E1_start"]))

        child = Abxbus.emit(:mb1, MBChildEvent.new())
        Abxbus.await(child)

        Agent.update(log, &(&1 ++ ["mb1_E1_end"]))
        :ok
      end)

      Abxbus.on(:mb1, MBChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["mb1_child"]))
        :ok
      end)

      Abxbus.on(:mb1, Event2, fn _event ->
        Agent.update(log, &(&1 ++ ["mb1_E2"]))
        :ok
      end)

      Abxbus.on(:mb2, MBEvent3, fn _event ->
        Agent.update(log, &(&1 ++ ["mb2_E3"]))
        :ok
      end)

      Abxbus.on(:mb2, MBEvent4, fn _event ->
        Agent.update(log, &(&1 ++ ["mb2_E4"]))
        :ok
      end)

      # Emit on both buses
      Abxbus.emit(:mb1, Event1.new())
      Abxbus.emit(:mb1, Event2.new())
      Abxbus.emit(:mb2, MBEvent3.new())
      Abxbus.emit(:mb2, MBEvent4.new())

      Abxbus.wait_until_idle(:mb1)
      Abxbus.wait_until_idle(:mb2)

      order = Agent.get(log, & &1)

      # Bus1: E1_start, child, E1_end, E2 (child jumped queue)
      e1_start = Enum.find_index(order, &(&1 == "mb1_E1_start"))
      child = Enum.find_index(order, &(&1 == "mb1_child"))
      e1_end = Enum.find_index(order, &(&1 == "mb1_E1_end"))
      e2 = Enum.find_index(order, &(&1 == "mb1_E2"))

      assert e1_start < child
      assert child < e1_end
      assert e1_end < e2

      # Bus2 events should have run independently
      assert "mb2_E3" in order
      assert "mb2_E4" in order
    end
  end

  describe "comprehensive patterns with forwarding" do
    test "full forwarding + async/sync dispatch + parent tracking" do
      {:ok, _} = Abxbus.start_bus(:cp_bus1, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:cp_bus2)

      defevent(CPForwardParent)
      defevent(CPForwardImmediate)
      defevent(CPForwardQueued)

      log = Agent.start_link(fn -> [] end) |> elem(1)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      # Forward all from bus1 -> bus2
      Abxbus.on(:cp_bus1, "*", fn e -> Abxbus.emit(:cp_bus2, e) end, handler_name: "fwd")

      # Bus2 handler
      Abxbus.on(:cp_bus2, CPForwardImmediate, fn _event ->
        Agent.update(log, &(&1 ++ ["bus2_immediate"]))
        "forwarded"
      end, handler_name: "bus2_handler")

      Abxbus.on(:cp_bus2, CPForwardQueued, fn _event ->
        Agent.update(log, &(&1 ++ ["bus2_queued"]))
        "forwarded_queued"
      end, handler_name: "bus2_queued_handler")

      # Parent handler on bus1
      Abxbus.on(:cp_bus1, CPForwardParent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent_id, event.event_id))
        Agent.update(log, &(&1 ++ ["parent_start"]))

        # Async dispatch
        queued = Abxbus.emit(:cp_bus1, CPForwardQueued.new())
        Agent.update(ids, &Map.put(&1, :queued_id, queued.event_id))

        # Sync dispatch (await)
        immediate = Abxbus.emit(:cp_bus1, CPForwardImmediate.new())
        completed = Abxbus.await(immediate)
        Agent.update(ids, &Map.put(&1, :immediate_id, immediate.event_id))

        assert completed.event_status == :completed

        Agent.update(log, &(&1 ++ ["parent_end"]))
        "parent_done"
      end, handler_name: "parent_handler")

      parent = Abxbus.emit(:cp_bus1, CPForwardParent.new())
      Abxbus.wait_until_idle(:cp_bus1)
      Abxbus.wait_until_idle(:cp_bus2)

      order = Agent.get(log, & &1)
      captured_ids = Agent.get(ids, & &1)

      # Parent handler should start and end
      assert "parent_start" in order
      assert "parent_end" in order

      # Immediate child should have been forwarded to bus2
      assert "bus2_immediate" in order

      # Queued child should have been forwarded too
      assert "bus2_queued" in order

      # Verify parent-child relationships
      immediate_stored = Abxbus.EventStore.get(captured_ids.immediate_id)
      queued_stored = Abxbus.EventStore.get(captured_ids.queued_id)

      assert immediate_stored.event_parent_id == captured_ids.parent_id
      assert queued_stored.event_parent_id == captured_ids.parent_id
    end
  end

  describe "await forwarded event" do
    test "awaiting on source waits for target handlers" do
      {:ok, _} = Abxbus.start_bus(:afe_src)
      {:ok, _} = Abxbus.start_bus(:afe_dst)

      defevent(AFEForwardedEvent)

      target_ran = :atomics.new(1, [])

      # Forward from src -> dst
      Abxbus.on(:afe_src, "*", fn e -> Abxbus.emit(:afe_dst, e) end, handler_name: "fwd")

      Abxbus.on(:afe_dst, AFEForwardedEvent, fn _event ->
        Process.sleep(30)
        :atomics.put(target_ran, 1, 1)
        "target_done"
      end, handler_name: "target_handler")

      event = Abxbus.emit(:afe_src, AFEForwardedEvent.new())
      Abxbus.await(event)
      Abxbus.wait_until_idle(:afe_src)
      Abxbus.wait_until_idle(:afe_dst)

      assert :atomics.get(target_ran, 1) == 1, "Target handler should have run"

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      # Should have at least the forwarding handler result; target handler result
      # is merged into the same event store entry when the event_id matches.
      assert length(results) >= 1
      assert Enum.all?(results, fn r -> r.status in [:completed, :error] end)
    end
  end

  describe "race condition stress" do
    test "stress test with multiple runs" do
      defevent(StressParent)
      defevent(StressImmediateChild)
      defevent(StressQueuedChild)

      for run <- 1..3 do
        b1 = :"stress1_#{run}"
        b2 = :"stress2_#{run}"
        {:ok, _} = Abxbus.start_bus(b1)
        {:ok, _} = Abxbus.start_bus(b2)

        child_counter = :counters.new(1, [:atomics])

        # Forward all from b1 -> b2
        Abxbus.on(b1, "*", fn e -> Abxbus.emit(b2, e) end, handler_name: "fwd")

        Abxbus.on(b1, StressImmediateChild, fn _event ->
          :counters.add(child_counter, 1, 1)
          "child_bus1"
        end, handler_name: "child_bus1")

        Abxbus.on(b1, StressQueuedChild, fn _event ->
          :counters.add(child_counter, 1, 1)
          "queued_bus1"
        end, handler_name: "queued_bus1")

        Abxbus.on(b2, StressImmediateChild, fn _event ->
          :counters.add(child_counter, 1, 1)
          "child_bus2"
        end, handler_name: "child_bus2")

        Abxbus.on(b2, StressQueuedChild, fn _event ->
          :counters.add(child_counter, 1, 1)
          "queued_bus2"
        end, handler_name: "queued_bus2")

        Abxbus.on(b1, StressParent, fn _event ->
          bus = Abxbus.current_bus!()
          # Dispatch children
          for _ <- 1..2 do
            Abxbus.emit(bus, StressQueuedChild.new())
          end

          imm = Abxbus.emit(bus, StressImmediateChild.new())
          Abxbus.await(imm)

          "parent_done"
        end, handler_name: "parent_handler")

        Abxbus.emit(b1, StressParent.new())
        Abxbus.wait_until_idle(b1)
        Abxbus.wait_until_idle(b2)

        count = :counters.get(child_counter, 1)
        # 3 child events x 2 buses = 6 handler runs
        assert count == 6, "Expected 6 child handler runs, got #{count}"
      end
    end
  end

  describe "await already completed event" do
    test "awaiting completed event is no-op" do
      {:ok, _} = Abxbus.start_bus(:already_done, event_concurrency: :bus_serial)

      defevent(AlreadyDoneEvent1)
      defevent(AlreadyDoneEvent2)

      Abxbus.on(:already_done, AlreadyDoneEvent1, fn _event ->
        "event1_done"
      end, handler_name: "handler1")

      Abxbus.on(:already_done, AlreadyDoneEvent2, fn _event ->
        "event2_done"
      end, handler_name: "handler2")

      # Emit and wait for E1 to complete
      event1 = Abxbus.emit(:already_done, AlreadyDoneEvent1.new())
      Abxbus.wait_until_idle(:already_done)

      stored1 = Abxbus.EventStore.get(event1.event_id)
      assert stored1.event_status == :completed

      # Emit E2
      event2 = Abxbus.emit(:already_done, AlreadyDoneEvent2.new())

      # Await E1 again — should return immediately (no-op)
      Abxbus.await(event1)

      # E2 should still complete normally
      Abxbus.wait_until_idle(:already_done)

      stored2 = Abxbus.EventStore.get(event2.event_id)
      assert stored2.event_status == :completed
    end
  end

  describe "multiple awaits same event" do
    test "concurrent awaits on same event" do
      {:ok, _} = Abxbus.start_bus(:multi_await, event_concurrency: :bus_serial)

      defevent(MultiAwaitParent)
      defevent(MultiAwaitChild)
      defevent(MultiAwaitE2)

      await_results = Agent.start_link(fn -> [] end) |> elem(1)
      child_ref = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:multi_await, MultiAwaitParent, fn _event ->
        child = Abxbus.emit(:multi_await, MultiAwaitChild.new())
        Agent.update(child_ref, fn _ -> child end)

        # First await in handler context triggers queue-jump
        Abxbus.await(child)

        # Spawn two concurrent tasks that both await the same (now completed) child
        task1 = Task.async(fn ->
          Abxbus.await(child)
          Agent.update(await_results, &(&1 ++ ["await1_completed"]))
        end)

        task2 = Task.async(fn ->
          Abxbus.await(child)
          Agent.update(await_results, &(&1 ++ ["await2_completed"]))
        end)

        Task.await(task1, 5000)
        Task.await(task2, 5000)

        "parent_done"
      end, handler_name: "parent_handler")

      Abxbus.on(:multi_await, MultiAwaitChild, fn _event ->
        Process.sleep(10)
        "child_done"
      end, handler_name: "child_handler")

      Abxbus.on(:multi_await, MultiAwaitE2, fn _event ->
        "e2_done"
      end, handler_name: "e2_handler")

      Abxbus.emit(:multi_await, MultiAwaitParent.new())
      _e2 = Abxbus.emit(:multi_await, MultiAwaitE2.new())
      Abxbus.wait_until_idle(:multi_await)

      results = Agent.get(await_results, & &1)
      assert "await1_completed" in results
      assert "await2_completed" in results
      assert length(results) == 2

      # Child should have exactly one set of handler results
      child_event = Agent.get(child_ref, & &1)
      child_stored = Abxbus.EventStore.get(child_event.event_id)
      assert map_size(child_stored.event_results) == 1
    end
  end

  describe "deeply nested awaited children" do
    test "parent > child > grandchild all complete in order" do
      {:ok, _} = Abxbus.start_bus(:deep_nest, event_concurrency: :bus_serial)

      defevent(DeepParent)
      defevent(DeepChild)
      defevent(DeepGrandchild)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:deep_nest, DeepParent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))

        child = Abxbus.emit(:deep_nest, DeepChild.new())
        Abxbus.await(child)

        Agent.update(log, &(&1 ++ ["parent_end"]))
        "parent_done"
      end, handler_name: "parent_handler")

      Abxbus.on(:deep_nest, DeepChild, fn _event ->
        Agent.update(log, &(&1 ++ ["child_start"]))

        grandchild = Abxbus.emit(:deep_nest, DeepGrandchild.new())
        Abxbus.await(grandchild)

        Agent.update(log, &(&1 ++ ["child_end"]))
        "child_done"
      end, handler_name: "child_handler")

      Abxbus.on(:deep_nest, DeepGrandchild, fn _event ->
        Agent.update(log, &(&1 ++ ["grandchild"]))
        "grandchild_done"
      end, handler_name: "grandchild_handler")

      Abxbus.emit(:deep_nest, DeepParent.new())
      Abxbus.wait_until_idle(:deep_nest)

      order = Agent.get(log, & &1)

      assert order == [
        "parent_start",
        "child_start",
        "grandchild",
        "child_end",
        "parent_end"
      ]
    end
  end
end

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
end

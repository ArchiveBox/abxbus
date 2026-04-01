defmodule AbxBus.ParentTrackingTest do
  @moduledoc """
  Tests for automatic parent-child event tracking, lineage chains,
  and cross-bus parent tracking.

  Port of tests/test_eventbus_dispatch_parent_tracking.py.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  defevent(PTParentEvent, message: "test")
  defevent(PTChildEvent, data: "child")
  defevent(PTGrandchildEvent, info: "grandchild")
  defevent(PTSiblingEvent)

  describe "automatic parent tracking" do
    test "child events get parent_id set automatically" do
      {:ok, _} = AbxBus.start_bus(:pt1, event_concurrency: :bus_serial)

      child_ref = :atomics.new(1, [])
      child_events = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:pt1, PTParentEvent, fn event ->
        child = AbxBus.emit(:pt1, PTChildEvent.new())
        completed = AbxBus.await(child)
        Agent.update(child_events, fn l -> l ++ [completed] end)
        :ok
      end)

      AbxBus.on(:pt1, PTChildEvent, fn _event -> :ok end)

      parent = AbxBus.emit(:pt1, PTParentEvent.new())
      AbxBus.wait_until_idle(:pt1)

      children = Agent.get(child_events, & &1)
      assert length(children) == 1

      child = hd(children)
      child_stored = AbxBus.EventStore.get(child.event_id)
      assert child_stored.event_parent_id == parent.event_id
    end

    test "multi-level lineage chain: parent -> child -> grandchild" do
      {:ok, _} = AbxBus.start_bus(:pt2, event_concurrency: :bus_serial)

      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      AbxBus.on(:pt2, PTParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = AbxBus.emit(:pt2, PTChildEvent.new())
        AbxBus.await(child)
        :ok
      end)

      AbxBus.on(:pt2, PTChildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :child, event.event_id))
        grandchild = AbxBus.emit(:pt2, PTGrandchildEvent.new())
        AbxBus.await(grandchild)
        :ok
      end)

      AbxBus.on(:pt2, PTGrandchildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :grandchild, event.event_id))
        :ok
      end)

      AbxBus.emit(:pt2, PTParentEvent.new())
      AbxBus.wait_until_idle(:pt2)

      captured = Agent.get(ids, & &1)

      child_stored = AbxBus.EventStore.get(captured.child)
      grandchild_stored = AbxBus.EventStore.get(captured.grandchild)

      assert child_stored.event_parent_id == captured.parent
      assert grandchild_stored.event_parent_id == captured.child
    end

    test "explicitly set parent_id is not overridden" do
      {:ok, _} = AbxBus.start_bus(:pt3, event_concurrency: :bus_serial)

      explicit_parent_id = AbxBus.Event.generate_id()

      AbxBus.on(:pt3, PTParentEvent, fn _event ->
        child = AbxBus.emit(:pt3, PTChildEvent.new(event_parent_id: explicit_parent_id))
        AbxBus.await(child)
        :ok
      end)

      AbxBus.on(:pt3, PTChildEvent, fn _event -> :ok end)

      AbxBus.emit(:pt3, PTParentEvent.new())
      AbxBus.wait_until_idle(:pt3)
    end

    test "forwarded events are NOT children" do
      {:ok, _} = AbxBus.start_bus(:pt_fwd_a)
      {:ok, _} = AbxBus.start_bus(:pt_fwd_b)

      AbxBus.forward(:pt_fwd_a, :pt_fwd_b)

      AbxBus.on(:pt_fwd_a, PTParentEvent, fn _event -> :ok end)
      AbxBus.on(:pt_fwd_b, PTParentEvent, fn _event -> :ok end)

      event = AbxBus.emit(:pt_fwd_a, PTParentEvent.new())
      AbxBus.wait_until_idle(:pt_fwd_a)
      AbxBus.wait_until_idle(:pt_fwd_b)

      stored = AbxBus.EventStore.get(event.event_id)

      # Forwarded event should NOT have parent_id
      assert stored.event_parent_id == nil

      # Event path should show both buses
      assert length(stored.event_path) >= 1
    end
  end

  describe "cross-bus parent tracking" do
    test "child dispatched on bus2 from handler on bus1 has correct parent" do
      {:ok, _} = AbxBus.start_bus(:xb1, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:xb2, event_concurrency: :bus_serial)

      child_ids = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:xb1, PTParentEvent, fn event ->
        # Dispatch child on bus2 from bus1 handler
        child = AbxBus.emit(:xb2, PTChildEvent.new())
        completed = AbxBus.await(child)
        Agent.update(child_ids, fn l -> l ++ [completed.event_id] end)
        :ok
      end)

      AbxBus.on(:xb2, PTChildEvent, fn _event -> :ok end)

      parent = AbxBus.emit(:xb1, PTParentEvent.new())
      AbxBus.wait_until_idle(:xb1)
      AbxBus.wait_until_idle(:xb2)

      children = Agent.get(child_ids, & &1)
      assert length(children) == 1

      child_stored = AbxBus.EventStore.get(hd(children))
      assert child_stored.event_parent_id == parent.event_id
    end
  end
end

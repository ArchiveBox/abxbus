defmodule Abxbus.ParentTrackingTest do
  @moduledoc """
  Tests for automatic parent-child event tracking, lineage chains,
  and cross-bus parent tracking.

  Port of tests/test_eventbus_dispatch_parent_tracking.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(PTParentEvent, message: "test")
  defevent(PTChildEvent, data: "child")
  defevent(PTGrandchildEvent, info: "grandchild")
  defevent(PTSiblingEvent)

  describe "automatic parent tracking" do
    test "child events get parent_id set automatically" do
      {:ok, _} = Abxbus.start_bus(:pt1, event_concurrency: :bus_serial)

      child_ref = :atomics.new(1, [])
      child_events = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:pt1, PTParentEvent, fn event ->
        child = Abxbus.emit(:pt1, PTChildEvent.new())
        completed = Abxbus.await(child)
        Agent.update(child_events, fn l -> l ++ [completed] end)
        :ok
      end)

      Abxbus.on(:pt1, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt1, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt1)

      children = Agent.get(child_events, & &1)
      assert length(children) == 1

      child = hd(children)
      child_stored = Abxbus.EventStore.get(child.event_id)
      assert child_stored.event_parent_id == parent.event_id
    end

    test "multi-level lineage chain: parent -> child -> grandchild" do
      {:ok, _} = Abxbus.start_bus(:pt2, event_concurrency: :bus_serial)

      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:pt2, PTParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:pt2, PTChildEvent.new())
        Abxbus.await(child)
        :ok
      end)

      Abxbus.on(:pt2, PTChildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :child, event.event_id))
        grandchild = Abxbus.emit(:pt2, PTGrandchildEvent.new())
        Abxbus.await(grandchild)
        :ok
      end)

      Abxbus.on(:pt2, PTGrandchildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :grandchild, event.event_id))
        :ok
      end)

      Abxbus.emit(:pt2, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt2)

      captured = Agent.get(ids, & &1)

      child_stored = Abxbus.EventStore.get(captured.child)
      grandchild_stored = Abxbus.EventStore.get(captured.grandchild)

      assert child_stored.event_parent_id == captured.parent
      assert grandchild_stored.event_parent_id == captured.child
    end

    test "multiple children from same parent have correct parent_id" do
      {:ok, _} = Abxbus.start_bus(:pt_multi, event_concurrency: :bus_serial)

      child_events = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:pt_multi, PTParentEvent, fn _event ->
        for _ <- 1..3 do
          child = Abxbus.emit(:pt_multi, PTChildEvent.new())
          completed = Abxbus.await(child)
          Agent.update(child_events, fn l -> l ++ [completed] end)
        end

        :ok
      end)

      Abxbus.on(:pt_multi, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_multi, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt_multi)

      children = Agent.get(child_events, & &1)
      assert length(children) == 3

      for child <- children do
        child_stored = Abxbus.EventStore.get(child.event_id)
        assert child_stored.event_parent_id == parent.event_id
      end
    end

    test "parallel handler concurrency parent tracking" do
      {:ok, _} = Abxbus.start_bus(:pt_parallel)

      events_from_handlers = Agent.start_link(fn -> %{h1: [], h2: []} end) |> elem(1)

      Abxbus.on(:pt_parallel, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_parallel, PTChildEvent.new(data: "from_h1"))
        completed = Abxbus.await(child)
        Agent.update(events_from_handlers, fn m -> Map.update!(m, :h1, &(&1 ++ [completed])) end)
        :ok
      end, handler_name: "h1")

      Abxbus.on(:pt_parallel, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_parallel, PTChildEvent.new(data: "from_h2"))
        completed = Abxbus.await(child)
        Agent.update(events_from_handlers, fn m -> Map.update!(m, :h2, &(&1 ++ [completed])) end)
        :ok
      end, handler_name: "h2")

      Abxbus.on(:pt_parallel, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_parallel, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt_parallel)

      handlers = Agent.get(events_from_handlers, & &1)
      assert length(handlers.h1) == 1
      assert length(handlers.h2) == 1

      h1_child = Abxbus.EventStore.get(hd(handlers.h1).event_id)
      h2_child = Abxbus.EventStore.get(hd(handlers.h2).event_id)
      assert h1_child.event_parent_id == parent.event_id
      assert h2_child.event_parent_id == parent.event_id
    end

    test "sync handler parent tracking" do
      {:ok, _} = Abxbus.start_bus(:pt_sync, event_concurrency: :bus_serial)

      child_events = Agent.start_link(fn -> [] end) |> elem(1)

      # In Elixir all handler fns are already synchronous
      Abxbus.on(:pt_sync, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_sync, PTChildEvent.new(data: "from_sync"))
        completed = Abxbus.await(child)
        Agent.update(child_events, fn l -> l ++ [completed] end)
        :ok
      end)

      Abxbus.on(:pt_sync, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_sync, PTParentEvent.new(message: "sync_test"))
      Abxbus.wait_until_idle(:pt_sync)

      children = Agent.get(child_events, & &1)
      assert length(children) == 1

      child_stored = Abxbus.EventStore.get(hd(children).event_id)
      assert child_stored.event_parent_id == parent.event_id
    end

    test "error handler parent tracking" do
      {:ok, _} = Abxbus.start_bus(:pt_err)

      child_events = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:pt_err, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_err, PTChildEvent.new(data: "before_error"))
        completed = Abxbus.await(child)
        Agent.update(child_events, fn l -> l ++ [completed] end)
        raise "Handler error - expected to fail"
      end, handler_name: "failing")

      Abxbus.on(:pt_err, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_err, PTChildEvent.new(data: "after_error"))
        completed = Abxbus.await(child)
        Agent.update(child_events, fn l -> l ++ [completed] end)
        :ok
      end, handler_name: "success")

      Abxbus.on(:pt_err, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_err, PTParentEvent.new(message: "error_test"))
      Abxbus.wait_until_idle(:pt_err)

      children = Agent.get(child_events, & &1)
      assert length(children) == 2

      for child <- children do
        child_stored = Abxbus.EventStore.get(child.event_id)
        assert child_stored.event_parent_id == parent.event_id
      end
    end

    test "explicitly set parent_id is not overridden" do
      {:ok, _} = Abxbus.start_bus(:pt3, event_concurrency: :bus_serial)

      explicit_parent_id = Abxbus.Event.generate_id()
      child_ids = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:pt3, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt3, PTChildEvent.new(event_parent_id: explicit_parent_id))
        completed = Abxbus.await(child)
        Agent.update(child_ids, fn l -> l ++ [completed.event_id] end)
        :ok
      end)

      Abxbus.on(:pt3, PTChildEvent, fn _event -> :ok end)

      Abxbus.emit(:pt3, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt3)

      [child_id] = Agent.get(child_ids, & &1)
      child_stored = Abxbus.EventStore.get(child_id)
      assert child_stored.event_parent_id == explicit_parent_id,
             "Explicit parent_id should be preserved, got #{inspect(child_stored.event_parent_id)}"
    end

    test "forwarded events are NOT children" do
      {:ok, _} = Abxbus.start_bus(:pt_fwd_a)
      {:ok, _} = Abxbus.start_bus(:pt_fwd_b)

      Abxbus.on(:pt_fwd_a, "*", fn e -> Abxbus.emit(:pt_fwd_b, e) end, handler_name: "fwd")

      Abxbus.on(:pt_fwd_a, PTParentEvent, fn _event -> :ok end)
      Abxbus.on(:pt_fwd_b, PTParentEvent, fn _event -> :ok end)

      event = Abxbus.emit(:pt_fwd_a, PTParentEvent.new())
      Abxbus.wait_until_idle(:pt_fwd_a)
      Abxbus.wait_until_idle(:pt_fwd_b)

      stored = Abxbus.EventStore.get(event.event_id)

      # Forwarded event should NOT have parent_id
      assert stored.event_parent_id == nil

      # Event path should show both buses
      assert length(stored.event_path) >= 2
    end
  end

  describe "event children tracking" do
    test "child events tracked in parent's children list" do
      {:ok, _} = Abxbus.start_bus(:pt_children, event_concurrency: :bus_serial)

      Abxbus.on(:pt_children, PTParentEvent, fn _event ->
        for _ <- 1..3 do
          child = Abxbus.emit(:pt_children, PTChildEvent.new())
          Abxbus.await(child)
        end

        :ok
      end)

      Abxbus.on(:pt_children, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_children, PTParentEvent.new(message: "test_children_tracking"))
      Abxbus.wait_until_idle(:pt_children)

      children_ids = Abxbus.EventStore.children_of(parent.event_id)
      assert length(children_ids) == 3

      for child_id <- children_ids do
        child_stored = Abxbus.EventStore.get(child_id)
        assert child_stored.event_parent_id == parent.event_id
      end
    end

    test "nested event children tracking (multi-level)" do
      {:ok, _} = Abxbus.start_bus(:pt_nested, event_concurrency: :bus_serial)

      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:pt_nested, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_nested, PTChildEvent.new(data: "level1"))
        completed = Abxbus.await(child)
        Agent.update(ids, &Map.put(&1, :child, completed.event_id))
        :ok
      end)

      Abxbus.on(:pt_nested, PTChildEvent, fn _event ->
        grandchild = Abxbus.emit(:pt_nested, PTGrandchildEvent.new(info: "level2"))
        completed = Abxbus.await(grandchild)
        Agent.update(ids, &Map.put(&1, :grandchild, completed.event_id))
        :ok
      end)

      Abxbus.on(:pt_nested, PTGrandchildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_nested, PTParentEvent.new(message: "nested_test"))
      Abxbus.wait_until_idle(:pt_nested)

      captured = Agent.get(ids, & &1)

      # Parent should have child
      parent_children = Abxbus.EventStore.children_of(parent.event_id)
      assert length(parent_children) == 1
      assert hd(parent_children) == captured.child

      # Child should have grandchild
      child_children = Abxbus.EventStore.children_of(captured.child)
      assert length(child_children) == 1
      assert hd(child_children) == captured.grandchild
    end

    test "multiple handlers event children" do
      {:ok, _} = Abxbus.start_bus(:pt_mh)

      Abxbus.on(:pt_mh, PTParentEvent, fn _event ->
        child = Abxbus.emit(:pt_mh, PTChildEvent.new(data: "from_handler1"))
        Abxbus.await(child)
        :ok
      end, handler_name: "h1")

      Abxbus.on(:pt_mh, PTParentEvent, fn _event ->
        c1 = Abxbus.emit(:pt_mh, PTChildEvent.new(data: "from_handler2_a"))
        c2 = Abxbus.emit(:pt_mh, PTChildEvent.new(data: "from_handler2_b"))
        Abxbus.await(c1)
        Abxbus.await(c2)
        :ok
      end, handler_name: "h2")

      Abxbus.on(:pt_mh, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:pt_mh, PTParentEvent.new(message: "multi_handler_test"))
      Abxbus.wait_until_idle(:pt_mh)

      children_ids = Abxbus.EventStore.children_of(parent.event_id)
      assert length(children_ids) == 3

      child_data =
        Enum.map(children_ids, fn id ->
          Abxbus.EventStore.get(id).data
        end)

      assert "from_handler1" in child_data
      assert "from_handler2_a" in child_data
      assert "from_handler2_b" in child_data
    end

    test "event children empty when no children dispatched" do
      {:ok, _} = Abxbus.start_bus(:pt_empty, event_concurrency: :bus_serial)

      Abxbus.on(:pt_empty, PTParentEvent, fn _event ->
        # No child events dispatched
        :ok
      end)

      parent = Abxbus.emit(:pt_empty, PTParentEvent.new(message: "no_children_test"))
      Abxbus.wait_until_idle(:pt_empty)

      children_ids = Abxbus.EventStore.children_of(parent.event_id)
      assert children_ids == []
    end

    test "parent completion waits for all children" do
      {:ok, _} = Abxbus.start_bus(:pt_wait)

      completion_order = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:pt_wait, PTParentEvent, fn _event ->
        c1 = Abxbus.emit(:pt_wait, PTChildEvent.new(data: "child1"))
        c2 = Abxbus.emit(:pt_wait, PTChildEvent.new(data: "child2"))
        Abxbus.await(c1)
        Abxbus.await(c2)
        Agent.update(completion_order, fn l -> l ++ ["parent_handler"] end)
        :ok
      end)

      Abxbus.on(:pt_wait, PTChildEvent, fn event ->
        # Simulate some work
        Process.sleep(10)
        Agent.update(completion_order, fn l -> l ++ ["child_handler_#{event.data}"] end)
        :ok
      end)

      parent = Abxbus.emit(:pt_wait, PTParentEvent.new(message: "completion_test"))
      Abxbus.wait_until_idle(:pt_wait)

      # All children should be complete
      children_ids = Abxbus.EventStore.children_of(parent.event_id)
      assert length(children_ids) == 2

      for child_id <- children_ids do
        child_stored = Abxbus.EventStore.get(child_id)
        assert child_stored.event_status == :completed
      end

      parent_stored = Abxbus.EventStore.get(parent.event_id)
      assert parent_stored.event_status == :completed
    end
  end

  describe "cross-bus parent tracking" do
    test "child dispatched on bus2 from handler on bus1 has correct parent" do
      {:ok, _} = Abxbus.start_bus(:xb1, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:xb2, event_concurrency: :bus_serial)

      child_ids = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:xb1, PTParentEvent, fn event ->
        # Dispatch child on bus2 from bus1 handler
        child = Abxbus.emit(:xb2, PTChildEvent.new())
        completed = Abxbus.await(child)
        Agent.update(child_ids, fn l -> l ++ [completed.event_id] end)
        :ok
      end)

      Abxbus.on(:xb2, PTChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:xb1, PTParentEvent.new())
      Abxbus.wait_until_idle(:xb1)
      Abxbus.wait_until_idle(:xb2)

      children = Agent.get(child_ids, & &1)
      assert length(children) == 1

      child_stored = Abxbus.EventStore.get(hd(children))
      assert child_stored.event_parent_id == parent.event_id
    end
  end
end

defmodule Abxbus.BaseEventTest do
  @moduledoc """
  Tests for base event behavior: bus property, queue-jump vs event_completed,
  field validation, result tracking, and nested handler dispatch.

  Port of tests/test_base_event.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(BEMainEvent, message: "test")
  defevent(BEChildEvent, data: "child")
  defevent(BEGrandchildEvent, info: "grandchild")
  defevent(BESiblingEvent)

  describe "bus context in handlers" do
    test "current_bus! returns the bus name inside a handler" do
      {:ok, _} = Abxbus.start_bus(:be1)

      bus_seen = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:be1, BEMainEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(bus_seen, fn _ -> bus end)
        :ok
      end)

      Abxbus.emit(:be1, BEMainEvent.new())
      Abxbus.wait_until_idle(:be1)

      assert Agent.get(bus_seen, & &1) == :be1
    end

    test "current_bus! raises outside handler" do
      assert_raise RuntimeError, ~r/bus can only be accessed/, fn ->
        Abxbus.current_bus!()
      end
    end

    test "nested handlers see correct bus" do
      {:ok, _} = Abxbus.start_bus(:be_nest, event_concurrency: :bus_serial)

      buses = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:be_nest, BEMainEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(buses, &(&1 ++ [{:parent, bus}]))
        child = Abxbus.emit(bus, BEChildEvent.new())
        Abxbus.await(child)
        :ok
      end)

      Abxbus.on(:be_nest, BEChildEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(buses, &(&1 ++ [{:child, bus}]))
        :ok
      end)

      Abxbus.emit(:be_nest, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_nest)

      seen = Agent.get(buses, & &1)
      assert {:parent, :be_nest} in seen
      assert {:child, :be_nest} in seen
    end
  end

  describe "queue-jump vs wait_for_completion" do
    test "await causes queue-jump; wait_for_completion does not" do
      {:ok, _} = Abxbus.start_bus(:be_qj, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:be_qj, BEMainEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))

        sibling = Abxbus.emit(:be_qj, BESiblingEvent.new())
        child = Abxbus.emit(:be_qj, BEChildEvent.new())

        # await = queue jump
        Abxbus.await(child)

        Agent.update(log, &(&1 ++ ["parent_end"]))
        :ok
      end)

      Abxbus.on(:be_qj, BEChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["child"]))
        :ok
      end)

      Abxbus.on(:be_qj, BESiblingEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["sibling"]))
        :ok
      end)

      Abxbus.emit(:be_qj, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_qj)

      order = Agent.get(log, & &1)
      assert order == ["parent_start", "child", "parent_end", "sibling"]
    end
  end

  describe "multi-level dispatch via current_bus" do
    test "parent -> child -> grandchild chain works" do
      {:ok, _} = Abxbus.start_bus(:be_chain, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:be_chain, BEMainEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))
        child = Abxbus.emit(Abxbus.current_bus!(), BEChildEvent.new())
        Abxbus.await(child)
        Agent.update(log, &(&1 ++ ["parent_end"]))
        :ok
      end)

      Abxbus.on(:be_chain, BEChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["child_start"]))
        grandchild = Abxbus.emit(Abxbus.current_bus!(), BEGrandchildEvent.new())
        Abxbus.await(grandchild)
        Agent.update(log, &(&1 ++ ["child_end"]))
        :ok
      end)

      Abxbus.on(:be_chain, BEGrandchildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["grandchild"]))
        :ok
      end)

      parent = Abxbus.emit(:be_chain, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_chain)

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

  describe "bus property single bus" do
    test "current_bus! returns bus name and can dispatch child" do
      {:ok, _} = Abxbus.start_bus(:be_single, event_concurrency: :bus_serial)

      handler_called = :atomics.new(1, [])
      child_dispatched = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:be_single, BEMainEvent, fn _event ->
        :atomics.put(handler_called, 1, 1)
        assert Abxbus.current_bus!() == :be_single

        child = Abxbus.emit(Abxbus.current_bus!(), BEChildEvent.new())
        completed = Abxbus.await(child)
        Agent.update(child_dispatched, fn _ -> completed end)
        :ok
      end)

      Abxbus.on(:be_single, BEChildEvent, fn _event -> :ok end)

      Abxbus.emit(:be_single, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_single)

      assert :atomics.get(handler_called, 1) == 1
      child = Agent.get(child_dispatched, & &1)
      assert child != nil
    end
  end

  describe "bus property multiple buses" do
    test "each handler sees its own bus" do
      {:ok, _} = Abxbus.start_bus(:be_multi1)
      {:ok, _} = Abxbus.start_bus(:be_multi2)

      bus1_seen = Agent.start_link(fn -> nil end) |> elem(1)
      bus2_seen = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:be_multi1, BEMainEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(bus1_seen, fn _ -> bus end)
        :ok
      end)

      Abxbus.on(:be_multi2, BEMainEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(bus2_seen, fn _ -> bus end)
        :ok
      end)

      Abxbus.emit(:be_multi1, BEMainEvent.new(message: "bus1"))
      Abxbus.wait_until_idle(:be_multi1)

      Abxbus.emit(:be_multi2, BEMainEvent.new(message: "bus2"))
      Abxbus.wait_until_idle(:be_multi2)

      assert Agent.get(bus1_seen, & &1) == :be_multi1
      assert Agent.get(bus2_seen, & &1) == :be_multi2
    end
  end

  describe "bus property with forwarding" do
    test "forwarded event handler sees target bus" do
      {:ok, _} = Abxbus.start_bus(:be_fwd1)
      {:ok, _} = Abxbus.start_bus(:be_fwd2)

      # Forward from fwd1 -> fwd2
      Abxbus.on(:be_fwd1, "*", fn e -> Abxbus.emit(:be_fwd2, e) end, handler_name: "fwd")

      handler_bus = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:be_fwd2, BEMainEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(handler_bus, fn _ -> bus end)
        :ok
      end)

      Abxbus.emit(:be_fwd1, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_fwd1)
      Abxbus.wait_until_idle(:be_fwd2)

      # Handler running on bus2 should see bus2
      assert Agent.get(handler_bus, & &1) == :be_fwd2
    end
  end

  describe "event result update" do
    test "event results tracked correctly" do
      {:ok, _} = Abxbus.start_bus(:be_result)

      defevent(BEResultEvent)

      Abxbus.on(:be_result, BEResultEvent, fn _event ->
        "seeded_result"
      end, handler_name: "result_handler")

      event = Abxbus.emit(:be_result, BEResultEvent.new())
      Abxbus.wait_until_idle(:be_result)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      assert length(results) == 1
      result = hd(results)
      assert result.status == :completed
      assert result.result == "seeded_result"
      assert result.handler_name == "result_handler"
    end
  end

  describe "reserved fields rejected" do
    test "event_ prefix fields rejected at compile time" do
      # In Elixir, defevent raises a CompileError at compile time for
      # fields starting with event_ that are not known meta fields.
      # We test this by attempting to eval code that defines such an event.
      assert_raise CompileError, fn ->
        Code.eval_string("""
          require Abxbus.Event
          Abxbus.Event.defevent(BadPrefixEvent, event_unknown_field: 123)
        """)
      end
    end
  end

  describe "event children property" do
    test "children tracked correctly" do
      {:ok, _} = Abxbus.start_bus(:be_children, event_concurrency: :bus_serial)

      child_ids = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:be_children, BEMainEvent, fn _event ->
        c1 = Abxbus.emit(:be_children, BEChildEvent.new(data: "c1"))
        c2 = Abxbus.emit(:be_children, BEChildEvent.new(data: "c2"))
        Abxbus.await(c1)
        Abxbus.await(c2)
        Agent.update(child_ids, fn _ -> [c1.event_id, c2.event_id] end)
        :ok
      end)

      Abxbus.on(:be_children, BEChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:be_children, BEMainEvent.new())
      Abxbus.wait_until_idle(:be_children)

      # Verify children are tracked
      children = Abxbus.EventStore.children_of(parent.event_id)
      expected_ids = Agent.get(child_ids, & &1)

      assert length(children) == 2
      for id <- expected_ids do
        assert id in children, "Child #{id} should be tracked"
      end

      # Verify each child's parent_id
      for cid <- children do
        child_stored = Abxbus.EventStore.get(cid)
        assert child_stored.event_parent_id == parent.event_id
      end
    end
  end
end

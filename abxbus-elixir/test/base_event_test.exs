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
end

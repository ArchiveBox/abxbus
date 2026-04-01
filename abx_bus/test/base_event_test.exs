defmodule AbxBus.BaseEventTest do
  @moduledoc """
  Tests for base event behavior: bus property, queue-jump vs event_completed,
  field validation, result tracking, and nested handler dispatch.

  Port of tests/test_base_event.py.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  defevent(BEMainEvent, message: "test")
  defevent(BEChildEvent, data: "child")
  defevent(BEGrandchildEvent, info: "grandchild")
  defevent(BESiblingEvent)

  describe "bus context in handlers" do
    test "current_bus! returns the bus name inside a handler" do
      {:ok, _} = AbxBus.start_bus(:be1)

      bus_seen = Agent.start_link(fn -> nil end) |> elem(1)

      AbxBus.on(:be1, BEMainEvent, fn _event ->
        bus = AbxBus.current_bus!()
        Agent.update(bus_seen, fn _ -> bus end)
        :ok
      end)

      AbxBus.emit(:be1, BEMainEvent.new())
      AbxBus.wait_until_idle(:be1)

      assert Agent.get(bus_seen, & &1) == :be1
    end

    test "current_bus! raises outside handler" do
      assert_raise RuntimeError, ~r/bus can only be accessed/, fn ->
        AbxBus.current_bus!()
      end
    end

    test "nested handlers see correct bus" do
      {:ok, _} = AbxBus.start_bus(:be_nest, event_concurrency: :bus_serial)

      buses = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:be_nest, BEMainEvent, fn _event ->
        Agent.update(buses, &(&1 ++ [{:parent, AbxBus.current_bus!()}]))
        child = AbxBus.emit(AbxBus.current_bus!(), BEChildEvent.new())
        AbxBus.await(child)
        :ok
      end)

      AbxBus.on(:be_nest, BEChildEvent, fn _event ->
        Agent.update(buses, &(&1 ++ [{:child, AbxBus.current_bus!()}]))
        :ok
      end)

      AbxBus.emit(:be_nest, BEMainEvent.new())
      AbxBus.wait_until_idle(:be_nest)

      seen = Agent.get(buses, & &1)
      assert {:parent, :be_nest} in seen
      assert {:child, :be_nest} in seen
    end
  end

  describe "queue-jump vs wait_for_completion" do
    test "await causes queue-jump; wait_for_completion does not" do
      {:ok, _} = AbxBus.start_bus(:be_qj, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:be_qj, BEMainEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))

        sibling = AbxBus.emit(:be_qj, BESiblingEvent.new())
        child = AbxBus.emit(:be_qj, BEChildEvent.new())

        # await = queue jump
        AbxBus.await(child)

        Agent.update(log, &(&1 ++ ["parent_end"]))
        :ok
      end)

      AbxBus.on(:be_qj, BEChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["child"]))
        :ok
      end)

      AbxBus.on(:be_qj, BESiblingEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["sibling"]))
        :ok
      end)

      AbxBus.emit(:be_qj, BEMainEvent.new())
      AbxBus.wait_until_idle(:be_qj)

      order = Agent.get(log, & &1)
      assert order == ["parent_start", "child", "parent_end", "sibling"]
    end
  end

  describe "multi-level dispatch via current_bus" do
    test "parent -> child -> grandchild chain works" do
      {:ok, _} = AbxBus.start_bus(:be_chain, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:be_chain, BEMainEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))
        child = AbxBus.emit(AbxBus.current_bus!(), BEChildEvent.new())
        AbxBus.await(child)
        Agent.update(log, &(&1 ++ ["parent_end"]))
        :ok
      end)

      AbxBus.on(:be_chain, BEChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["child_start"]))
        grandchild = AbxBus.emit(AbxBus.current_bus!(), BEGrandchildEvent.new())
        AbxBus.await(grandchild)
        Agent.update(log, &(&1 ++ ["child_end"]))
        :ok
      end)

      AbxBus.on(:be_chain, BEGrandchildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["grandchild"]))
        :ok
      end)

      parent = AbxBus.emit(:be_chain, BEMainEvent.new())
      AbxBus.wait_until_idle(:be_chain)

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

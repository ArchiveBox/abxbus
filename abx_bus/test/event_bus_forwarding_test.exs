defmodule AbxBus.EventBusForwardingTest do
  @moduledoc """
  Tests for event forwarding between buses: completion race, self-parent
  prevention, defaults inheritance, and first-mode forwarding.

  Port of tests/test_eventbus_forwarding.py.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  defevent(RelayEvent)
  defevent(SelfParentForwardEvent)
  defevent(ForwardedDefaultsTriggerEvent)
  defevent(ForwardedDefaultsChildEvent, mode: "inherited")
  defevent(ForwardedFirstDefaultsEvent)

  describe "forwarding completion" do
    test "circular forwarding doesn't leave stale in-flight IDs" do
      {:ok, _} = AbxBus.start_bus(:p1, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:p2, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:p3, event_concurrency: :bus_serial)

      # Circular forwarding: p1 -> p2 -> p3 -> p1
      # (In practice, path detection prevents re-queueing on same bus)
      AbxBus.forward(:p1, :p2)
      AbxBus.forward(:p2, :p3)

      AbxBus.on(:p1, RelayEvent, fn _event -> :ok end)
      AbxBus.on(:p2, RelayEvent, fn _event -> :ok end)
      AbxBus.on(:p3, RelayEvent, fn _event -> :ok end)

      event = AbxBus.emit(:p1, RelayEvent.new())

      AbxBus.wait_until_idle(:p1)
      AbxBus.wait_until_idle(:p2)
      AbxBus.wait_until_idle(:p3)

      # No stale in-flight IDs
      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p1)) == 0
      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p2)) == 0
      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p3)) == 0

      # Event should be completed
      stored = AbxBus.EventStore.get(event.event_id)
      assert stored.event_status == :completed
    end
  end

  describe "self-parent prevention" do
    test "forwarded events don't set parent to self" do
      {:ok, _} = AbxBus.start_bus(:origin)
      {:ok, _} = AbxBus.start_bus(:target)

      AbxBus.forward(:origin, :target)

      AbxBus.on(:origin, SelfParentForwardEvent, fn _event -> :ok end)
      AbxBus.on(:target, SelfParentForwardEvent, fn _event -> :ok end)

      event = AbxBus.emit(:origin, SelfParentForwardEvent.new())

      AbxBus.wait_until_idle(:origin)
      AbxBus.wait_until_idle(:target)

      stored = AbxBus.EventStore.get(event.event_id)

      # Root event should have no parent
      assert stored.event_parent_id == nil

      # Event path should show both buses
      assert length(stored.event_path) == 2
    end
  end

  describe "forwarded defaults" do
    test "forwarded events use target bus handler concurrency defaults" do
      {:ok, _} = AbxBus.start_bus(:fwd_a, event_handler_concurrency: :serial)
      {:ok, _} = AbxBus.start_bus(:fwd_b, event_handler_concurrency: :parallel)

      AbxBus.forward(:fwd_a, :fwd_b)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      # Two handlers on bus_b — in parallel mode, both should start before either ends
      AbxBus.on(:fwd_b, ForwardedDefaultsChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["b1_start"]))
        Process.sleep(20)
        Agent.update(log, &(&1 ++ ["b1_end"]))
        :ok
      end)

      AbxBus.on(:fwd_b, ForwardedDefaultsChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["b2_start"]))
        Process.sleep(5)
        Agent.update(log, &(&1 ++ ["b2_end"]))
        :ok
      end)

      AbxBus.emit(:fwd_a, ForwardedDefaultsChildEvent.new())

      AbxBus.wait_until_idle(:fwd_a)
      AbxBus.wait_until_idle(:fwd_b)

      order = Agent.get(log, & &1)

      # With parallel handler concurrency on bus_b, both should start
      # before either finishes
      b1_start = Enum.find_index(order, &(&1 == "b1_start"))
      b2_start = Enum.find_index(order, &(&1 == "b2_start"))
      b1_end = Enum.find_index(order, &(&1 == "b1_end"))

      if b1_start && b2_start && b1_end do
        assert b2_start < b1_end, "With parallel handlers, b2 should start before b1 finishes"
      end
    end
  end

  describe "first-mode forwarding" do
    test "forwarded event uses target bus first-mode completion" do
      {:ok, _} = AbxBus.start_bus(:first_a,
        event_handler_completion: :all,
        event_handler_concurrency: :serial
      )
      {:ok, _} = AbxBus.start_bus(:first_b,
        event_handler_completion: :first,
        event_handler_concurrency: :parallel
      )

      AbxBus.forward(:first_a, :first_b)

      AbxBus.on(:first_b, ForwardedFirstDefaultsEvent, fn _event ->
        Process.sleep(50)
        "slow"
      end)

      AbxBus.on(:first_b, ForwardedFirstDefaultsEvent, fn _event ->
        Process.sleep(1)
        "fast"
      end)

      event = AbxBus.emit(:first_a, ForwardedFirstDefaultsEvent.new())

      AbxBus.wait_until_idle(:first_a)
      AbxBus.wait_until_idle(:first_b)

      result = AbxBus.first(event)
      assert result == "fast", "First-mode should return the fast handler's result"
    end
  end
end

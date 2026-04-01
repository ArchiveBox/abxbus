defmodule AbxBus.EventBusForwardingTest do
  @moduledoc """
  Tests for event forwarding between buses via wildcard handlers.
  Forwarding is done with `bus.on("*", fn e -> AbxBus.emit(other, e) end)`.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  defevent(RelayEvent)
  defevent(SelfParentForwardEvent)
  defevent(ForwardedDefaultsChildEvent, mode: "inherited")
  defevent(ForwardedFirstDefaultsEvent)

  describe "forwarding completion" do
    test "linear forwarding chain completes without stale in-flight IDs" do
      {:ok, _} = AbxBus.start_bus(:p1, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:p2, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:p3, event_concurrency: :bus_serial)

      # p1 -> p2 -> p3 via wildcard handlers
      AbxBus.on(:p1, "*", fn e -> AbxBus.emit(:p2, e) end, handler_name: "fwd_p1_p2")
      AbxBus.on(:p2, "*", fn e -> AbxBus.emit(:p3, e) end, handler_name: "fwd_p2_p3")

      AbxBus.on(:p1, RelayEvent, fn _event -> :ok end)
      AbxBus.on(:p2, RelayEvent, fn _event -> :ok end)
      AbxBus.on(:p3, RelayEvent, fn _event -> :ok end)

      event = AbxBus.emit(:p1, RelayEvent.new())

      AbxBus.wait_until_idle(:p1)
      AbxBus.wait_until_idle(:p2)
      AbxBus.wait_until_idle(:p3)

      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p1)) == 0
      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p2)) == 0
      assert MapSet.size(AbxBus.BusServer.in_flight_event_ids(:p3)) == 0

      stored = AbxBus.EventStore.get(event.event_id)
      assert stored.event_status == :completed
    end
  end

  describe "self-parent prevention" do
    test "forwarded events don't set parent to self" do
      {:ok, _} = AbxBus.start_bus(:origin)
      {:ok, _} = AbxBus.start_bus(:target)

      AbxBus.on(:origin, "*", fn e -> AbxBus.emit(:target, e) end, handler_name: "fwd")

      AbxBus.on(:origin, SelfParentForwardEvent, fn _event -> :ok end)
      AbxBus.on(:target, SelfParentForwardEvent, fn _event -> :ok end)

      event = AbxBus.emit(:origin, SelfParentForwardEvent.new())

      AbxBus.wait_until_idle(:origin)
      AbxBus.wait_until_idle(:target)

      stored = AbxBus.EventStore.get(event.event_id)
      assert stored.event_parent_id == nil
      assert length(stored.event_path) == 2
    end
  end

  describe "forwarded defaults" do
    test "forwarded events use target bus handler concurrency defaults" do
      {:ok, _} = AbxBus.start_bus(:fwd_a, event_handler_concurrency: :serial)
      {:ok, _} = AbxBus.start_bus(:fwd_b, event_handler_concurrency: :parallel)

      AbxBus.on(:fwd_a, "*", fn e -> AbxBus.emit(:fwd_b, e) end, handler_name: "fwd")

      log = Agent.start_link(fn -> [] end) |> elem(1)

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

      b1_start = Enum.find_index(order, &(&1 == "b1_start"))
      b2_start = Enum.find_index(order, &(&1 == "b2_start"))
      b1_end = Enum.find_index(order, &(&1 == "b1_end"))

      assert b1_start != nil
      assert b2_start != nil
      assert b1_end != nil
      assert b2_start < b1_end, "With parallel handlers, b2 should start before b1 finishes"
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

      AbxBus.on(:first_a, "*", fn e -> AbxBus.emit(:first_b, e) end, handler_name: "fwd")

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

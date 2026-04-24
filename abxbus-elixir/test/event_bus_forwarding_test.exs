defmodule Abxbus.EventBusForwardingTest do
  @moduledoc """
  Tests for event forwarding between buses via wildcard handlers.
  Forwarding is done with `bus.on("*", fn e -> Abxbus.emit(other, e) end)`.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(RelayEvent)
  defevent(SelfParentForwardEvent)
  defevent(ForwardedDefaultsChildEvent, mode: "inherited")
  defevent(ForwardedFirstDefaultsEvent)

  describe "forwarding completion" do
    test "linear forwarding chain completes without stale in-flight IDs" do
      {:ok, _} = Abxbus.start_bus(:p1, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:p2, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:p3, event_concurrency: :bus_serial)

      # p1 -> p2 -> p3 via wildcard handlers
      Abxbus.on(:p1, "*", fn e -> Abxbus.emit(:p2, e) end, handler_name: "fwd_p1_p2")
      Abxbus.on(:p2, "*", fn e -> Abxbus.emit(:p3, e) end, handler_name: "fwd_p2_p3")

      Abxbus.on(:p1, RelayEvent, fn _event -> :ok end)
      Abxbus.on(:p2, RelayEvent, fn _event -> :ok end)
      Abxbus.on(:p3, RelayEvent, fn _event -> :ok end)

      event = Abxbus.emit(:p1, RelayEvent.new())

      Abxbus.wait_until_idle(:p1)
      Abxbus.wait_until_idle(:p2)
      Abxbus.wait_until_idle(:p3)

      assert MapSet.size(Abxbus.BusServer.in_flight_event_ids(:p1)) == 0
      assert MapSet.size(Abxbus.BusServer.in_flight_event_ids(:p2)) == 0
      assert MapSet.size(Abxbus.BusServer.in_flight_event_ids(:p3)) == 0

      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_status == :completed
    end
  end

  describe "self-parent prevention" do
    test "forwarded events don't set parent to self" do
      {:ok, _} = Abxbus.start_bus(:origin)
      {:ok, _} = Abxbus.start_bus(:target)

      Abxbus.on(:origin, "*", fn e -> Abxbus.emit(:target, e) end, handler_name: "fwd")

      Abxbus.on(:origin, SelfParentForwardEvent, fn _event -> :ok end)
      Abxbus.on(:target, SelfParentForwardEvent, fn _event -> :ok end)

      event = Abxbus.emit(:origin, SelfParentForwardEvent.new())

      Abxbus.wait_until_idle(:origin)
      Abxbus.wait_until_idle(:target)

      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_parent_id == nil
      assert length(stored.event_path) == 2
    end
  end

  describe "forwarded defaults" do
    test "forwarded events use target bus handler concurrency defaults" do
      {:ok, _} = Abxbus.start_bus(:fwd_a, event_handler_concurrency: :serial)
      {:ok, _} = Abxbus.start_bus(:fwd_b, event_handler_concurrency: :parallel)

      Abxbus.on(:fwd_a, "*", fn e -> Abxbus.emit(:fwd_b, e) end, handler_name: "fwd")

      # Use a barrier: both handlers signal they've started, then wait for release.
      # If parallel, both will be blocked at the barrier simultaneously.
      both_started = :counters.new(1, [:atomics])
      log = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:fwd_b, ForwardedDefaultsChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["b1_start"]))
        :counters.add(both_started, 1, 1)
        # Spin until both handlers have started (proves parallelism)
        spin_until(fn -> :counters.get(both_started, 1) >= 2 end, 500)
        Agent.update(log, &(&1 ++ ["b1_end"]))
        :ok
      end)

      Abxbus.on(:fwd_b, ForwardedDefaultsChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["b2_start"]))
        :counters.add(both_started, 1, 1)
        spin_until(fn -> :counters.get(both_started, 1) >= 2 end, 500)
        Agent.update(log, &(&1 ++ ["b2_end"]))
        :ok
      end)

      Abxbus.emit(:fwd_a, ForwardedDefaultsChildEvent.new())

      Abxbus.wait_until_idle(:fwd_a)
      Abxbus.wait_until_idle(:fwd_b)

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
      {:ok, _} = Abxbus.start_bus(:first_a,
        event_handler_completion: :all,
        event_handler_concurrency: :serial
      )
      {:ok, _} = Abxbus.start_bus(:first_b,
        event_handler_completion: :first,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:first_a, "*", fn e -> Abxbus.emit(:first_b, e) end, handler_name: "fwd")

      Abxbus.on(:first_b, ForwardedFirstDefaultsEvent, fn _event ->
        Process.sleep(50)
        "slow"
      end)

      Abxbus.on(:first_b, ForwardedFirstDefaultsEvent, fn _event ->
        Process.sleep(1)
        "fast"
      end)

      event = Abxbus.emit(:first_a, ForwardedFirstDefaultsEvent.new())

      Abxbus.wait_until_idle(:first_a)
      Abxbus.wait_until_idle(:first_b)

      result = Abxbus.first(event)
      assert result == "fast", "First-mode should return the fast handler's result"
    end
  end

  defp spin_until(fun, max_iters, iter \\ 0) do
    if iter >= max_iters, do: raise("spin_until exceeded #{max_iters} iterations")
    if fun.(), do: :ok, else: (Process.sleep(1); spin_until(fun, max_iters, iter + 1))
  end
end

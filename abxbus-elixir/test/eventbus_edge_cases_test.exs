defmodule Abxbus.EventbusEdgeCasesTest do
  @moduledoc """
  Tests for edge-case behaviour: event_reset for cross-bus dispatch,
  wait_until_idle on an already-idle bus, stop with and without clear.

  Port of tests/test_eventbus_edge_cases.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(ECResetEvent, label: "reset")
  defevent(ECIdleEvent)
  defevent(ECStopEvent)
  defevent(ECStop2Event)

  describe "event_reset" do
    test "event_reset creates fresh pending event for cross-bus dispatch" do
      {:ok, _} = Abxbus.start_bus(:ec_reset_a)
      {:ok, _} = Abxbus.start_bus(:ec_reset_b)

      called = Agent.start_link(fn -> %{a: false, b: false} end) |> elem(1)

      Abxbus.on(:ec_reset_a, ECResetEvent, fn _event ->
        Agent.update(called, &Map.put(&1, :a, true))
        "handled_a"
      end, handler_name: "handler_a")

      Abxbus.on(:ec_reset_b, ECResetEvent, fn _event ->
        Agent.update(called, &Map.put(&1, :b, true))
        "handled_b"
      end, handler_name: "handler_b")

      # Emit on bus A, wait for completion
      event_a = Abxbus.emit(:ec_reset_a, ECResetEvent.new())
      Abxbus.await(event_a)

      # Reset the completed event — should produce a new pending event with a new ID
      reset_event = Abxbus.event_reset(event_a)

      assert reset_event.event_id != event_a.event_id
      assert reset_event.event_status == :pending
      assert reset_event.event_results == %{}

      # Emit the reset event on bus B
      event_b = Abxbus.emit(:ec_reset_b, reset_event)
      Abxbus.await(event_b)

      state = Agent.get(called, & &1)
      assert state.a == true, "Handler on bus A should have been called"
      assert state.b == true, "Handler on bus B should have been called"
    end
  end

  describe "wait_until_idle" do
    test "wait_until_idle returns when bus is already idle" do
      {:ok, _} = Abxbus.start_bus(:ec_idle)

      # Should return immediately without blocking
      assert Abxbus.wait_until_idle(:ec_idle) == :ok
    end
  end

  describe "stop" do
    test "stop with clear removes all state" do
      {:ok, _} = Abxbus.start_bus(:ec_stop)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(:ec_stop, ECStopEvent, fn _event ->
        :counters.add(counter, 1, 1)
        :ok
      end, handler_name: "stop_handler")

      event = Abxbus.emit(:ec_stop, ECStopEvent.new())
      Abxbus.await(event)

      assert :counters.get(counter, 1) == 1

      # Stop with clear: true — removes all state
      Abxbus.stop(:ec_stop, clear: true)

      # After stop with clear, emitting should return an error
      result = Abxbus.emit(:ec_stop, ECStopEvent.new())
      assert match?({:error, :stopped}, result)
    end

    test "stop without clear just prevents new events" do
      {:ok, _} = Abxbus.start_bus(:ec_stop2)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(:ec_stop2, ECStop2Event, fn _event ->
        :counters.add(counter, 1, 1)
        :ok
      end, handler_name: "stop2_handler")

      event = Abxbus.emit(:ec_stop2, ECStop2Event.new())
      Abxbus.await(event)

      assert :counters.get(counter, 1) == 1

      # Stop without clear
      Abxbus.stop(:ec_stop2)

      # Emit should return {:error, :stopped}
      result = Abxbus.emit(:ec_stop2, ECStop2Event.new())
      assert match?({:error, :stopped}, result)
    end
  end
end

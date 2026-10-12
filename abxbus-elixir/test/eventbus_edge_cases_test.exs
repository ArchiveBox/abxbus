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

  describe "event_reset (additional)" do
    test "event_reset creates fresh event with new id" do
      {:ok, _} = Abxbus.start_bus(:ec_reset_fresh_a)
      {:ok, _} = Abxbus.start_bus(:ec_reset_fresh_b)

      Abxbus.on(:ec_reset_fresh_a, ECResetEvent, fn _event -> "handled_a" end,
        handler_name: "reset_fresh_handler_a")

      handled_b = :counters.new(1, [:atomics])
      Abxbus.on(:ec_reset_fresh_b, ECResetEvent, fn _event ->
        :counters.add(handled_b, 1, 1)
        "handled_b"
      end, handler_name: "reset_fresh_handler_b")

      # Emit on bus_a and wait for completion
      event_a = Abxbus.emit(:ec_reset_fresh_a, ECResetEvent.new())
      completed = Abxbus.await(event_a)

      assert completed.event_status == :completed

      # Reset the completed event
      reset = Abxbus.event_reset(completed)

      # Verify reset event has new ID, :pending status, empty results
      assert reset.event_id != completed.event_id
      assert reset.event_status == :pending
      assert reset.event_results == %{}

      # Can emit the reset event on bus_b
      event_b = Abxbus.emit(:ec_reset_fresh_b, reset)
      Abxbus.await(event_b)

      assert :counters.get(handled_b, 1) == 1

      Abxbus.stop(:ec_reset_fresh_a, clear: true)
      Abxbus.stop(:ec_reset_fresh_b, clear: true)
    end
  end

  describe "wait_until_idle timeout" do
    test "wait_until_idle timeout returns without hanging" do
      {:ok, _} = Abxbus.start_bus(:ec_idle_timeout)

      Abxbus.on(:ec_idle_timeout, ECIdleEvent, fn _event ->
        Process.sleep(500)
        :ok
      end, handler_name: "slow_idle_handler")

      Abxbus.emit(:ec_idle_timeout, ECIdleEvent.new())

      # With a very short timeout, the GenServer call should exit
      # We catch the timeout to verify it doesn't hang
      start_time = System.monotonic_time(:millisecond)

      try do
        Abxbus.wait_until_idle(:ec_idle_timeout, 10)
      catch
        :exit, {:timeout, _} -> :ok
      end

      elapsed = System.monotonic_time(:millisecond) - start_time
      assert elapsed < 200, "wait_until_idle with short timeout should return quickly, took #{elapsed}ms"

      # Clean up: wait for handler to finish before stopping
      Abxbus.wait_until_idle(:ec_idle_timeout)
      Abxbus.stop(:ec_idle_timeout, clear: true)
    end
  end
end

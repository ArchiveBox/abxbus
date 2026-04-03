defmodule Abxbus.EventbusErrorHandlingTest do
  @moduledoc """
  Tests for error types and isolation — handler errors are captured,
  do not crash the bus, and do not prevent other handlers from running.

  Port of tests/test_eventbus_error_handling.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(EHCrashEvent)
  defevent(EHIsolationEvent)
  defevent(EHTimeoutEvent)
  defevent(EHSubsequentEvent)

  describe "handler error capture" do
    test "handler error captured and does not crash bus" do
      {:ok, _} = Abxbus.start_bus(:eh_crash)

      Abxbus.on(:eh_crash, EHCrashEvent, fn _e ->
        raise "boom"
      end, handler_name: "crashing_handler")

      event = Abxbus.emit(:eh_crash, EHCrashEvent.new())
      Abxbus.wait_until_idle(:eh_crash)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      crash_result = Enum.find(results, &(&1.handler_name == "crashing_handler"))

      assert crash_result.status == :error,
             "Crashing handler should have :error status, got #{crash_result.status}"

      # Bus still works — emit another event and verify it processes fine
      {:ok, agent} = Agent.start_link(fn -> 0 end)

      Abxbus.on(:eh_crash, EHSubsequentEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end, handler_name: "followup_handler")

      Abxbus.emit(:eh_crash, EHSubsequentEvent.new())
      Abxbus.wait_until_idle(:eh_crash)

      assert Agent.get(agent, & &1) == 1,
             "Bus should still process events after a handler error"
    end
  end

  describe "handler error isolation" do
    test "handler error does not prevent other handlers from running" do
      {:ok, _} = Abxbus.start_bus(:eh_isolate,
        event_handler_concurrency: :parallel
      )

      {:ok, agent} = Agent.start_link(fn -> 0 end)

      Abxbus.on(:eh_isolate, EHIsolationEvent, fn _e ->
        raise "boom"
      end, handler_name: "failing_handler")

      Abxbus.on(:eh_isolate, EHIsolationEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        "success"
      end, handler_name: "working_handler")

      event = Abxbus.emit(:eh_isolate, EHIsolationEvent.new())
      Abxbus.wait_until_idle(:eh_isolate)

      assert Agent.get(agent, & &1) == 1,
             "Working handler should still execute despite failing handler"

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      working = Enum.find(results, &(&1.handler_name == "working_handler"))
      failing = Enum.find(results, &(&1.handler_name == "failing_handler"))

      assert working.status == :completed
      assert failing.status == :error
    end
  end

  describe "handler timeout" do
    test "handler timeout marks error result" do
      {:ok, _} = Abxbus.start_bus(:eh_timeout,
        event_handler_timeout: 0.05
      )

      Abxbus.on(:eh_timeout, EHTimeoutEvent, fn _e ->
        Process.sleep(200)
        "slow"
      end, handler_name: "slow_handler")

      event = Abxbus.emit(:eh_timeout, EHTimeoutEvent.new())
      Abxbus.wait_until_idle(:eh_timeout)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      slow_result = Enum.find(results, &(&1.handler_name == "slow_handler"))

      assert slow_result.status == :error,
             "Timed-out handler should have :error status, got #{slow_result.status}"
    end
  end

  describe "error isolation across events" do
    test "error in one event does not affect subsequent events" do
      {:ok, _} = Abxbus.start_bus(:eh_subsequent)

      Abxbus.on(:eh_subsequent, EHCrashEvent, fn _e ->
        raise "boom"
      end, handler_name: "crasher")

      # First event fails
      Abxbus.emit(:eh_subsequent, EHCrashEvent.new())
      Abxbus.wait_until_idle(:eh_subsequent)

      # Second event also triggers the failing handler, but the bus is still alive
      event2 = Abxbus.emit(:eh_subsequent, EHCrashEvent.new())
      Abxbus.wait_until_idle(:eh_subsequent)

      stored2 = Abxbus.EventStore.get(event2.event_id)
      results2 = Map.values(stored2.event_results)

      assert length(results2) > 0,
             "Bus should still process events after previous errors"

      crasher = Enum.find(results2, &(&1.handler_name == "crasher"))
      assert crasher.status == :error,
             "Handler should still report :error status on second invocation"
    end
  end
end

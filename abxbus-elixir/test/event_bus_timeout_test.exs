defmodule Abxbus.EventBusTimeoutTest do
  @moduledoc """
  Tests for timeout enforcement at event and handler levels,
  including hard caps, parallel timeout, and aborted vs cancelled semantics.

  Port of tests/test_eventbus_timeout.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(TimeoutFocusedEvent, event_timeout: 0.2, event_handler_timeout: 0.01)
  defevent(HardCapEvent, event_timeout: 0.15)
  defevent(HardCapParallelEvent, event_timeout: 0.1)
  defevent(MixedTimeoutEvent, event_timeout: 0.15)

  describe "handler timeout" do
    test "slow handler times out, fast handler succeeds" do
      {:ok, _} = Abxbus.start_bus(:hto,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:hto, TimeoutFocusedEvent, fn _event ->
        Process.sleep(50)
        "slow"
      end, handler_name: "slow_handler")

      Abxbus.on(:hto, TimeoutFocusedEvent, fn _event ->
        "fast"
      end, handler_name: "fast_handler")

      event = Abxbus.emit(:hto, TimeoutFocusedEvent.new())
      Abxbus.wait_until_idle(:hto)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      fast_result = Enum.find(results, &(&1.handler_name == "fast_handler"))
      slow_result = Enum.find(results, &(&1.handler_name == "slow_handler"))

      assert fast_result.status == :completed
      assert fast_result.result == "fast"

      assert slow_result.status == :error
    end
  end

  describe "event timeout hard cap" do
    test "event timeout cancels remaining serial handlers" do
      {:ok, _} = Abxbus.start_bus(:hcap,
        event_handler_concurrency: :serial
      )

      Abxbus.on(:hcap, HardCapEvent, fn _event ->
        Process.sleep(50)
        "first"
      end, handler_name: "first")

      Abxbus.on(:hcap, HardCapEvent, fn _event ->
        Process.sleep(200)
        "second"
      end, handler_name: "second")

      Abxbus.on(:hcap, HardCapEvent, fn _event ->
        "pending"
      end, handler_name: "pending")

      event = Abxbus.emit(:hcap, HardCapEvent.new())
      Abxbus.wait_until_idle(:hcap)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      # First handler should complete (30ms < 50ms)
      first = Enum.find(results, &(&1.handler_name == "first"))
      assert first.status == :completed

      # Remaining handlers should be error (aborted or cancelled)
      non_first = Enum.filter(results, &(&1.handler_name != "first"))

      second = Enum.find(non_first, &(&1.handler_name == "second"))
      pending = Enum.find(non_first, &(&1.handler_name == "pending"))

      # Second handler was running when timeout fired — should be aborted (error)
      assert second.status == :error,
             "Running handler 'second' should be aborted (error), got #{second.status}"

      # Pending handler never started — should be error (aborted by event timeout)
      assert pending.status == :error,
             "Never-started handler 'pending' should be error, got #{pending.status}"
    end

    test "event timeout aborts all parallel handlers" do
      {:ok, _} = Abxbus.start_bus(:hcpar,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:hcpar, HardCapParallelEvent, fn _event ->
        Process.sleep(100)
        "handler1"
      end, handler_name: "h1")

      Abxbus.on(:hcpar, HardCapParallelEvent, fn _event ->
        Process.sleep(100)
        "handler2"
      end, handler_name: "h2")

      event = Abxbus.emit(:hcpar, HardCapParallelEvent.new())
      Abxbus.wait_until_idle(:hcpar)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      assert length(results) == 2

      for r <- results do
        assert r.status == :error,
               "Handler #{r.handler_name} should be error, got #{r.status}"
      end
    end
  end

  describe "handler timeout preserves event timeout" do
    test "handler decorator timeout and event timeout both enforced" do
      {:ok, _} = Abxbus.start_bus(:mixed,
        event_handler_concurrency: :parallel
      )

      # Handler with short timeout (decorator-style)
      Abxbus.on(:mixed, MixedTimeoutEvent, fn _event ->
        Process.sleep(100)
        "decorated"
      end, handler_name: "decorated", timeout: 0.01)

      # Handler without explicit timeout — sleeps longer than event_timeout
      Abxbus.on(:mixed, MixedTimeoutEvent, fn _event ->
        Process.sleep(500)
        "long_running"
      end, handler_name: "long_running")

      event = Abxbus.emit(:mixed, MixedTimeoutEvent.new())
      Abxbus.wait_until_idle(:mixed)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      decorated = Enum.find(results, &(&1.handler_name == "decorated"))
      long_running = Enum.find(results, &(&1.handler_name == "long_running"))

      # Decorated should timeout from handler timeout (0.01s)
      assert decorated.status == :error

      # Long running should timeout from event timeout (0.05s)
      assert long_running.status == :error
    end
  end

  describe "followup events after timeout" do
    test "events emitted after parent timeout still execute" do
      {:ok, _} = Abxbus.start_bus(:tail, event_concurrency: :bus_serial)

      defevent(TimeoutParentEvent, event_timeout: 0.02)
      defevent(TimeoutChildEvent)
      defevent(TailEvent)

      tail_counter = :counters.new(1, [:atomics])

      Abxbus.on(:tail, TimeoutParentEvent, fn _event ->
        child = Abxbus.emit(:tail, TimeoutChildEvent.new())
        Abxbus.await(child)
        Process.sleep(50)  # Will exceed 0.02s timeout
        :ok
      end)

      Abxbus.on(:tail, TimeoutChildEvent, fn _event -> :ok end)

      Abxbus.on(:tail, TailEvent, fn _event ->
        :counters.add(tail_counter, 1, 1)
        :ok
      end)

      parent = Abxbus.emit(:tail, TimeoutParentEvent.new())

      # Give parent time to timeout
      Process.sleep(80)

      # Assert parent actually timed out
      parent_stored = Abxbus.EventStore.get(parent.event_id)
      parent_results = Map.values(parent_stored.event_results)
      assert Enum.any?(parent_results, fn r -> r.status == :error end),
             "Parent should have timed out with error results"

      # Dispatch tail event
      Abxbus.emit(:tail, TailEvent.new())

      Abxbus.wait_until_idle(:tail)

      assert :counters.get(tail_counter, 1) == 1,
             "Tail event should still execute after parent timeout"
    end
  end
end

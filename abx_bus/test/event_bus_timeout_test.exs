defmodule AbxBus.EventBusTimeoutTest do
  @moduledoc """
  Tests for timeout enforcement at event and handler levels,
  including hard caps, parallel timeout, and aborted vs cancelled semantics.

  Port of tests/test_eventbus_timeout.py.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  defevent(TimeoutFocusedEvent, event_timeout: 0.2, event_handler_timeout: 0.01)
  defevent(HardCapEvent, event_timeout: 0.05)
  defevent(HardCapParallelEvent, event_timeout: 0.03)
  defevent(MixedTimeoutEvent, event_timeout: 0.05)

  describe "handler timeout" do
    test "slow handler times out, fast handler succeeds" do
      {:ok, _} = AbxBus.start_bus(:hto,
        event_handler_concurrency: :parallel
      )

      AbxBus.on(:hto, TimeoutFocusedEvent, fn _event ->
        Process.sleep(50)
        "slow"
      end, handler_name: "slow_handler")

      AbxBus.on(:hto, TimeoutFocusedEvent, fn _event ->
        "fast"
      end, handler_name: "fast_handler")

      event = AbxBus.emit(:hto, TimeoutFocusedEvent.new())
      AbxBus.wait_until_idle(:hto)

      stored = AbxBus.EventStore.get(event.event_id)
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
      {:ok, _} = AbxBus.start_bus(:hcap,
        event_handler_concurrency: :serial
      )

      AbxBus.on(:hcap, HardCapEvent, fn _event ->
        Process.sleep(30)
        "first"
      end, handler_name: "first")

      AbxBus.on(:hcap, HardCapEvent, fn _event ->
        Process.sleep(30)
        "second"
      end, handler_name: "second")

      AbxBus.on(:hcap, HardCapEvent, fn _event ->
        "pending"
      end, handler_name: "pending")

      event = AbxBus.emit(:hcap, HardCapEvent.new())
      AbxBus.wait_until_idle(:hcap)

      stored = AbxBus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      # First handler should complete (30ms < 50ms)
      first = Enum.find(results, &(&1.handler_name == "first"))
      assert first.status == :completed

      # Remaining handlers should be error (aborted or cancelled)
      non_first = Enum.filter(results, &(&1.handler_name != "first"))

      for r <- non_first do
        assert r.status in [:error, :cancelled],
               "Handler #{r.handler_name} should be error or cancelled, got #{r.status}"
      end
    end

    test "event timeout aborts all parallel handlers" do
      {:ok, _} = AbxBus.start_bus(:hcpar,
        event_handler_concurrency: :parallel
      )

      AbxBus.on(:hcpar, HardCapParallelEvent, fn _event ->
        Process.sleep(100)
        "handler1"
      end, handler_name: "h1")

      AbxBus.on(:hcpar, HardCapParallelEvent, fn _event ->
        Process.sleep(100)
        "handler2"
      end, handler_name: "h2")

      event = AbxBus.emit(:hcpar, HardCapParallelEvent.new())
      AbxBus.wait_until_idle(:hcpar)

      stored = AbxBus.EventStore.get(event.event_id)
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
      {:ok, _} = AbxBus.start_bus(:mixed,
        event_handler_concurrency: :parallel
      )

      # Handler with short timeout (decorator-style)
      AbxBus.on(:mixed, MixedTimeoutEvent, fn _event ->
        Process.sleep(100)
        "decorated"
      end, handler_name: "decorated", timeout: 0.01)

      # Handler without explicit timeout
      AbxBus.on(:mixed, MixedTimeoutEvent, fn _event ->
        Process.sleep(100)
        "long_running"
      end, handler_name: "long_running")

      event = AbxBus.emit(:mixed, MixedTimeoutEvent.new())
      AbxBus.wait_until_idle(:mixed)

      stored = AbxBus.EventStore.get(event.event_id)
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
      {:ok, _} = AbxBus.start_bus(:tail, event_concurrency: :bus_serial)

      defevent(TimeoutParentEvent, event_timeout: 0.02)
      defevent(TimeoutChildEvent)
      defevent(TailEvent)

      tail_counter = :counters.new(1, [:atomics])

      AbxBus.on(:tail, TimeoutParentEvent, fn _event ->
        child = AbxBus.emit(:tail, TimeoutChildEvent.new())
        AbxBus.await(child)
        Process.sleep(50)  # Will exceed 0.02s timeout
        :ok
      end)

      AbxBus.on(:tail, TimeoutChildEvent, fn _event -> :ok end)

      AbxBus.on(:tail, TailEvent, fn _event ->
        :counters.add(tail_counter, 1, 1)
        :ok
      end)

      _parent = AbxBus.emit(:tail, TimeoutParentEvent.new())

      # Give parent time to timeout
      Process.sleep(40)

      # Dispatch tail event
      AbxBus.emit(:tail, TailEvent.new())

      AbxBus.wait_until_idle(:tail)

      assert :counters.get(tail_counter, 1) == 1,
             "Tail event should still execute after parent timeout"
    end
  end
end

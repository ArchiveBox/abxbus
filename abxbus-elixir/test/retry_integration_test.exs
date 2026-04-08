defmodule Abxbus.RetryIntegrationTest do
  @moduledoc """
  Integration tests for retry behaviour: max_attempts, retry_after,
  retry_backoff_factor, and retry_on_errors filtering.

  Port of tests/test_eventbus_retry_integration.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(RetrySuccessEvent)
  defevent(RetryExhaustEvent)
  defevent(RetryFilterNoMatchEvent)
  defevent(RetryFilterMatchEvent)
  defevent(RetryBackoffEvent)
  defevent(RetryNoRetryEvent)
  defevent(RetrySemaphoreEvent)
  defevent(RetryTimeoutEvent)

  # ── Helpers ────────────────────────────────────────────────────────────────

  defp unique_bus(base) do
    :"#{base}_#{System.unique_integer([:positive])}"
  end

  # ── Tests ──────────────────────────────────────────────────────────────────

  describe "retry integration" do
    test "retry handler retries on failure and succeeds" do
      bus = unique_bus(:retry_ok)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetrySuccessEvent, fn _event ->
        n = :counters.get(counter, 1) + 1
        :counters.put(counter, 1, n)

        if n < 3 do
          raise "transient failure #{n}"
        end

        :success
      end, max_attempts: 3, retry_after: 0.05, handler_name: "retry_handler")

      event = Abxbus.emit(bus, RetrySuccessEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "retry_handler"))

      assert :counters.get(counter, 1) == 3,
             "Handler should have been called 3 times, got #{:counters.get(counter, 1)}"

      assert handler_result.status == :completed,
             "Final result should be :completed, got #{handler_result.status}"

      Abxbus.stop(bus, clear: true)
    end

    test "retry exhausts all attempts and marks error" do
      bus = unique_bus(:retry_exhaust)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetryExhaustEvent, fn _event ->
        :counters.add(counter, 1, 1)
        raise "permanent failure"
      end, max_attempts: 3, retry_after: 0.01, handler_name: "exhaust_handler")

      event = Abxbus.emit(bus, RetryExhaustEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "exhaust_handler"))

      assert :counters.get(counter, 1) == 3,
             "Handler should have been called 3 times, got #{:counters.get(counter, 1)}"

      assert handler_result.status == :error,
             "Result should be :error after exhausting retries, got #{handler_result.status}"

      Abxbus.stop(bus, clear: true)
    end

    test "retry_on_errors only retries matching errors" do
      bus = unique_bus(:retry_nomatch)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetryFilterNoMatchEvent, fn _event ->
        :counters.add(counter, 1, 1)
        raise RuntimeError, "not in retry list"
      end,
        max_attempts: 3,
        retry_after: 0.01,
        retry_on_errors: [ArgumentError],
        handler_name: "nomatch_handler"
      )

      event = Abxbus.emit(bus, RetryFilterNoMatchEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "nomatch_handler"))

      assert :counters.get(counter, 1) == 1,
             "Handler should have been called only once (RuntimeError not in retry list), got #{:counters.get(counter, 1)}"

      assert handler_result.status == :error

      Abxbus.stop(bus, clear: true)
    end

    test "retry_on_errors retries matching errors" do
      bus = unique_bus(:retry_match)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetryFilterMatchEvent, fn _event ->
        n = :counters.get(counter, 1) + 1
        :counters.put(counter, 1, n)

        if n < 3 do
          raise ArgumentError, "transient arg error #{n}"
        end

        :recovered
      end,
        max_attempts: 3,
        retry_after: 0.01,
        retry_on_errors: [ArgumentError],
        handler_name: "match_handler"
      )

      event = Abxbus.emit(bus, RetryFilterMatchEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "match_handler"))

      assert :counters.get(counter, 1) == 3,
             "Handler should have been called 3 times, got #{:counters.get(counter, 1)}"

      assert handler_result.status == :completed,
             "Final result should be :completed, got #{handler_result.status}"

      Abxbus.stop(bus, clear: true)
    end

    test "retry with backoff delays between attempts" do
      bus = unique_bus(:retry_backoff)
      {:ok, _} = Abxbus.start_bus(bus)

      {:ok, timestamps} = Agent.start_link(fn -> [] end)

      Abxbus.on(bus, RetryBackoffEvent, fn _event ->
        Agent.update(timestamps, &[System.monotonic_time(:millisecond) | &1])
        raise "backoff failure"
      end,
        max_attempts: 3,
        retry_after: 0.05,
        retry_backoff_factor: 2.0,
        handler_name: "backoff_handler"
      )

      event = Abxbus.emit(bus, RetryBackoffEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "backoff_handler"))

      assert handler_result.status == :error

      ts = Agent.get(timestamps, & &1) |> Enum.reverse()
      assert length(ts) == 3, "Expected 3 timestamps, got #{length(ts)}"

      # First delay should be ~50ms (retry_after=0.05), second ~100ms (0.05 * 2.0)
      [t0, t1, t2] = ts
      delay1 = t1 - t0
      delay2 = t2 - t1

      assert delay1 >= 30,
             "First delay should be ~50ms, got #{delay1}ms"

      assert delay2 >= delay1 * 1.3,
             "Second delay (#{delay2}ms) should be noticeably larger than first (#{delay1}ms) due to backoff"

      Abxbus.stop(bus, clear: true)
    end

    test "retry with semaphore limits concurrent handlers" do
      bus = unique_bus(:retry_sem)
      {:ok, _} = Abxbus.start_bus(bus, event_handler_concurrency: :parallel)

      max_concurrent = :atomics.new(1, [])
      current = :atomics.new(1, [])

      handler_fn = fn _event ->
        :atomics.add(current, 1, 1)
        cur = :atomics.get(current, 1)
        # CAS loop to update max concurrency atomically
        update_max = fn update_max_fn ->
          old_max = :atomics.get(max_concurrent, 1)
          if cur > old_max do
            case :atomics.compare_exchange(max_concurrent, 1, old_max, cur) do
              :ok -> :ok
              _ -> update_max_fn.(update_max_fn)
            end
          else
            :ok
          end
        end
        update_max.(update_max)
        Process.sleep(50)
        :atomics.sub(current, 1, 1)
        :ok
      end

      for i <- 1..4 do
        Abxbus.on(bus, RetrySemaphoreEvent, handler_fn,
          handler_name: "sem_handler_#{i}",
          semaphore_scope: :global,
          semaphore_name: "test_sem",
          semaphore_limit: 2
        )
      end

      event = Abxbus.emit(bus, RetrySemaphoreEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      completed = stored.event_results |> Map.values() |> Enum.filter(&(&1.status == :completed))
      assert length(completed) == 4, "All 4 handlers should complete"

      assert :atomics.get(max_concurrent, 1) <= 2,
             "Max concurrent should be <= 2, got #{:atomics.get(max_concurrent, 1)}"

      Abxbus.stop(bus, clear: true)
    end

    test "retry timeout aborts handler" do
      bus = unique_bus(:retry_timeout)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetryTimeoutEvent, fn _event ->
        :counters.add(counter, 1, 1)
        # Sleep longer than the timeout
        Process.sleep(2000)
        :ok
      end,
        max_attempts: 3,
        retry_after: 0.01,
        timeout: 0.05,
        handler_name: "timeout_handler"
      )

      event = Abxbus.emit(bus, RetryTimeoutEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "timeout_handler"))

      assert handler_result.status == :error,
             "Handler should be in error status after timeout, got #{handler_result.status}"

      Abxbus.stop(bus, clear: true)
    end

    test "max_attempts 1 means no retries" do
      bus = unique_bus(:retry_noretry)
      {:ok, _} = Abxbus.start_bus(bus)

      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetryNoRetryEvent, fn _event ->
        :counters.add(counter, 1, 1)
        raise "single attempt failure"
      end, max_attempts: 1, handler_name: "noretry_handler")

      event = Abxbus.emit(bus, RetryNoRetryEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      handler_result = Enum.find(results, &(&1.handler_name == "noretry_handler"))

      assert :counters.get(counter, 1) == 1,
             "Handler should have been called exactly once, got #{:counters.get(counter, 1)}"

      assert handler_result.status == :error

      Abxbus.stop(bus, clear: true)
    end

    test "retry wrapping emit retries full dispatch cycle" do
      bus = unique_bus(:retry_dispatch_cycle)
      {:ok, _} = Abxbus.start_bus(bus)

      # Simpler approach: register a handler with max_attempts: 3 that fails
      # twice and then succeeds. Produces exactly 1 result (completed).
      counter = :counters.new(1, [:atomics])

      Abxbus.on(bus, RetrySuccessEvent, fn _event ->
        n = :counters.get(counter, 1) + 1
        :counters.put(counter, 1, n)

        if n < 3 do
          raise "attempt #{n} fails"
        else
          "success_on_#{n}"
        end
      end, max_attempts: 3, retry_after: 0.05, handler_name: "dispatch_cycle_handler")

      event = Abxbus.emit(bus, RetrySuccessEvent.new())
      Abxbus.wait_until_idle(bus)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      handler_results =
        Enum.filter(results, &(&1.handler_name == "dispatch_cycle_handler"))

      assert length(handler_results) == 1,
             "Expected exactly 1 result for the retrying handler, got #{length(handler_results)}"

      [handler_result] = handler_results
      assert handler_result.status == :completed
      assert handler_result.result == "success_on_3"

      assert :counters.get(counter, 1) == 3,
             "Handler should have been called 3 times total (2 failures + 1 success)"

      Abxbus.stop(bus, clear: true)
    end
  end
end

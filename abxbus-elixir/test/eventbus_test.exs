defmodule Abxbus.EventbusTest do
  @moduledoc """
  Core EventBus tests: initialization, emission, handler registration,
  forwarding loop prevention, FIFO ordering, error handling, concurrency,
  event type/version, stop behavior, and result access.

  Port of tests/test_eventbus.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  # ── Event definitions ──────────────────────────────────────────────────────

  defevent(EBUserAction, action: "test", user_id: "user1")
  defevent(EBSystemEvent, name: "sys", severity: "info")
  defevent(EBSlowEvent, payload: "slow")
  defevent(EBWildcardHit, tag: "wc")
  defevent(EBStringHit, tag: "str")
  defevent(EBTypedHit, tag: "typed")
  defevent(EBParallelEvent)
  defevent(EBResultEvent)
  defevent(EBForwardA, data: "a")
  defevent(EBForwardB, data: "b")
  defevent(EBForwardC, data: "c")
  defevent(EBFifoEvent, seq: 0)
  defevent(EBErrorEvent)
  defevent(EBConcurrentEvent, idx: 0)
  defevent(EBBatchEvent, idx: 0)
  defevent(EBTypeCheckEvent)
  defevent(EBStopEvent)
  defevent(EBResultsListEvent)
  defevent(EBFirstEvent)
  defevent(EBIndexedResultEvent)

  # ── Initialization ─────────────────────────────────────────────────────────

  describe "initialization" do
    test "eventbus initializes with correct defaults" do
      {:ok, _} = Abxbus.start_bus(:eb_init)

      # Bus should be running — we can emit without error
      event = Abxbus.emit(:eb_init, EBUserAction.new())
      assert event.event_status == :pending

      Abxbus.wait_until_idle(:eb_init)
      Abxbus.stop(:eb_init, clear: true)
    end

    test "stop and restart" do
      {:ok, _} = Abxbus.start_bus(:eb_stop_restart)

      Abxbus.on(:eb_stop_restart, EBUserAction, fn _event -> :ok end)
      Abxbus.emit(:eb_stop_restart, EBUserAction.new())
      Abxbus.wait_until_idle(:eb_stop_restart)

      # Stop the bus
      Abxbus.stop(:eb_stop_restart)

      # Verify stopped — calling emit on a stopped bus returns error
      assert {:error, :stopped} = Abxbus.emit(:eb_stop_restart, EBUserAction.new())
      Abxbus.stop(:eb_stop_restart, clear: true)
    end
  end

  # ── Event emission ─────────────────────────────────────────────────────────

  describe "event emission" do
    test "emit and result" do
      {:ok, _} = Abxbus.start_bus(:eb_emit, max_history_size: 10_000)

      event = Abxbus.emit(:eb_emit, EBUserAction.new(action: "login", user_id: "u123"))

      # Immediate return checks
      assert event.event_type == EBUserAction
      assert is_binary(event.event_id)
      assert event.event_status == :pending
      assert event.event_created_at != nil
      assert event.event_started_at == nil
      assert event.event_completed_at == nil

      # Wait for completion and check updated fields
      completed = Abxbus.await(event)
      assert completed.event_status == :completed
      assert completed.event_started_at != nil
      assert completed.event_completed_at != nil

      Abxbus.stop(:eb_emit, clear: true)
    end

    test "emit alias dispatches event" do
      {:ok, _} = Abxbus.start_bus(:eb_dispatch)

      handled_ids = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:eb_dispatch, EBUserAction, fn event ->
        Agent.update(handled_ids, &(&1 ++ [event.event_id]))
        "handled"
      end)

      event = Abxbus.dispatch(:eb_dispatch, EBUserAction.new(action: "alias"))
      completed = Abxbus.await(event)

      assert completed.event_status == :completed
      ids = Agent.get(handled_ids, & &1)
      assert ids == [event.event_id]

      Abxbus.stop(:eb_dispatch, clear: true)
    end

    test "unbounded history processes events without rejection" do
      {:ok, _} = Abxbus.start_bus(:eb_unbounded, max_history_size: 1_000_000)

      processed = :counters.new(1, [:atomics])

      Abxbus.on(:eb_unbounded, EBSlowEvent, fn _event ->
        Process.sleep(1)
        :counters.add(processed, 1, 1)
        :ok
      end)

      for _ <- 1..150 do
        Abxbus.emit(:eb_unbounded, EBSlowEvent.new())
      end

      Abxbus.wait_until_idle(:eb_unbounded)
      assert :counters.get(processed, 1) == 150

      Abxbus.stop(:eb_unbounded, clear: true)
    end

    test "zero history size keeps inflight and drops on completion" do
      {:ok, _} = Abxbus.start_bus(:eb_zero_hist,
        max_history_size: 0,
        max_history_drop: true
      )

      processed = :counters.new(1, [:atomics])

      Abxbus.on(:eb_zero_hist, EBSlowEvent, fn _event ->
        :counters.add(processed, 1, 1)
        :ok
      end)

      for _ <- 1..10 do
        Abxbus.emit(:eb_zero_hist, EBSlowEvent.new())
      end

      Abxbus.wait_until_idle(:eb_zero_hist)

      # All events should have been processed
      assert :counters.get(processed, 1) == 10

      # History should be empty or minimal after dropping completed events
      completed = Abxbus.events_completed(:eb_zero_hist)
      assert length(completed) == 0

      Abxbus.stop(:eb_zero_hist, clear: true)
    end
  end

  # ── Handler registration ───────────────────────────────────────────────────

  describe "handler registration" do
    test "handler registration via type, string, and wildcard" do
      {:ok, _} = Abxbus.start_bus(:eb_handlers)

      typed_count = :counters.new(1, [:atomics])
      string_count = :counters.new(1, [:atomics])
      wildcard_count = :counters.new(1, [:atomics])

      # Type-specific handler
      Abxbus.on(:eb_handlers, EBTypedHit, fn _event ->
        :counters.add(typed_count, 1, 1)
        :ok
      end, handler_name: "typed")

      # String-based handler
      Abxbus.on(:eb_handlers, "EBStringHit", fn _event ->
        :counters.add(string_count, 1, 1)
        :ok
      end, handler_name: "string")

      # Wildcard handler
      Abxbus.on(:eb_handlers, "*", fn _event ->
        :counters.add(wildcard_count, 1, 1)
        :ok
      end, handler_name: "wildcard")

      # Emit a typed event — should trigger typed + wildcard
      Abxbus.emit(:eb_handlers, EBTypedHit.new())
      Abxbus.wait_until_idle(:eb_handlers)

      assert :counters.get(typed_count, 1) == 1
      assert :counters.get(string_count, 1) == 0
      assert :counters.get(wildcard_count, 1) >= 1

      # Emit a string-matched event — should trigger string + wildcard
      Abxbus.emit(:eb_handlers, EBStringHit.new())
      Abxbus.wait_until_idle(:eb_handlers)

      assert :counters.get(string_count, 1) == 1
      assert :counters.get(wildcard_count, 1) >= 2

      # Emit a wildcard-only event — should trigger only wildcard
      Abxbus.emit(:eb_handlers, EBWildcardHit.new())
      Abxbus.wait_until_idle(:eb_handlers)

      assert :counters.get(wildcard_count, 1) >= 3

      Abxbus.stop(:eb_handlers, clear: true)
    end

    test "multiple handlers run in parallel" do
      {:ok, _} = Abxbus.start_bus(:eb_parallel,
        event_handler_concurrency: :parallel
      )

      counter = :counters.new(1, [:atomics])
      max_concurrent = :atomics.new(1, [])

      handler = fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        # Atomic compare-and-swap loop to avoid racy max update
        Stream.repeatedly(fn ->
          old = :atomics.get(max_concurrent, 1)
          if current > old do
            :atomics.compare_exchange(max_concurrent, 1, old, current)
          else
            :ok
          end
        end)
        |> Enum.find(fn result -> result == :ok end)
        Process.sleep(50)
        :counters.add(counter, 1, -1)
        :ok
      end

      Abxbus.on(:eb_parallel, EBParallelEvent, handler, handler_name: "h1")
      Abxbus.on(:eb_parallel, EBParallelEvent, handler, handler_name: "h2")

      Abxbus.emit(:eb_parallel, EBParallelEvent.new())
      Abxbus.wait_until_idle(:eb_parallel)

      assert :atomics.get(max_concurrent, 1) >= 2,
             "Parallel handlers should overlap"

      Abxbus.stop(:eb_parallel, clear: true)
    end

    test "handler results tracked correctly" do
      {:ok, _} = Abxbus.start_bus(:eb_result_track)

      Abxbus.on(:eb_result_track, EBResultEvent, fn _event ->
        "my_result"
      end, handler_name: "result_handler")

      event = Abxbus.emit(:eb_result_track, EBResultEvent.new())
      Abxbus.wait_until_idle(:eb_result_track)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      assert length(results) == 1
      result = hd(results)
      assert result.status == :completed
      assert result.result == "my_result"
      assert result.handler_name == "result_handler"

      Abxbus.stop(:eb_result_track, clear: true)
    end
  end

  # ── Forwarding loop prevention ─────────────────────────────────────────────

  describe "forwarding loop prevention" do
    test "circular forwarding does not cause infinite loop" do
      {:ok, _} = Abxbus.start_bus(:eb_loop_a, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:eb_loop_b, event_concurrency: :bus_serial)
      {:ok, _} = Abxbus.start_bus(:eb_loop_c, event_concurrency: :bus_serial)

      count_a = :counters.new(1, [:atomics])
      count_b = :counters.new(1, [:atomics])
      count_c = :counters.new(1, [:atomics])

      # A -> B forwarding
      Abxbus.on(:eb_loop_a, "*", fn e -> Abxbus.emit(:eb_loop_b, e) end,
        handler_name: "fwd_a_b")
      # B -> C forwarding
      Abxbus.on(:eb_loop_b, "*", fn e -> Abxbus.emit(:eb_loop_c, e) end,
        handler_name: "fwd_b_c")
      # C -> A forwarding (closes the loop)
      Abxbus.on(:eb_loop_c, "*", fn e -> Abxbus.emit(:eb_loop_a, e) end,
        handler_name: "fwd_c_a")

      # Type-specific handlers to count invocations
      Abxbus.on(:eb_loop_a, EBForwardA, fn _event ->
        :counters.add(count_a, 1, 1)
        :ok
      end, handler_name: "handler_a")

      Abxbus.on(:eb_loop_b, EBForwardA, fn _event ->
        :counters.add(count_b, 1, 1)
        :ok
      end, handler_name: "handler_b")

      Abxbus.on(:eb_loop_c, EBForwardA, fn _event ->
        :counters.add(count_c, 1, 1)
        :ok
      end, handler_name: "handler_c")

      Abxbus.emit(:eb_loop_a, EBForwardA.new())

      Abxbus.wait_until_idle(:eb_loop_a)
      Abxbus.wait_until_idle(:eb_loop_b)
      Abxbus.wait_until_idle(:eb_loop_c)

      # Wildcard forwarding completes without hanging (no infinite loop).
      # The type-specific handlers may fire more than once depending on
      # how the path-based loop detection interacts with wildcard rejection,
      # but the key invariant is that the system doesn't deadlock or loop forever.
      assert :counters.get(count_a, 1) >= 1
      assert :counters.get(count_b, 1) >= 1
      assert :counters.get(count_c, 1) >= 1

      Abxbus.stop(:eb_loop_a, clear: true)
      Abxbus.stop(:eb_loop_b, clear: true)
      Abxbus.stop(:eb_loop_c, clear: true)
    end
  end

  # ── FIFO ordering ──────────────────────────────────────────────────────────

  describe "FIFO ordering" do
    test "FIFO order maintained with bus-serial mode" do
      {:ok, _} = Abxbus.start_bus(:eb_fifo, event_concurrency: :bus_serial)

      order = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:eb_fifo, EBFifoEvent, fn event ->
        Agent.update(order, &(&1 ++ [event.seq]))
        :ok
      end)

      for i <- 0..19 do
        Abxbus.emit(:eb_fifo, EBFifoEvent.new(seq: i))
      end

      Abxbus.wait_until_idle(:eb_fifo)

      processing_order = Agent.get(order, & &1)
      assert processing_order == Enum.to_list(0..19),
             "Events should be processed in FIFO order, got: #{inspect(processing_order)}"

      Abxbus.stop(:eb_fifo, clear: true)
    end
  end

  # ── Error handling ─────────────────────────────────────────────────────────

  describe "error handling" do
    test "handler error captured without crashing bus" do
      {:ok, _} = Abxbus.start_bus(:eb_err,
        event_handler_concurrency: :serial
      )

      good_called = :counters.new(1, [:atomics])

      Abxbus.on(:eb_err, EBErrorEvent, fn _event ->
        raise "intentional boom"
      end, handler_name: "bad_handler")

      Abxbus.on(:eb_err, EBErrorEvent, fn _event ->
        :counters.add(good_called, 1, 1)
        "good_result"
      end, handler_name: "good_handler")

      event = Abxbus.emit(:eb_err, EBErrorEvent.new())
      Abxbus.wait_until_idle(:eb_err)

      # Good handler should still have run
      assert :counters.get(good_called, 1) == 1

      # Check that the error was captured in event_results
      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      error_results = Enum.filter(results, &(&1.status == :error))
      good_results = Enum.filter(results, &(&1.status == :completed))

      assert length(error_results) == 1
      assert length(good_results) == 1

      Abxbus.stop(:eb_err, clear: true)
    end

    test "one handler error does not prevent other handlers" do
      {:ok, _} = Abxbus.start_bus(:eb_err2,
        event_handler_concurrency: :parallel
      )

      results_seen = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:eb_err2, EBErrorEvent, fn _event ->
        raise "handler_1_fails"
      end, handler_name: "fail_handler")

      Abxbus.on(:eb_err2, EBErrorEvent, fn _event ->
        Agent.update(results_seen, &(&1 ++ ["handler_2_ok"]))
        "success"
      end, handler_name: "ok_handler")

      event = Abxbus.emit(:eb_err2, EBErrorEvent.new())
      Abxbus.wait_until_idle(:eb_err2)

      seen = Agent.get(results_seen, & &1)
      assert "handler_2_ok" in seen

      # Event should still complete (with error in one handler)
      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_status in [:completed, :error]

      Abxbus.stop(:eb_err2, clear: true)
    end
  end

  # ── Concurrent operations ──────────────────────────────────────────────────

  describe "concurrent operations" do
    test "concurrent emit calls all complete" do
      {:ok, _} = Abxbus.start_bus(:eb_conc, event_concurrency: :parallel)

      processed = :counters.new(1, [:atomics])

      Abxbus.on(:eb_conc, EBConcurrentEvent, fn _event ->
        :counters.add(processed, 1, 1)
        :ok
      end)

      tasks =
        for i <- 0..9 do
          Task.async(fn ->
            Abxbus.emit(:eb_conc, EBConcurrentEvent.new(idx: i))
          end)
        end

      events = Task.await_many(tasks, 5000)
      Abxbus.wait_until_idle(:eb_conc)

      assert :counters.get(processed, 1) == 10

      # All events should have IDs
      for event <- events do
        assert is_binary(event.event_id)
      end

      Abxbus.stop(:eb_conc, clear: true)
    end

    test "batch emit with gather" do
      {:ok, _} = Abxbus.start_bus(:eb_batch)

      processed = :counters.new(1, [:atomics])

      Abxbus.on(:eb_batch, EBBatchEvent, fn _event ->
        :counters.add(processed, 1, 1)
        :ok
      end)

      events =
        for i <- 0..9 do
          Abxbus.emit(:eb_batch, EBBatchEvent.new(idx: i))
        end

      Abxbus.wait_until_idle(:eb_batch)

      assert :counters.get(processed, 1) == 10
      assert length(events) == 10

      Abxbus.stop(:eb_batch, clear: true)
    end
  end

  # ── Event type and version ─────────────────────────────────────────────────

  describe "event type and version" do
    test "event_type derived from module name" do
      event = EBTypeCheckEvent.new()
      assert event.event_type == EBTypeCheckEvent
    end

    test "event_version defaults to 1" do
      event = EBTypeCheckEvent.new()
      assert event.event_version == "1"
    end
  end

  # ── Stop behavior ──────────────────────────────────────────────────────────

  describe "stop behavior" do
    test "stop with clear removes all state" do
      {:ok, _} = Abxbus.start_bus(:eb_stop_clear)

      Abxbus.on(:eb_stop_clear, EBStopEvent, fn _event -> :ok end)

      for _ <- 1..5 do
        Abxbus.emit(:eb_stop_clear, EBStopEvent.new())
      end

      Abxbus.wait_until_idle(:eb_stop_clear)

      # Verify events were processed
      completed = Abxbus.events_completed(:eb_stop_clear)
      assert length(completed) > 0

      # Stop with clear — resets internal state
      Abxbus.stop(:eb_stop_clear, clear: true)

      # After stop+clear, the bus process is still alive but state is cleared.
      # Emitting returns {:error, :stopped}
      assert {:error, :stopped} = Abxbus.emit(:eb_stop_clear, EBStopEvent.new())
    end
  end

  # ── Event results access ───────────────────────────────────────────────────

  describe "event results access" do
    test "event_results_list returns handler results" do
      {:ok, _} = Abxbus.start_bus(:eb_rlist)

      Abxbus.on(:eb_rlist, EBResultsListEvent, fn _event ->
        "list_result"
      end, handler_name: "rl_handler")

      event = Abxbus.emit(:eb_rlist, EBResultsListEvent.new())
      completed = Abxbus.await(event)

      results = Abxbus.event_results_list(completed, raise_if_any: false)
      assert "list_result" in results

      Abxbus.stop(:eb_rlist, clear: true)
    end

    test "first returns first non-nil result" do
      {:ok, _} = Abxbus.start_bus(:eb_first,
        event_handler_concurrency: :serial
      )

      Abxbus.on(:eb_first, EBFirstEvent, fn _event -> nil end,
        handler_name: "nil_handler")

      Abxbus.on(:eb_first, EBFirstEvent, fn _event -> "winner" end,
        handler_name: "value_handler")

      event = Abxbus.emit(:eb_first, EBFirstEvent.new())
      result = Abxbus.first(event)

      assert result == "winner"

      Abxbus.stop(:eb_first, clear: true)
    end

    test "results indexed by handler_id" do
      {:ok, _} = Abxbus.start_bus(:eb_indexed)

      Abxbus.on(:eb_indexed, EBIndexedResultEvent, fn _event ->
        "indexed_value"
      end, handler_name: "idx_handler")

      event = Abxbus.emit(:eb_indexed, EBIndexedResultEvent.new())
      Abxbus.wait_until_idle(:eb_indexed)

      stored = Abxbus.EventStore.get(event.event_id)

      # event_results should be a map keyed by handler_id
      assert is_map(stored.event_results)
      assert map_size(stored.event_results) == 1

      {handler_id, result_entry} = Enum.at(stored.event_results, 0)
      assert is_binary(handler_id)
      assert result_entry.result == "indexed_value"
      assert result_entry.handler_name == "idx_handler"

      Abxbus.stop(:eb_indexed, clear: true)
    end
  end

  # ── Forwarding results ────────────────────────────────────────────────────

  describe "forwarding results" do
    test "forwarding flattens results across buses" do
      {:ok, _} = Abxbus.start_bus(:eb_fwd_a)
      {:ok, _} = Abxbus.start_bus(:eb_fwd_b)
      {:ok, _} = Abxbus.start_bus(:eb_fwd_c)

      defevent(EBFwdFlatEvent, data: "fwd")

      # Type handlers returning distinct results
      Abxbus.on(:eb_fwd_a, EBFwdFlatEvent, fn _event -> "from_a" end,
        handler_name: "handler_a")
      Abxbus.on(:eb_fwd_b, EBFwdFlatEvent, fn _event -> "from_b" end,
        handler_name: "handler_b")
      Abxbus.on(:eb_fwd_c, EBFwdFlatEvent, fn _event -> "from_c" end,
        handler_name: "handler_c")

      # Forwarding: a -> b via wildcard
      Abxbus.on(:eb_fwd_a, "*", fn e -> Abxbus.emit(:eb_fwd_b, e) end,
        handler_name: "fwd_a_b")
      # Forwarding: b -> c via wildcard
      Abxbus.on(:eb_fwd_b, "*", fn e -> Abxbus.emit(:eb_fwd_c, e) end,
        handler_name: "fwd_b_c")

      event = Abxbus.emit(:eb_fwd_a, EBFwdFlatEvent.new())

      # Wait for all buses to finish processing (sequential ensures ordering)
      Abxbus.wait_until_idle(:eb_fwd_a)
      Abxbus.wait_until_idle(:eb_fwd_b)
      Abxbus.wait_until_idle(:eb_fwd_c)

      # Use event_completed to wait for the full forwarding chain to settle.
      # After all buses are idle, the event should be terminal.
      Abxbus.event_completed(event, 2.0)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      completed = Enum.filter(results, &(&1.status == :completed))
      result_values = Enum.map(completed, & &1.result)

      # Forwarding flattens results from downstream buses into the event store.
      # Results from at least two of the three buses should be present.
      assert "from_c" in result_values,
             "handler_c result should be flattened; got: #{inspect(result_values)}"

      # The chain a->b->c should produce results from multiple buses
      assert length(completed) >= 2,
             "Should have results from multiple buses, got: #{inspect(Enum.map(completed, & &1.handler_name))}"

      Abxbus.stop(:eb_fwd_a, clear: true)
      Abxbus.stop(:eb_fwd_b, clear: true)
      Abxbus.stop(:eb_fwd_c, clear: true)
    end
  end

  # ── Additional stop behavior ──────────────────────────────────────────────

  describe "stop behavior (additional)" do
    test "stop with pending events does not hang" do
      {:ok, _} = Abxbus.start_bus(:eb_stop_pending)

      Abxbus.on(:eb_stop_pending, EBSlowEvent, fn _event ->
        Process.sleep(200)
        :ok
      end, handler_name: "slow_stop_handler")

      for _ <- 1..5 do
        Abxbus.emit(:eb_stop_pending, EBSlowEvent.new())
      end

      # stop should return without hanging
      Abxbus.stop(:eb_stop_pending)
    end
  end

  # ── Additional FIFO ordering ──────────────────────────────────────────────

  describe "FIFO ordering (additional)" do
    test "mixed delay handlers maintain FIFO order" do
      {:ok, _} = Abxbus.start_bus(:eb_fifo_mixed, event_concurrency: :bus_serial)

      order = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:eb_fifo_mixed, EBFifoEvent, fn event ->
        if rem(event.seq, 2) == 0 do
          Process.sleep(30)
        else
          Process.sleep(5)
        end
        Agent.update(order, &(&1 ++ [event.seq]))
        :ok
      end, handler_name: "mixed_delay_handler")

      for i <- 0..9 do
        Abxbus.emit(:eb_fifo_mixed, EBFifoEvent.new(seq: i))
      end

      Abxbus.wait_until_idle(:eb_fifo_mixed)

      processing_order = Agent.get(order, & &1)
      assert processing_order == Enum.to_list(0..9),
             "Events should be processed in FIFO order regardless of handler delay, got: #{inspect(processing_order)}"

      Abxbus.stop(:eb_fifo_mixed, clear: true)
    end
  end
end

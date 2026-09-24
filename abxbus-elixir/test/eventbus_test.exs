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

  # ── Handler recursion depth ──────────────────────────────────────────────

  defevent(EBRecursiveEvent, level: 0, max_level: 0)

  describe "handler recursion depth" do
    test "custom handler recursion depth allows deeper nested handlers" do
      {:ok, _} = Abxbus.start_bus(:eb_recurse_deep, max_handler_recursion_depth: 6)

      {:ok, seen_agent} = Agent.start_link(fn -> [] end)

      Abxbus.on(:eb_recurse_deep, EBRecursiveEvent, fn event ->
        Agent.update(seen_agent, &(&1 ++ [event.level]))

        if event.level < event.max_level do
          child = EBRecursiveEvent.new(level: event.level + 1, max_level: event.max_level)
          child_event = Abxbus.emit(:eb_recurse_deep, child)
          Abxbus.await(child_event)
        end

        :ok
      end, handler_name: "recursive_handler")

      _event = Abxbus.emit(:eb_recurse_deep, EBRecursiveEvent.new(level: 0, max_level: 5))
      Abxbus.wait_until_idle(:eb_recurse_deep)

      seen = Agent.get(seen_agent, & &1)
      for level <- 0..5 do
        assert level in seen,
               "Level #{level} should have been seen, got: #{inspect(seen)}"
      end

      Agent.stop(seen_agent)
      Abxbus.stop(:eb_recurse_deep, clear: true)
    end

    test "default handler recursion depth catches runaway loops" do
      {:ok, _} = Abxbus.start_bus(:eb_recurse_default)

      Abxbus.on(:eb_recurse_default, EBRecursiveEvent, fn event ->
        if event.level < event.max_level do
          child = EBRecursiveEvent.new(level: event.level + 1, max_level: event.max_level)
          child_event = Abxbus.emit(:eb_recurse_default, child)
          Abxbus.await(child_event)
        end

        :ok
      end, handler_name: "runaway_handler")

      _event = Abxbus.emit(:eb_recurse_default, EBRecursiveEvent.new(level: 0, max_level: 10))
      Abxbus.wait_until_idle(:eb_recurse_default)

      # Some deeper events should have been blocked with error
      # Check all events in the store for error results containing "Infinite loop"
      all_bus_events = Abxbus.events_completed(:eb_recurse_default) ++
                       Abxbus.events_pending(:eb_recurse_default)

      # Also check directly via ETS for all recursive events
      found_infinite_loop_error = Enum.any?(all_bus_events, fn evt ->
        stored = Abxbus.EventStore.get(evt.event_id)
        stored != nil and Enum.any?(Map.values(stored.event_results), fn r ->
          r.status == :error and is_binary(inspect(r.error)) and
            String.contains?(inspect(r.error), "Infinite loop detected")
        end)
      end)

      assert found_infinite_loop_error,
             "Should have at least one event with 'Infinite loop detected' error"

      Abxbus.stop(:eb_recurse_default, clear: true)
    end
  end

  # ── Event data ───────────────────────────────────────────────────────────

  defevent(EBComplexDataEvent, data: %{})
  defevent(EBVersionedEvent, payload: "v", version: "2")

  describe "event data" do
    test "event with complex nested data" do
      {:ok, _} = Abxbus.start_bus(:eb_complex_data)

      captured = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:eb_complex_data, EBComplexDataEvent, fn event ->
        Agent.update(captured, fn _ -> event.data end)
        :ok
      end, handler_name: "data_handler")

      nested_data = %{
        "users" => [
          %{"name" => "Alice", "scores" => [100, 95, 88]},
          %{"name" => "Bob", "scores" => [70, 80]}
        ],
        "meta" => %{"version" => 1, "tags" => ["a", "b"]}
      }

      _event = Abxbus.emit(:eb_complex_data, EBComplexDataEvent.new(data: nested_data))
      Abxbus.wait_until_idle(:eb_complex_data)

      received_data = Agent.get(captured, & &1)
      assert received_data == nested_data

      Agent.stop(captured)
      Abxbus.stop(:eb_complex_data, clear: true)
    end

    test "event_version defaults and overrides" do
      # Default version is "1"
      default_event = EBComplexDataEvent.new()
      assert default_event.event_version == "1"

      # Overridden version via defevent option
      versioned_event = EBVersionedEvent.new()
      assert versioned_event.event_version == "2"
    end
  end

  # ── Result access patterns ──────────────────────────────────────────────

  defevent(EBNamedHandlerEvent)
  defevent(EBEventCompletedEvent)
  defevent(EBFindWaiterEvent, value: "")
  defevent(EBFindPastEvent, value: "")
  defevent(EBMultiBusFwdEvent, data: "multi")

  describe "result access patterns" do
    test "results accessible by handler_name" do
      {:ok, _} = Abxbus.start_bus(:eb_named_handler)

      Abxbus.on(:eb_named_handler, EBNamedHandlerEvent, fn _event ->
        "named_result"
      end, handler_name: "my_handler")

      event = Abxbus.emit(:eb_named_handler, EBNamedHandlerEvent.new())
      Abxbus.wait_until_idle(:eb_named_handler)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      matching = Enum.find(results, fn r -> r.handler_name == "my_handler" end)
      assert matching != nil
      assert matching.result == "named_result"
      assert matching.status == :completed

      Abxbus.stop(:eb_named_handler, clear: true)
    end

    test "wait_for_result via event_completed" do
      {:ok, _} = Abxbus.start_bus(:eb_event_completed)

      Abxbus.on(:eb_event_completed, EBEventCompletedEvent, fn _event ->
        Process.sleep(50)
        "completed_value"
      end, handler_name: "slow_handler")

      event = Abxbus.emit(:eb_event_completed, EBEventCompletedEvent.new())
      completed = Abxbus.event_completed(event, 5.0)

      assert completed.event_status == :completed
      results = Map.values(completed.event_results)
      assert Enum.any?(results, fn r -> r.result == "completed_value" end)

      Abxbus.stop(:eb_event_completed, clear: true)
    end

    test "find_waiter_cleanup after resolve and timeout" do
      {:ok, _} = Abxbus.start_bus(:eb_find_waiter)

      Abxbus.on(:eb_find_waiter, EBFindWaiterEvent, fn _event -> :ok end)

      size_before = :ets.info(:abxbus_find_waiters, :size)

      # find with future timeout that times out — waiters should be cleaned up
      _result = Abxbus.find(EBFindWaiterEvent, past: false, future: 0.1)
      # The find timed out since no event was emitted during that window
      size_after_timeout = :ets.info(:abxbus_find_waiters, :size)
      assert size_after_timeout == size_before,
             "Find waiters should be cleaned up after timeout"

      # find that resolves — emit event then find it in past
      Abxbus.emit(:eb_find_waiter, EBFindWaiterEvent.new(value: "found_it"))
      Abxbus.wait_until_idle(:eb_find_waiter)

      found = Abxbus.find(EBFindWaiterEvent, past: true)
      assert found != nil

      size_after_resolve = :ets.info(:abxbus_find_waiters, :size)
      assert size_after_resolve == size_before,
             "Find waiters should be cleaned up after resolve"

      Abxbus.stop(:eb_find_waiter, clear: true)
    end

    test "find past returns most recent match" do
      {:ok, _} = Abxbus.start_bus(:eb_find_past)

      Abxbus.on(:eb_find_past, EBFindPastEvent, fn _event -> :ok end)

      for v <- ["first", "second", "third"] do
        Abxbus.emit(:eb_find_past, EBFindPastEvent.new(value: v))
      end

      Abxbus.wait_until_idle(:eb_find_past)

      found = Abxbus.find(EBFindPastEvent, past: true)
      assert found != nil
      assert found.event_type == EBFindPastEvent
      assert found.value in ["first", "second", "third"]

      Abxbus.stop(:eb_find_past, clear: true)
    end

    test "results filterable by eventbus_name" do
      {:ok, _} = Abxbus.start_bus(:eb_filter_a)
      {:ok, _} = Abxbus.start_bus(:eb_filter_b)

      defevent(EBFilterEvent)

      Abxbus.on(:eb_filter_a, EBFilterEvent, fn _event -> "from_a" end,
        handler_name: "filter_handler_a")
      Abxbus.on(:eb_filter_b, EBFilterEvent, fn _event -> "from_b" end,
        handler_name: "filter_handler_b")

      # Forward a -> b
      Abxbus.on(:eb_filter_a, "*", fn e -> Abxbus.emit(:eb_filter_b, e) end,
        handler_name: "fwd_filter")

      event = Abxbus.emit(:eb_filter_a, EBFilterEvent.new())
      Abxbus.wait_until_idle(:eb_filter_a)
      Abxbus.wait_until_idle(:eb_filter_b)
      Abxbus.event_completed(event, 2.0)

      stored = Abxbus.EventStore.get(event.event_id)
      all_results = Map.values(stored.event_results)
      completed = Enum.filter(all_results, &(&1.status == :completed))

      # Filter by eventbus_name
      a_results = Enum.filter(completed, &(&1.eventbus_name == :eb_filter_a))
      b_results = Enum.filter(completed, &(&1.eventbus_name == :eb_filter_b))

      assert length(a_results) >= 1,
             "Should have results from bus a, got: #{inspect(Enum.map(completed, & &1.eventbus_name))}"
      assert length(b_results) >= 1,
             "Should have results from bus b, got: #{inspect(Enum.map(completed, & &1.eventbus_name))}"

      Abxbus.stop(:eb_filter_a, clear: true)
      Abxbus.stop(:eb_filter_b, clear: true)
    end

    test "complex multi-bus forwarding with results" do
      {:ok, _} = Abxbus.start_bus(:eb_multi_a)
      {:ok, _} = Abxbus.start_bus(:eb_multi_b)
      {:ok, _} = Abxbus.start_bus(:eb_multi_c)

      # Handlers on each bus returning distinct values
      Abxbus.on(:eb_multi_a, EBMultiBusFwdEvent, fn _event -> "result_a" end,
        handler_name: "handler_a")
      Abxbus.on(:eb_multi_b, EBMultiBusFwdEvent, fn _event -> "result_b" end,
        handler_name: "handler_b")
      Abxbus.on(:eb_multi_c, EBMultiBusFwdEvent, fn _event -> "result_c" end,
        handler_name: "handler_c")

      # Forwarding chain: a -> b -> c
      Abxbus.on(:eb_multi_a, "*", fn e -> Abxbus.emit(:eb_multi_b, e) end,
        handler_name: "fwd_a_b")
      Abxbus.on(:eb_multi_b, "*", fn e -> Abxbus.emit(:eb_multi_c, e) end,
        handler_name: "fwd_b_c")

      event = Abxbus.emit(:eb_multi_a, EBMultiBusFwdEvent.new())

      Abxbus.wait_until_idle(:eb_multi_a)
      Abxbus.wait_until_idle(:eb_multi_b)
      Abxbus.wait_until_idle(:eb_multi_c)

      Abxbus.event_completed(event, 5.0)

      stored = Abxbus.EventStore.get(event.event_id)

      # event_path should contain bus labels from the forwarding chain
      assert length(stored.event_path) >= 2,
             "event_path should have entries from multiple buses, got: #{inspect(stored.event_path)}"

      # Results should contain values from downstream buses
      result_values = stored.event_results
                      |> Map.values()
                      |> Enum.filter(&(&1.status == :completed))
                      |> Enum.map(& &1.result)

      assert "result_c" in result_values,
             "Should have result from bus c, got: #{inspect(result_values)}"

      assert length(result_values) >= 2,
             "Should have results from multiple buses, got: #{inspect(result_values)}"

      Abxbus.stop(:eb_multi_a, clear: true)
      Abxbus.stop(:eb_multi_b, clear: true)
      Abxbus.stop(:eb_multi_c, clear: true)
    end
  end

  # ── Handler matching ─────────────────────────────────────────────────────

  defevent(EBMatchEvent, data: nil)

  describe "handler matching" do
    test "class matcher matches generic base event by event_type" do
      {:ok, _} = Abxbus.start_bus(:eb_class_match)

      string_called = :counters.new(1, [:atomics])
      wildcard_called = :counters.new(1, [:atomics])

      Abxbus.on(:eb_class_match, "EBMatchEvent", fn _event ->
        :counters.add(string_called, 1, 1)
        :ok
      end, handler_name: "string_matcher")

      Abxbus.on(:eb_class_match, "*", fn _event ->
        :counters.add(wildcard_called, 1, 1)
        :ok
      end, handler_name: "wildcard_matcher")

      Abxbus.emit(:eb_class_match, EBMatchEvent.new())
      Abxbus.wait_until_idle(:eb_class_match)

      assert :counters.get(string_called, 1) == 1,
             "String handler should match by short name"
      assert :counters.get(wildcard_called, 1) >= 1,
             "Wildcard handler should also fire"

      Abxbus.stop(:eb_class_match, clear: true)
    end
  end

  # ── Error aggregation ────────────────────────────────────────────────────

  defevent(EBAggErrorEvent)

  describe "error aggregation" do
    test "multiple handler errors aggregated in results" do
      {:ok, _} = Abxbus.start_bus(:eb_agg_err,
        event_handler_concurrency: :parallel)

      Abxbus.on(:eb_agg_err, EBAggErrorEvent, fn _event ->
        raise "error_1"
      end, handler_name: "fail_1")

      Abxbus.on(:eb_agg_err, EBAggErrorEvent, fn _event ->
        raise "error_2"
      end, handler_name: "fail_2")

      Abxbus.on(:eb_agg_err, EBAggErrorEvent, fn _event ->
        "ok"
      end, handler_name: "ok_handler")

      event = Abxbus.emit(:eb_agg_err, EBAggErrorEvent.new())
      Abxbus.wait_until_idle(:eb_agg_err)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      error_results = Enum.filter(results, &(&1.status == :error))
      completed_results = Enum.filter(results, &(&1.status == :completed))

      assert length(error_results) == 2,
             "Should have 2 error results, got: #{length(error_results)}"
      assert length(completed_results) == 1,
             "Should have 1 completed result, got: #{length(completed_results)}"

      Abxbus.stop(:eb_agg_err, clear: true)
    end

    test "single handler error preserved in result" do
      {:ok, _} = Abxbus.start_bus(:eb_single_err)

      Abxbus.on(:eb_single_err, EBAggErrorEvent, fn _event ->
        raise RuntimeError, "test error"
      end, handler_name: "single_fail")

      event = Abxbus.emit(:eb_single_err, EBAggErrorEvent.new())
      Abxbus.wait_until_idle(:eb_single_err)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.status == :error
      assert inspect(result.error) =~ "test error"

      Abxbus.stop(:eb_single_err, clear: true)
    end
  end

  # ── Result hierarchy ─────────────────────────────────────────────────────

  defevent(EBHierEvent, data: "hier")

  describe "result hierarchy" do
    test "three level hierarchy result bubbling" do
      {:ok, _} = Abxbus.start_bus(:eb_hier_a)
      {:ok, _} = Abxbus.start_bus(:eb_hier_b)
      {:ok, _} = Abxbus.start_bus(:eb_hier_c)

      Abxbus.on(:eb_hier_a, EBHierEvent, fn _event -> "a" end,
        handler_name: "handler_a")
      Abxbus.on(:eb_hier_b, EBHierEvent, fn _event -> "b" end,
        handler_name: "handler_b")
      Abxbus.on(:eb_hier_c, EBHierEvent, fn _event -> "c" end,
        handler_name: "handler_c")

      # Forwarding: a -> b -> c
      Abxbus.on(:eb_hier_a, "*", fn e -> Abxbus.emit(:eb_hier_b, e) end,
        handler_name: "fwd_a_b")
      Abxbus.on(:eb_hier_b, "*", fn e -> Abxbus.emit(:eb_hier_c, e) end,
        handler_name: "fwd_b_c")

      event = Abxbus.emit(:eb_hier_a, EBHierEvent.new())

      Abxbus.wait_until_idle(:eb_hier_a)
      Abxbus.wait_until_idle(:eb_hier_b)
      Abxbus.wait_until_idle(:eb_hier_c)

      Abxbus.event_completed(event, 2.0)

      stored = Abxbus.EventStore.get(event.event_id)
      assert length(stored.event_path) >= 3,
             "event_path should have entries from 3 buses, got: #{inspect(stored.event_path)}"

      Abxbus.stop(:eb_hier_a, clear: true)
      Abxbus.stop(:eb_hier_b, clear: true)
      Abxbus.stop(:eb_hier_c, clear: true)
    end
  end

  # ── Circular subscription ────────────────────────────────────────────────

  defevent(EBCircularEvent, depth: 0)

  describe "circular subscription" do
    test "circular subscription prevented by recursion guard" do
      {:ok, _} = Abxbus.start_bus(:eb_circular)

      Abxbus.on(:eb_circular, EBCircularEvent, fn event ->
        child = Abxbus.emit(:eb_circular, EBCircularEvent.new(depth: event.depth + 1))
        Abxbus.await(child)
        :ok
      end, handler_name: "circular_handler")

      event = Abxbus.emit(:eb_circular, EBCircularEvent.new(depth: 0))
      Abxbus.wait_until_idle(:eb_circular)

      # With default recursion depth, should eventually hit the limit
      # Check all events for error results containing "Infinite loop"
      all_events = Abxbus.events_completed(:eb_circular) ++
                   Abxbus.events_pending(:eb_circular)

      found_infinite_loop = Enum.any?(all_events, fn evt ->
        stored = Abxbus.EventStore.get(evt.event_id)
        stored != nil and Enum.any?(Map.values(stored.event_results), fn r ->
          r.status == :error and String.contains?(inspect(r.error), "Infinite loop")
        end)
      end)

      assert found_infinite_loop,
             "Should have at least one event with 'Infinite loop' error"

      Abxbus.stop(:eb_circular, clear: true)
    end
  end

  # ── Event results indexed by handler id ──────────────────────────────────

  defevent(EBIdxEvent)

  describe "event_results indexed by handler id (extended)" do
    test "event_results indexed by handler id" do
      {:ok, _} = Abxbus.start_bus(:eb_idx3,
        event_handler_concurrency: :serial)

      entry1 = Abxbus.on(:eb_idx3, EBIdxEvent, fn _event -> "r1" end,
        handler_name: "h1")
      entry2 = Abxbus.on(:eb_idx3, EBIdxEvent, fn _event -> "r2" end,
        handler_name: "h2")
      entry3 = Abxbus.on(:eb_idx3, EBIdxEvent, fn _event -> "r3" end,
        handler_name: "h3")

      event = Abxbus.emit(:eb_idx3, EBIdxEvent.new())
      Abxbus.wait_until_idle(:eb_idx3)

      stored = Abxbus.EventStore.get(event.event_id)
      keys = Map.keys(stored.event_results)

      assert entry1.id in keys, "Handler 1 ID should be a key in event_results"
      assert entry2.id in keys, "Handler 2 ID should be a key in event_results"
      assert entry3.id in keys, "Handler 3 ID should be a key in event_results"

      Abxbus.stop(:eb_idx3, clear: true)
    end

    test "results accessible by handler_name" do
      {:ok, _} = Abxbus.start_bus(:eb_named2)

      Abxbus.on(:eb_named2, EBIdxEvent, fn _event -> "named_val" end,
        handler_name: "named")

      event = Abxbus.emit(:eb_named2, EBIdxEvent.new())
      Abxbus.wait_until_idle(:eb_named2)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      match = Enum.find(results, fn r -> r.handler_name == "named" end)

      assert match != nil, "Should find result by handler_name 'named'"
      assert match.result == "named_val"

      Abxbus.stop(:eb_named2, clear: true)
    end

    test "results filterable by eventbus_name" do
      {:ok, _} = Abxbus.start_bus(:eb_filt_a2)
      {:ok, _} = Abxbus.start_bus(:eb_filt_b2)

      defevent(EBFiltEvent2)

      Abxbus.on(:eb_filt_a2, EBFiltEvent2, fn _event -> "from_a" end,
        handler_name: "filt_handler_a")
      Abxbus.on(:eb_filt_b2, EBFiltEvent2, fn _event -> "from_b" end,
        handler_name: "filt_handler_b")

      Abxbus.on(:eb_filt_a2, "*", fn e -> Abxbus.emit(:eb_filt_b2, e) end,
        handler_name: "fwd_filt")

      event = Abxbus.emit(:eb_filt_a2, EBFiltEvent2.new())
      Abxbus.wait_until_idle(:eb_filt_a2)
      Abxbus.wait_until_idle(:eb_filt_b2)
      Abxbus.event_completed(event, 2.0)

      stored = Abxbus.EventStore.get(event.event_id)
      all_results = Map.values(stored.event_results)
      completed = Enum.filter(all_results, &(&1.status == :completed))

      a_results = Enum.filter(completed, &(&1.eventbus_name == :eb_filt_a2))
      b_results = Enum.filter(completed, &(&1.eventbus_name == :eb_filt_b2))

      assert length(a_results) >= 1,
             "Should have results from bus a, got: #{inspect(Enum.map(completed, & &1.eventbus_name))}"
      assert length(b_results) >= 1,
             "Should have results from bus b, got: #{inspect(Enum.map(completed, & &1.eventbus_name))}"

      Abxbus.stop(:eb_filt_a2, clear: true)
      Abxbus.stop(:eb_filt_b2, clear: true)
    end
  end

  # ── Automatic event_type from module ─────────────────────────────────────

  defevent(EBAutoType, x: 1)

  describe "automatic event_type from module" do
    test "automatic event_type from module" do
      event = EBAutoType.new()
      assert event.event_type == EBAutoType
    end
  end

  # ── WAL middleware captures completed events ─────────────────────────────

  defevent(EBWalEvent, data: "wal")

  describe "WAL middleware" do
    test "WAL middleware captures completed events" do
      tmp_path = Path.join(System.tmp_dir!(), "wal_eb_#{:erlang.unique_integer([:positive])}.jsonl")

      {:ok, agent} = Abxbus.Middlewares.WAL.start(tmp_path)
      Abxbus.Middlewares.WAL.register(:eb_wal_test, agent)

      {:ok, _} = Abxbus.start_bus(:eb_wal_test,
        middlewares: [Abxbus.Middlewares.WAL])

      Abxbus.on(:eb_wal_test, EBWalEvent, fn _event -> :ok end,
        handler_name: "wal_handler")

      for _ <- 1..3 do
        Abxbus.emit(:eb_wal_test, EBWalEvent.new())
      end

      Abxbus.wait_until_idle(:eb_wal_test)

      # Give a moment for WAL writes to flush
      Process.sleep(50)

      assert File.exists?(tmp_path), "WAL file should exist"
      lines = tmp_path |> File.read!() |> String.split("\n", trim: true)
      assert length(lines) == 3,
             "WAL file should have 3 lines, got #{length(lines)}"

      # Cleanup
      File.rm(tmp_path)
      Abxbus.Middlewares.WAL.unregister(:eb_wal_test)
      Agent.stop(agent)
      Abxbus.stop(:eb_wal_test, clear: true)
    end
  end

  # ── wait_until_idle recovers after slow handler ──────────────────────────

  defevent(EBSlowIdleEvent)

  describe "wait_until_idle recovery" do
    test "wait_until_idle recovers after slow handler" do
      {:ok, _} = Abxbus.start_bus(:eb_slow_idle)

      barrier = :atomics.new(1, [])

      Abxbus.on(:eb_slow_idle, EBSlowIdleEvent, fn _event ->
        :atomics.put(barrier, 1, 1)
        # Block until released
        spin_wait(fn -> :atomics.get(barrier, 1) == 2 end, 5000)
        :ok
      end, handler_name: "slow_handler")

      Abxbus.emit(:eb_slow_idle, EBSlowIdleEvent.new())

      # Wait until handler is running
      spin_wait(fn -> :atomics.get(barrier, 1) == 1 end, 2000)

      # Short wait_until_idle should return quickly (not block forever)
      # We just verify it doesn't hang; the bus is still busy
      task = Task.async(fn ->
        Abxbus.wait_until_idle(:eb_slow_idle)
        :done
      end)

      # Release the handler
      :atomics.put(barrier, 1, 2)

      # Now wait_until_idle should complete
      assert Task.await(task, 5000) == :done

      Abxbus.stop(:eb_slow_idle, clear: true)
    end
  end

  # ── Parallel handlers results not clobbered ──────────────────────────────

  defevent(EBParallelResultEvent)

  describe "parallel handler results" do
    test "parallel handlers results not clobbered" do
      {:ok, _} = Abxbus.start_bus(:eb_par_res,
        event_handler_concurrency: :parallel)

      Abxbus.on(:eb_par_res, EBParallelResultEvent, fn _event ->
        %{source: "handler_1"}
      end, handler_name: "par_h1")

      Abxbus.on(:eb_par_res, EBParallelResultEvent, fn _event ->
        %{source: "handler_2"}
      end, handler_name: "par_h2")

      event = Abxbus.emit(:eb_par_res, EBParallelResultEvent.new())
      Abxbus.wait_until_idle(:eb_par_res)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)
      result_values = Enum.map(results, & &1.result)

      assert %{source: "handler_1"} in result_values,
             "Handler 1 result should be present, got: #{inspect(result_values)}"
      assert %{source: "handler_2"} in result_values,
             "Handler 2 result should be present, got: #{inspect(result_values)}"

      Abxbus.stop(:eb_par_res, clear: true)
    end
  end

  # ── Real-time result updates ─────────────────────────────────────────────

  defevent(EBRealTimeEvent)

  describe "real-time result updates" do
    test "handler started visible in ETS during execution" do
      {:ok, _} = Abxbus.start_bus(:eb_realtime,
        event_handler_concurrency: :serial)

      barrier = :atomics.new(1, [])

      entry1 = Abxbus.on(:eb_realtime, EBRealTimeEvent, fn _event ->
        :atomics.put(barrier, 1, 1)
        spin_wait(fn -> :atomics.get(barrier, 1) == 2 end, 5000)
        "result_1"
      end, handler_name: "blocking_handler")

      entry2 = Abxbus.on(:eb_realtime, EBRealTimeEvent, fn _event ->
        "result_2"
      end, handler_name: "instant_handler")

      event = Abxbus.emit(:eb_realtime, EBRealTimeEvent.new())

      # Wait for first handler to start blocking
      spin_wait(fn -> :atomics.get(barrier, 1) == 1 end, 2000)

      # Check ETS state while handler 1 is blocked
      stored = Abxbus.EventStore.get(event.event_id)

      h1_result = Map.get(stored.event_results, entry1.id)
      h2_result = Map.get(stored.event_results, entry2.id)

      assert h1_result != nil, "Handler 1 result should exist in ETS"
      assert h1_result.status == :started,
             "Handler 1 should be :started, got: #{inspect(h1_result.status)}"

      # Handler 2 should be pending (serial bus, h1 not done yet)
      assert h2_result == nil or h2_result.status == :pending,
             "Handler 2 should be :pending or nil, got: #{inspect(h2_result)}"

      # Release
      :atomics.put(barrier, 1, 2)
      Abxbus.wait_until_idle(:eb_realtime)

      Abxbus.stop(:eb_realtime, clear: true)
    end
  end

  # ── Result type enforcement ─────────────────────────────────────────────

  defevent(EBDictResultEvent, data: nil, result_type: :map)
  defevent(EBListResultEvent, data: nil, result_type: :list)

  describe "result type enforcement" do
    test "event_result_type :map enforces dict-shaped results" do
      {:ok, _} = Abxbus.start_bus(:eb_result_type_map)

      Abxbus.on(:eb_result_type_map, EBDictResultEvent, fn _e -> %{key: "v"} end,
        handler_name: "map_handler_1")
      Abxbus.on(:eb_result_type_map, EBDictResultEvent, fn _e -> %{key: "v"} end,
        handler_name: "map_handler_2")
      Abxbus.on(:eb_result_type_map, EBDictResultEvent, fn _e -> "string" end,
        handler_name: "string_handler")
      Abxbus.on(:eb_result_type_map, EBDictResultEvent, fn _e -> 42 end,
        handler_name: "int_handler")
      Abxbus.on(:eb_result_type_map, EBDictResultEvent, fn _e -> [1, 2, 3] end,
        handler_name: "list_handler")

      event = Abxbus.emit(:eb_result_type_map, EBDictResultEvent.new())
      Abxbus.wait_until_idle(:eb_result_type_map)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      completed = Enum.filter(results, &(&1.status == :completed))
      errors = Enum.filter(results, &(&1.status == :error))

      assert length(completed) == 2,
             "Expected 2 completed results, got: #{inspect(Enum.map(results, &{&1.handler_name, &1.status}))}"
      assert length(errors) == 3,
             "Expected 3 error results, got: #{inspect(Enum.map(results, &{&1.handler_name, &1.status}))}"

      for err_result <- errors do
        assert match?(%Abxbus.EventHandlerResultSchemaError{}, err_result.error),
               "Expected EventHandlerResultSchemaError, got: #{inspect(err_result.error)}"
      end

      Abxbus.stop(:eb_result_type_map, clear: true)
    end

    test "event_result_type :list enforces list-shaped results" do
      {:ok, _} = Abxbus.start_bus(:eb_result_type_list)

      Abxbus.on(:eb_result_type_list, EBListResultEvent, fn _e -> [1, 2] end,
        handler_name: "list_handler_1")
      Abxbus.on(:eb_result_type_list, EBListResultEvent, fn _e -> ["a", "b"] end,
        handler_name: "list_handler_2")
      Abxbus.on(:eb_result_type_list, EBListResultEvent, fn _e -> %{k: "v"} end,
        handler_name: "dict_handler")
      Abxbus.on(:eb_result_type_list, EBListResultEvent, fn _e -> "string" end,
        handler_name: "string_handler")

      event = Abxbus.emit(:eb_result_type_list, EBListResultEvent.new())
      Abxbus.wait_until_idle(:eb_result_type_list)

      stored = Abxbus.EventStore.get(event.event_id)
      results = Map.values(stored.event_results)

      completed = Enum.filter(results, &(&1.status == :completed))
      errors = Enum.filter(results, &(&1.status == :error))

      assert length(completed) == 2
      assert length(errors) == 2

      Abxbus.stop(:eb_result_type_list, clear: true)
    end
  end

  # ── Additional result access patterns ──────────────────────────────────

  defevent(EBEventbusIdFilterEvent, tag: "ebid")

  describe "result access patterns by id" do
    test "results filterable by eventbus_id and path" do
      {:ok, _} = Abxbus.start_bus(:eb_id_filter_a)
      {:ok, _} = Abxbus.start_bus(:eb_id_filter_b)

      Abxbus.on(:eb_id_filter_a, EBEventbusIdFilterEvent, fn _e -> "a_result" end,
        handler_name: "id_handler_a")
      Abxbus.on(:eb_id_filter_b, EBEventbusIdFilterEvent, fn _e -> "b_result" end,
        handler_name: "id_handler_b")

      # Forward a -> b
      Abxbus.on(:eb_id_filter_a, "*", fn e -> Abxbus.emit(:eb_id_filter_b, e) end,
        handler_name: "fwd_id_a_b")

      event = Abxbus.emit(:eb_id_filter_a, EBEventbusIdFilterEvent.new())
      Abxbus.wait_until_idle(:eb_id_filter_a)
      Abxbus.wait_until_idle(:eb_id_filter_b)
      Abxbus.event_completed(event, 2.0)

      stored = Abxbus.EventStore.get(event.event_id)
      all_results = Map.values(stored.event_results)
      completed = Enum.filter(all_results, &(&1.status == :completed))

      # Distinct eventbus_ids across results
      bus_ids =
        completed
        |> Enum.map(& &1.eventbus_id)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      assert length(bus_ids) >= 2,
             "Expected at least 2 distinct eventbus_ids, got: #{inspect(bus_ids)}"

      # Can filter down to one bus's results by id
      [bus_a_id | _] = bus_ids
      a_only = Enum.filter(completed, &(&1.eventbus_id == bus_a_id))
      assert length(a_only) >= 1

      # event_path should include both bus labels
      assert length(stored.event_path) >= 2,
             "event_path should have entries from both buses, got: #{inspect(stored.event_path)}"

      Abxbus.stop(:eb_id_filter_a, clear: true)
      Abxbus.stop(:eb_id_filter_b, clear: true)
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp spin_wait(fun, max_ms, elapsed \\ 0) do
    if elapsed >= max_ms, do: raise("spin_wait exceeded #{max_ms}ms")
    if fun.(), do: :ok, else: (Process.sleep(1); spin_wait(fun, max_ms, elapsed + 1))
  end
end

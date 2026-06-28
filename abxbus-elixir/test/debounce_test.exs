defmodule Abxbus.DebounceTest do
  @moduledoc """
  Tests for the debounce pattern using find-or-dispatch.

  Port of tests/test_eventbus_debounce.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(DebFreshEvent, value: "fresh")
  defevent(DebNoMatchEvent, value: "nomatch")
  defevent(DebStaleEvent, value: "stale")
  defevent(DebDebounceEvent, value: "debounce")
  defevent(DebDebouncePatternEvent, value: "debounce_pattern")
  defevent(DebRecentEvent, value: "recent")
  defevent(DebOrExistEvent, value: "or_exist")
  defevent(DebOrNewEvent, value: "or_new")
  defevent(DebPastOnlyEvent, value: "pastonly")
  defevent(DebPastFloatEvent, value: "pastfloat")
  defevent(DebFutureTimeoutEvent, value: "future")

  # ── Helpers ────────────────────────────────────────────────────────────────

  defp unique_bus(base) do
    :"#{base}_#{System.unique_integer([:positive])}"
  end

  # ── Tests ──────────────────────────────────────────────────────────────────

  describe "find past (debounce)" do
    test "find past returns existing fresh event without re-dispatch" do
      bus = unique_bus(:deb_fresh)
      {:ok, _} = Abxbus.start_bus(bus)

      Abxbus.on(bus, DebFreshEvent, fn _e -> :ok end, handler_name: "fresh_handler")

      event = Abxbus.emit(bus, DebFreshEvent.new())
      Abxbus.wait_until_idle(bus)

      # find with past: true should return the existing event
      found = Abxbus.find(DebFreshEvent, past: true)

      assert found != nil, "find(past: true) should return the existing event"
      assert found.event_id == event.event_id

      Abxbus.stop(bus, clear: true)
    end

    test "find past returns nil when no match, allowing dispatch" do
      bus = unique_bus(:deb_nomatch)
      {:ok, _} = Abxbus.start_bus(bus)

      Abxbus.on(bus, DebNoMatchEvent, fn _e -> :ok end, handler_name: "nomatch_handler")

      # No events emitted yet — find should return nil
      found = Abxbus.find(DebNoMatchEvent, past: true)
      assert found == nil, "find(past: true) should return nil when nothing emitted"

      # Now we can dispatch
      event = Abxbus.emit(bus, DebNoMatchEvent.new())
      Abxbus.wait_until_idle(bus)

      found_after = Abxbus.find(DebNoMatchEvent, past: true)
      assert found_after != nil
      assert found_after.event_id == event.event_id

      Abxbus.stop(bus, clear: true)
    end

    test "find past with time window ignores stale events" do
      bus = unique_bus(:deb_stale)
      {:ok, _} = Abxbus.start_bus(bus)

      Abxbus.on(bus, DebStaleEvent, fn _e -> :ok end, handler_name: "stale_handler")

      # Emit an event and let it complete
      _old_event = Abxbus.emit(bus, DebStaleEvent.new())
      Abxbus.wait_until_idle(bus)

      # Wait 200ms so the event becomes stale relative to a 100ms window
      Process.sleep(200)

      # Search with a tight time window — should miss the stale event
      found_stale = Abxbus.find(DebStaleEvent, past: 0.1)
      assert found_stale == nil,
             "find(past: 0.1) should return nil for event older than 100ms"

      # Emit a fresh event
      _new_event = Abxbus.emit(bus, DebStaleEvent.new())
      Abxbus.wait_until_idle(bus)

      # Search with a wider window — should find an event (new one is within window)
      found_new = Abxbus.find(DebStaleEvent, past: 0.5)
      assert found_new != nil, "find(past: 0.5) should return an event within the time window"

      Abxbus.stop(bus, clear: true)
    end

    test "debounce pattern: find or dispatch" do
      bus = unique_bus(:deb_pattern)
      {:ok, _} = Abxbus.start_bus(bus)

      emit_count = :counters.new(1, [:atomics])

      Abxbus.on(bus, DebDebouncePatternEvent, fn _e -> :ok end, handler_name: "debounce_handler")

      # Debounce helper: find existing or dispatch new
      debounce = fn ->
        case Abxbus.find(DebDebouncePatternEvent, past: 0.5) do
          nil ->
            :counters.add(emit_count, 1, 1)
            event = Abxbus.emit(bus, DebDebouncePatternEvent.new())
            Abxbus.wait_until_idle(bus)
            event

          existing ->
            existing
        end
      end

      # First call: no past event, dispatches new one
      first = debounce.()
      assert first != nil

      # Second call immediately: finds the recent event, no new dispatch
      second = debounce.()
      assert second != nil
      assert second.event_id == first.event_id

      assert :counters.get(emit_count, 1) == 1,
             "Only 1 event should have been emitted, got #{:counters.get(emit_count, 1)}"

      Abxbus.stop(bus, clear: true)
    end
  end

  describe "debounce or-chain patterns" do
    test "debounce prefers recent history over dispatch" do
      bus = unique_bus(:deb_recent)
      {:ok, _} = Abxbus.start_bus(bus)

      Abxbus.on(bus, DebRecentEvent, fn _e -> :ok end, handler_name: "recent_handler")

      event = Abxbus.emit(bus, DebRecentEvent.new())
      Abxbus.wait_until_idle(bus)

      # find with past: 1.0 should return the existing event (no new dispatch)
      found = Abxbus.find(DebRecentEvent, past: 1.0)
      assert found != nil, "find(past: 1.0) should return the existing event"
      assert found.event_id == event.event_id

      Abxbus.stop(bus, clear: true)
    end

    test "or chain without waiting finds existing" do
      bus = unique_bus(:deb_or_exist)
      {:ok, _} = Abxbus.start_bus(bus)

      emit_count = :counters.new(1, [:atomics])

      Abxbus.on(bus, DebOrExistEvent, fn _e -> :ok end, handler_name: "or_handler")

      event = Abxbus.emit(bus, DebOrExistEvent.new())
      Abxbus.wait_until_idle(bus)

      # Or-chain pattern: find(type, past: true) || emit(bus, type.new())
      start_t = System.monotonic_time(:millisecond)
      result = Abxbus.find(DebOrExistEvent, past: true) ||
        (fn ->
          :counters.add(emit_count, 1, 1)
          Abxbus.emit(bus, DebOrExistEvent.new())
        end).()

      elapsed = System.monotonic_time(:millisecond) - start_t
      assert result != nil
      assert elapsed < 200,
             "find(past: true) || emit should return quickly when found, took #{elapsed}ms"
      assert result.event_id == event.event_id
      assert :counters.get(emit_count, 1) == 0,
             "Should not have dispatched a new event, dispatched #{:counters.get(emit_count, 1)}"

      Abxbus.stop(bus, clear: true)
    end

    test "or chain dispatches when no match" do
      bus = unique_bus(:deb_or_new)
      {:ok, _} = Abxbus.start_bus(bus)

      emit_count = :counters.new(1, [:atomics])

      Abxbus.on(bus, DebOrNewEvent, fn _e -> :ok end, handler_name: "or_new_handler")

      # No prior events of this type — find should return nil
      result = Abxbus.find(DebOrNewEvent, past: true) ||
        (fn ->
          :counters.add(emit_count, 1, 1)
          Abxbus.emit(bus, DebOrNewEvent.new())
        end).()

      assert result != nil
      assert :counters.get(emit_count, 1) == 1,
             "Should have dispatched exactly 1 event"

      Abxbus.wait_until_idle(bus)

      Abxbus.stop(bus, clear: true)
    end
  end

  describe "find timing semantics" do
    test "find past only returns immediately without waiting" do
      bus = unique_bus(:deb_pastonly)
      {:ok, _} = Abxbus.start_bus(bus)

      start_t = System.monotonic_time(:millisecond)
      result = Abxbus.find(DebPastOnlyEvent, past: true, future: false)
      elapsed = System.monotonic_time(:millisecond) - start_t

      assert result == nil, "find(past: true, future: false) with no events should return nil"
      assert elapsed < 100,
             "find(past: true, future: false) should return immediately, took #{elapsed}ms"

      Abxbus.stop(bus, clear: true)
    end

    test "find past float returns immediately without waiting" do
      bus = unique_bus(:deb_pastfloat)
      {:ok, _} = Abxbus.start_bus(bus)

      start_t = System.monotonic_time(:millisecond)
      result = Abxbus.find(DebPastFloatEvent, past: 0.5)
      elapsed = System.monotonic_time(:millisecond) - start_t

      assert result == nil, "find(past: 0.5) with no events should return nil"
      assert elapsed < 100,
             "find(past: 0.5) should return immediately, took #{elapsed}ms"

      Abxbus.stop(bus, clear: true)
    end

    test "find future waits then returns nil on timeout" do
      bus = unique_bus(:deb_future)
      {:ok, _} = Abxbus.start_bus(bus)

      start_t = System.monotonic_time(:millisecond)
      result = Abxbus.find(DebFutureTimeoutEvent, past: false, future: 0.1)
      elapsed = System.monotonic_time(:millisecond) - start_t

      assert result == nil, "find(future: 0.1) with no events should return nil after timeout"
      assert elapsed >= 80,
             "find(future: 0.1) should wait ~100ms, only waited #{elapsed}ms"
      assert elapsed < 300,
             "find(future: 0.1) should not wait too long, waited #{elapsed}ms"

      Abxbus.stop(bus, clear: true)
    end
  end
end

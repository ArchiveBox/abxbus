defmodule Abxbus.EventHandlerTest do
  @moduledoc """
  Tests for handler completion modes (first/all), handler concurrency,
  and special result handling.

  Port of tests/test_event_handler.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(FirstModeEvent)
  defevent(AllModeEvent)
  defevent(SerialHandlerEvent)
  defevent(NilResultEvent)
  defevent(FalseResultEvent)
  defevent(IntCompletionEvent)
  defevent(ChildCompletionEvent)
  defevent(CompletionOverrideEvent)
  defevent(ConcurrencyCheckEvent)

  describe "first completion mode" do
    test "returns first non-nil result and cancels remaining" do
      {:ok, _} = Abxbus.start_bus(:first,
        event_handler_completion: :first,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:first, FirstModeEvent, fn _event ->
        Process.sleep(50)
        "slow"
      end, handler_name: "slow")

      Abxbus.on(:first, FirstModeEvent, fn _event ->
        Process.sleep(1)
        "fast"
      end, handler_name: "fast")

      event = Abxbus.emit(:first, FirstModeEvent.new())
      result = Abxbus.first(event)

      assert result == "fast"
    end

    test "nil results are skipped in first mode" do
      {:ok, _} = Abxbus.start_bus(:first_nil,
        event_handler_completion: :first,
        event_handler_concurrency: :serial
      )

      Abxbus.on(:first_nil, NilResultEvent, fn _event -> nil end,
        handler_name: "nil_handler")

      Abxbus.on(:first_nil, NilResultEvent, fn _event -> "actual_value" end,
        handler_name: "value_handler")

      event = Abxbus.emit(:first_nil, NilResultEvent.new())
      result = Abxbus.first(event)

      assert result == "actual_value"
    end

    test "false is a valid first result (not skipped)" do
      {:ok, _} = Abxbus.start_bus(:first_false,
        event_handler_completion: :first,
        event_handler_concurrency: :serial
      )

      Abxbus.on(:first_false, FalseResultEvent, fn _event -> false end,
        handler_name: "false_handler")

      Abxbus.on(:first_false, FalseResultEvent, fn _event -> "second" end,
        handler_name: "second_handler")

      event = Abxbus.emit(:first_false, FalseResultEvent.new())
      result = Abxbus.first(event)

      assert result == false
    end

    test "first() preserves falsy values like 0" do
      {:ok, _} = Abxbus.start_bus(:first_zero,
        event_handler_completion: :all,
        event_handler_concurrency: :serial
      )

      second_called = :atomics.new(1, [])

      Abxbus.on(:first_zero, IntCompletionEvent, fn _event -> 0 end,
        handler_name: "zero_handler")

      Abxbus.on(:first_zero, IntCompletionEvent, fn _event ->
        :atomics.put(second_called, 1, 1)
        99
      end, handler_name: "second_handler")

      event = Abxbus.emit(:first_zero, IntCompletionEvent.new())
      result = Abxbus.first(event)

      assert result == 0
    end

    test "first() skips event struct results and uses next winner" do
      {:ok, _} = Abxbus.start_bus(:first_struct_skip,
        event_handler_completion: :all,
        event_handler_concurrency: :serial
      )

      Abxbus.on(:first_struct_skip, FirstModeEvent, fn _event ->
        ChildCompletionEvent.new()
      end, handler_name: "struct_handler")

      Abxbus.on(:first_struct_skip, FirstModeEvent, fn _event ->
        "non_struct_value"
      end, handler_name: "value_handler")

      event = Abxbus.emit(:first_struct_skip, FirstModeEvent.new())
      result = Abxbus.first(event)

      # first() should skip the event struct result and return the string value
      assert result == "non_struct_value"
    end

    test "first() returns nil when all handlers fail" do
      {:ok, _} = Abxbus.start_bus(:first_all_fail,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:first_all_fail, FirstModeEvent, fn _event ->
        raise "boom1"
      end, handler_name: "fail_fast")

      Abxbus.on(:first_all_fail, FirstModeEvent, fn _event ->
        Process.sleep(10)
        raise "boom2"
      end, handler_name: "fail_slow")

      event = Abxbus.emit(:first_all_fail, FirstModeEvent.new())
      result = Abxbus.first(event)

      assert result == nil
    end
  end

  describe "completion mode overrides" do
    test "explicit override on event beats bus default" do
      {:ok, _} = Abxbus.start_bus(:completion_override,
        event_handler_concurrency: :serial,
        event_handler_completion: :first
      )

      second_called = :atomics.new(1, [])

      Abxbus.on(:completion_override, CompletionOverrideEvent, fn _event ->
        "first"
      end, handler_name: "first_handler")

      Abxbus.on(:completion_override, CompletionOverrideEvent, fn _event ->
        :atomics.put(second_called, 1, 1)
        "second"
      end, handler_name: "second_handler")

      event = Abxbus.emit(:completion_override,
        CompletionOverrideEvent.new(event_handler_completion: :all)
      )

      assert event.event_handler_completion == :all

      Abxbus.await(event)

      assert :atomics.get(second_called, 1) == 1,
             "Second handler should have been called when completion overridden to :all"
    end
  end

  describe "all completion mode" do
    test "waits for all handlers to complete" do
      {:ok, _} = Abxbus.start_bus(:all_mode,
        event_handler_completion: :all,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:all_mode, AllModeEvent, fn _event ->
        Process.sleep(20)
        "handler_1"
      end, handler_name: "h1")

      Abxbus.on(:all_mode, AllModeEvent, fn _event ->
        Process.sleep(10)
        "handler_2"
      end, handler_name: "h2")

      event = Abxbus.emit(:all_mode, AllModeEvent.new())
      completed = Abxbus.await(event)

      stored = Abxbus.EventStore.get(completed.event_id)
      results = Map.values(stored.event_results)

      assert length(results) == 2

      for r <- results do
        assert r.status == :completed
      end
    end
  end

  describe "serial handler concurrency" do
    test "handlers run one at a time" do
      {:ok, _} = Abxbus.start_bus(:serial_h,
        event_handler_concurrency: :serial
      )

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      handler = fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)
        Process.sleep(10)
        :counters.add(counter, 1, -1)
        :ok
      end

      Abxbus.on(:serial_h, SerialHandlerEvent, handler, handler_name: "h1")
      Abxbus.on(:serial_h, SerialHandlerEvent, handler, handler_name: "h2")

      Abxbus.emit(:serial_h, SerialHandlerEvent.new())
      Abxbus.wait_until_idle(:serial_h)

      assert :atomics.get(max_ref, 1) == 1, "Serial handlers should run one at a time"
    end
  end

  describe "handler concurrency defaults" do
    test "handler concurrency nil until processing (bus default remains unset on dispatch)" do
      {:ok, _} = Abxbus.start_bus(:conc_default,
        event_handler_concurrency: :parallel
      )

      Abxbus.on(:conc_default, ConcurrencyCheckEvent, fn _event -> "ok" end,
        handler_name: "one_handler")

      event = Abxbus.emit(:conc_default, ConcurrencyCheckEvent.new())
      assert event.event_handler_concurrency == nil

      Abxbus.await(event)
    end

    test "per-event concurrency override controls execution mode" do
      {:ok, _} = Abxbus.start_bus(:conc_per_event,
        event_handler_concurrency: :parallel
      )

      counter = :counters.new(1, [:atomics])
      max_serial = :atomics.new(1, [])
      max_parallel = :atomics.new(1, [])

      make_handler = fn max_ref ->
        fn _event ->
          :counters.add(counter, 1, 1)
          current = :counters.get(counter, 1)
          old = :atomics.get(max_ref, 1)
          if current > old, do: :atomics.put(max_ref, 1, current)
          Process.sleep(20)
          :counters.add(counter, 1, -1)
          :ok
        end
      end

      Abxbus.on(:conc_per_event, ConcurrencyCheckEvent, make_handler.(max_serial),
        handler_name: "h_a")
      Abxbus.on(:conc_per_event, ConcurrencyCheckEvent, make_handler.(max_parallel),
        handler_name: "h_b")

      # Emit with serial override
      serial_event = Abxbus.emit(:conc_per_event,
        ConcurrencyCheckEvent.new(event_handler_concurrency: :serial)
      )
      assert serial_event.event_handler_concurrency == :serial
      Abxbus.await(serial_event)

      assert :atomics.get(max_serial, 1) == 1,
             "Serial override should limit to 1 concurrent handler"

      # Reset counters for parallel test
      :counters.put(counter, 1, 0)

      parallel_event = Abxbus.emit(:conc_per_event,
        ConcurrencyCheckEvent.new(event_handler_concurrency: :parallel)
      )
      assert parallel_event.event_handler_concurrency == :parallel
      Abxbus.await(parallel_event)

      assert :atomics.get(max_parallel, 1) >= 2,
             "Parallel override should allow 2+ concurrent handlers"
    end
  end
end

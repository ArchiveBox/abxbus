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
end

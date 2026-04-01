defmodule Abxbus.DispatchDefaultsTest do
  @moduledoc """
  Tests for default resolution: event overrides beat bus defaults,
  defaults are nil until dispatch time.

  Port of tests/test_eventbus_dispatch_defaults.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(DefaultsEvent)
  defevent(ConcurrencyOverrideEvent, event_concurrency: :global_serial)
  defevent(HandlerOverrideEvent,
    event_handler_concurrency: :serial,
    event_handler_completion: :all
  )

  describe "event defaults stay nil until dispatch" do
    test "emitted event retains nil overrides" do
      {:ok, _} = Abxbus.start_bus(:dd1,
        event_concurrency: :parallel,
        event_handler_concurrency: :parallel,
        event_handler_completion: :all
      )

      Abxbus.on(:dd1, DefaultsEvent, fn _event -> :ok end)

      event = Abxbus.emit(:dd1, DefaultsEvent.new())

      # Event-level fields should be nil (bus defaults apply at processing time)
      assert event.event_concurrency == nil
      assert event.event_handler_concurrency == nil
      assert event.event_handler_completion == nil
    end
  end

  describe "class-level field overrides" do
    test "event class override takes precedence over bus default" do
      {:ok, _} = Abxbus.start_bus(:dd2,
        event_concurrency: :parallel
      )

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      Abxbus.on(:dd2, ConcurrencyOverrideEvent, fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)
        Process.sleep(10)
        :counters.add(counter, 1, -1)
        :ok
      end)

      Abxbus.emit(:dd2, ConcurrencyOverrideEvent.new())
      Abxbus.emit(:dd2, ConcurrencyOverrideEvent.new())

      Abxbus.wait_until_idle(:dd2)

      # global_serial override should force max 1
      assert :atomics.get(max_ref, 1) == 1
    end
  end

  describe "handler-level field overrides" do
    test "event handler_concurrency override forces serial on parallel bus" do
      {:ok, _} = Abxbus.start_bus(:dd3,
        event_handler_concurrency: :parallel
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

      # Two handlers on an event that overrides to serial
      Abxbus.on(:dd3, HandlerOverrideEvent, handler, handler_name: "h1")
      Abxbus.on(:dd3, HandlerOverrideEvent, handler, handler_name: "h2")

      Abxbus.emit(:dd3, HandlerOverrideEvent.new())
      Abxbus.wait_until_idle(:dd3)

      # Handler override to serial should force max 1 concurrent handler
      assert :atomics.get(max_ref, 1) == 1,
             "Handler concurrency override to serial should limit to 1, got #{:atomics.get(max_ref, 1)}"
    end
  end
end

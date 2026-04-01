defmodule AbxBus.EventBusLockingTest do
  @moduledoc """
  Tests for event concurrency modes: global-serial, bus-serial, parallel,
  handler concurrency, event-level overrides, and queue-jump.

  Port of tests/test_eventbus_locking.py.
  """

  use ExUnit.Case, async: false

  import AbxBus.TestEvents

  # ── Event definitions ──────────────────────────────────────────────────────

  defevent(GlobalSerialEvent, order: 0, source: nil)
  defevent(PerBusSerialEvent, order: 0, source: nil)
  defevent(ParallelEvent, order: 0)
  defevent(ParallelHandlerEvent)
  defevent(OverrideParallelEvent, event_concurrency: :parallel)
  defevent(OverrideSerialEvent, event_concurrency: :bus_serial)
  defevent(ParentEvent)
  defevent(ChildEvent)
  defevent(SiblingEvent)
  defevent(HandlerLockEvent, order: 0, source: nil)

  # ── Tests ──────────────────────────────────────────────────────────────────

  describe "global-serial mode" do
    test "only one event runs at a time across all buses" do
      {:ok, _} = AbxBus.start_bus(:gs_a, event_concurrency: :global_serial)
      {:ok, _} = AbxBus.start_bus(:gs_b, event_concurrency: :global_serial)

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])
      log = Agent.start_link(fn -> [] end) |> elem(1)

      handler = fn event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        # Track max concurrency
        old_max = :atomics.get(max_ref, 1)
        if current > old_max, do: :atomics.put(max_ref, 1, current)

        Agent.update(log, fn l -> l ++ ["#{event.source}:#{event.order}"] end)
        Process.sleep(5)

        :counters.add(counter, 1, -1)
        :ok
      end

      AbxBus.on(:gs_a, GlobalSerialEvent, handler)
      AbxBus.on(:gs_b, GlobalSerialEvent, handler)

      # Emit 3 events on each bus
      for i <- 0..2 do
        AbxBus.emit(:gs_a, GlobalSerialEvent.new(order: i, source: "a"))
        AbxBus.emit(:gs_b, GlobalSerialEvent.new(order: i, source: "b"))
      end

      AbxBus.wait_until_idle(:gs_a)
      AbxBus.wait_until_idle(:gs_b)

      max_in_flight = :atomics.get(max_ref, 1)
      assert max_in_flight == 1, "Global serial should allow only 1 event at a time, got #{max_in_flight}"
    end
  end

  describe "bus-serial mode" do
    test "events serialized per-bus but overlap across buses" do
      {:ok, _} = AbxBus.start_bus(:bs_a, event_concurrency: :bus_serial)
      {:ok, _} = AbxBus.start_bus(:bs_b, event_concurrency: :bus_serial)

      counter_a = :counters.new(1, [:atomics])
      counter_b = :counters.new(1, [:atomics])
      global_counter = :counters.new(1, [:atomics])
      max_a = :atomics.new(1, [])
      max_b = :atomics.new(1, [])
      max_global = :atomics.new(1, [])

      sync_event = :erlang.make_ref()

      make_handler = fn source_counter, source_max ->
        fn _event ->
          :counters.add(source_counter, 1, 1)
          :counters.add(global_counter, 1, 1)

          current = :counters.get(source_counter, 1)
          old_max = :atomics.get(source_max, 1)
          if current > old_max, do: :atomics.put(source_max, 1, current)

          g = :counters.get(global_counter, 1)
          old_g = :atomics.get(max_global, 1)
          if g > old_g, do: :atomics.put(max_global, 1, g)

          Process.sleep(10)

          :counters.add(source_counter, 1, -1)
          :counters.add(global_counter, 1, -1)
          :ok
        end
      end

      AbxBus.on(:bs_a, PerBusSerialEvent, make_handler.(counter_a, max_a))
      AbxBus.on(:bs_b, PerBusSerialEvent, make_handler.(counter_b, max_b))

      for i <- 0..2 do
        AbxBus.emit(:bs_a, PerBusSerialEvent.new(order: i, source: "a"))
        AbxBus.emit(:bs_b, PerBusSerialEvent.new(order: i, source: "b"))
      end

      AbxBus.wait_until_idle(:bs_a)
      AbxBus.wait_until_idle(:bs_b)

      assert :atomics.get(max_a, 1) == 1, "Bus A should process one event at a time"
      assert :atomics.get(max_b, 1) == 1, "Bus B should process one event at a time"
      assert :atomics.get(max_global, 1) >= 2, "Events from different buses should overlap"
    end
  end

  describe "parallel mode" do
    test "same bus can process multiple events concurrently" do
      {:ok, _} = AbxBus.start_bus(:par, event_concurrency: :parallel, event_handler_concurrency: :parallel)

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])
      release = :erlang.make_ref()

      AbxBus.on(:par, ParallelEvent, fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)

        Process.sleep(20)
        :counters.add(counter, 1, -1)
        :ok
      end)

      AbxBus.emit(:par, ParallelEvent.new(order: 0))
      AbxBus.emit(:par, ParallelEvent.new(order: 1))

      AbxBus.wait_until_idle(:par)

      assert :atomics.get(max_ref, 1) >= 2, "Parallel mode should run events concurrently"
    end
  end

  describe "handler concurrency parallel" do
    test "multiple handlers for same event run concurrently" do
      {:ok, _} = AbxBus.start_bus(:hcp,
        event_concurrency: :bus_serial,
        event_handler_concurrency: :parallel
      )

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      handler = fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)

        Process.sleep(20)
        :counters.add(counter, 1, -1)
        :ok
      end

      AbxBus.on(:hcp, ParallelHandlerEvent, handler)
      AbxBus.on(:hcp, ParallelHandlerEvent, handler)

      AbxBus.emit(:hcp, ParallelHandlerEvent.new())
      AbxBus.wait_until_idle(:hcp)

      assert :atomics.get(max_ref, 1) >= 2, "Parallel handler concurrency should run handlers concurrently"
    end
  end

  describe "event-level overrides" do
    test "event override to parallel beats bus-serial default" do
      {:ok, _} = AbxBus.start_bus(:ovp, event_concurrency: :bus_serial)

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      AbxBus.on(:ovp, OverrideParallelEvent, fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)
        Process.sleep(20)
        :counters.add(counter, 1, -1)
        :ok
      end)

      AbxBus.emit(:ovp, OverrideParallelEvent.new())
      AbxBus.emit(:ovp, OverrideParallelEvent.new())

      AbxBus.wait_until_idle(:ovp)

      assert :atomics.get(max_ref, 1) >= 2
    end

    test "event override to bus-serial beats parallel default" do
      {:ok, _} = AbxBus.start_bus(:ovs, event_concurrency: :parallel)

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      AbxBus.on(:ovs, OverrideSerialEvent, fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)
        Process.sleep(10)
        :counters.add(counter, 1, -1)
        :ok
      end)

      AbxBus.emit(:ovs, OverrideSerialEvent.new())
      AbxBus.emit(:ovs, OverrideSerialEvent.new())

      AbxBus.wait_until_idle(:ovs)

      assert :atomics.get(max_ref, 1) == 1
    end
  end

  describe "queue-jump" do
    test "awaited child preempts queued sibling on same bus" do
      {:ok, _} = AbxBus.start_bus(:qj, event_concurrency: :bus_serial)

      log = Agent.start_link(fn -> [] end) |> elem(1)

      AbxBus.on(:qj, ParentEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["parent_start"]))

        # Emit sibling (fire-and-forget — stays in queue)
        AbxBus.emit(:qj, SiblingEvent.new())

        # Emit and await child (queue-jump)
        child = AbxBus.emit(:qj, ChildEvent.new())
        AbxBus.await(child)

        Agent.update(log, &(&1 ++ ["parent_end"]))
        :ok
      end)

      AbxBus.on(:qj, ChildEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["child"]))
        :ok
      end)

      AbxBus.on(:qj, SiblingEvent, fn _event ->
        Agent.update(log, &(&1 ++ ["sibling"]))
        :ok
      end)

      event = AbxBus.emit(:qj, ParentEvent.new())
      AbxBus.wait_until_idle(:qj)

      order = Agent.get(log, & &1)
      assert order == ["parent_start", "child", "parent_end", "sibling"],
             "Expected queue-jump order, got: #{inspect(order)}"
    end
  end

  describe "global handler lock via semaphore" do
    test "handler semaphore limits concurrency across buses" do
      {:ok, _} = AbxBus.start_bus(:sem_a, event_concurrency: :parallel)
      {:ok, _} = AbxBus.start_bus(:sem_b, event_concurrency: :parallel)

      counter = :counters.new(1, [:atomics])
      max_ref = :atomics.new(1, [])

      handler_opts = [
        semaphore_scope: :global,
        semaphore_name: "test_lock",
        semaphore_limit: 1
      ]

      handler = fn _event ->
        :counters.add(counter, 1, 1)
        current = :counters.get(counter, 1)
        old = :atomics.get(max_ref, 1)
        if current > old, do: :atomics.put(max_ref, 1, current)
        Process.sleep(5)
        :counters.add(counter, 1, -1)
        :ok
      end

      AbxBus.on(:sem_a, HandlerLockEvent, handler, handler_opts)
      AbxBus.on(:sem_b, HandlerLockEvent, handler, handler_opts)

      for _ <- 1..4 do
        AbxBus.emit(:sem_a, HandlerLockEvent.new(source: "a"))
        AbxBus.emit(:sem_b, HandlerLockEvent.new(source: "b"))
      end

      AbxBus.wait_until_idle(:sem_a)
      AbxBus.wait_until_idle(:sem_b)

      assert :atomics.get(max_ref, 1) == 1, "Semaphore should limit concurrency to 1"
    end
  end
end

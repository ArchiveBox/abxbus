defmodule Abxbus.PinpointTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(PinEvent, payload: nil)

  defp measure(label, n, fun) do
    fun.()
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 55)} #{Float.round(us_per, 1)} µs/op")
  end

  test "pinpoint overhead" do
    n = 10_000
    events = for _ <- 1..n, do: PinEvent.new(payload: "x")

    IO.puts("\n=== Pinpoint (#{n} iterations) ===\n")

    # 1. Direct GenServer.call bypassing Abxbus.emit wrapper
    {:ok, _} = Abxbus.start_bus(:pin_a, event_concurrency: :bus_serial)
    measure("BusServer.emit direct (0 handlers)", n, fn ->
      for e <- events do
        Abxbus.BusServer.emit(:pin_a, e)
      end
      Abxbus.wait_until_idle(:pin_a)
    end)

    # 2. Full Abxbus.emit (includes maybe_set_parent/track_child)
    {:ok, _} = Abxbus.start_bus(:pin_b, event_concurrency: :bus_serial)
    measure("Abxbus.emit full wrapper (0 handlers)", n, fn ->
      for e <- events do
        Abxbus.emit(:pin_b, e)
      end
      Abxbus.wait_until_idle(:pin_b)
    end)

    # 3. Check if GC is the issue: run with large heap to avoid GC
    {:ok, _} = Abxbus.start_bus(:pin_c, event_concurrency: :bus_serial)
    Process.flag(:min_heap_size, 1_000_000)
    measure("Abxbus.emit (0 handlers, large heap caller)", n, fn ->
      for e <- events do
        Abxbus.emit(:pin_c, e)
      end
      Abxbus.wait_until_idle(:pin_c)
    end)

    # 4. Measure the cost of maybe_set_parent + maybe_track_child alone
    measure("maybe_set_parent + maybe_track_child", n, fn ->
      for e <- events do
        # These are the wrapper operations
        Process.get(:abxbus_current_event_id)
        Process.get(:abxbus_current_bus)
      end
    end)

    # 5. Measure list append (event_path ++ [label])
    measure("list append ([] ++ [label])", n, fn ->
      for _ <- 1..n do
        [] ++ ["test#abc"]
      end
    end)

    # 6. Measure :queue operations
    measure(":queue.in + :queue.peek + :queue.out", n, fn ->
      q = :queue.new()
      for _ <- 1..n do
        q = :queue.in(:item, q)
        {:value, _} = :queue.peek(q)
        {{:value, _}, q} = :queue.out(q)
      end
    end)

    # 7. How much time does maybe_mark_tree_complete take?
    for e <- events, do: Abxbus.EventStore.put(%{e | event_status: :completed, event_pending_bus_count: 0})
    measure("maybe_mark_tree_complete (0 children, 0 waiters)", n, fn ->
      for e <- events do
        Abxbus.EventStore.children_of(e.event_id)
        Abxbus.EventStore.notify_waiters(e.event_id, e)
      end
    end)

    # 8. Just ETS: put_or_merge + index + put(completed)
    measure("3 ETS ops: insert_new + insert + insert", n, fn ->
      for e <- events do
        :ets.insert(:abxbus_events, {e.event_id, e})
        :ets.insert(:abxbus_bus_events, {:pin_ets, e.event_id})
        :ets.insert(:abxbus_events, {e.event_id, %{e | event_status: :completed}})
      end
    end)

    IO.puts("")
  end
end

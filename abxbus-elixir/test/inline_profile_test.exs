defmodule Abxbus.InlineProfileTest do
  @moduledoc """
  Ultra-targeted profiling of the inline emit path.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(ProfileEvent, payload: nil)

  defp measure(label, n, fun) do
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 50)} #{Float.round(us_per, 1)} µs/op")
    us_per
  end

  @tag timeout: 30_000
  test "profile components of inline serial emit" do
    n = 10_000

    IO.puts("\n=== Inline Emit Profiling (#{n} iterations) ===\n")

    # Pre-create events outside the measurement
    events = for _ <- 1..n, do: ProfileEvent.new(payload: "x")

    # 1. Just GenServer.call with enqueue only (no processing)
    {:ok, _} = Abxbus.start_bus(:prof_a, event_concurrency: :parallel)
    measure("GenServer.call + enqueue (parallel, no handler)", n, fn ->
      for event <- events do
        Abxbus.emit(:prof_a, event)
      end
      Abxbus.wait_until_idle(:prof_a)
    end)

    # 2. Serial bus with 0 handlers (inline path)
    {:ok, _} = Abxbus.start_bus(:prof_b, event_concurrency: :bus_serial)
    measure("Serial inline, 0 handlers", n, fn ->
      for event <- events do
        Abxbus.emit(:prof_b, event)
      end
      Abxbus.wait_until_idle(:prof_b)
    end)

    # 3. Serial bus with 1 trivial handler
    {:ok, _} = Abxbus.start_bus(:prof_c, event_concurrency: :bus_serial)
    counter = :counters.new(1, [:atomics])
    Abxbus.on(:prof_c, ProfileEvent, fn _e -> :counters.add(counter, 1, 1); :ok end,
      handler_name: "counter")
    measure("Serial inline, 1 handler", n, fn ->
      for event <- events do
        Abxbus.emit(:prof_c, event)
      end
      Abxbus.wait_until_idle(:prof_c)
    end)
    assert :counters.get(counter, 1) == n

    # 4. Just Event.new cost (for reference)
    measure("Event.new only", n, fn ->
      for _ <- 1..n, do: ProfileEvent.new(payload: "x")
    end)

    # 5. Just Abxbus.emit wrapper (maybe_set_parent + BusServer.emit + maybe_track_child)
    # Compare with raw GenServer.call
    measure("Raw GenServer.call to serial bus (no processing)", n, fn ->
      {:ok, _} = Abxbus.start_bus(:prof_d, event_concurrency: :bus_serial)
      for _ <- 1..n do
        GenServer.call(:prof_d, :queue_size)
      end
    end)

    # 6. maybe_mark_tree_complete cost in isolation
    events2 = for _ <- 1..n do
      e = ProfileEvent.new(payload: "x")
      Abxbus.EventStore.put(e)
      e
    end
    measure("maybe_mark_tree_complete (no parent, no children)", n, fn ->
      for event <- events2 do
        # Simulate what maybe_mark_tree_complete does
        children_ids = Abxbus.EventStore.children_of(event.event_id)
        all_children_complete =
          Enum.all?(children_ids, fn child_id ->
            case Abxbus.EventStore.get(child_id) do
              nil -> true
              child -> child.event_status in [:completed, :error]
            end
          end)
        if all_children_complete and event.event_pending_bus_count <= 0 do
          Abxbus.EventStore.update(event.event_id, %{event_status: :completed})
          updated = Abxbus.EventStore.get(event.event_id)
          Abxbus.EventStore.notify_waiters(event.event_id, updated)
        end
      end
    end)

    # 7. ETS operations sum
    measure("ETS: put_or_merge + index + update + get + put + get", n, fn ->
      for _ <- 1..n do
        e = ProfileEvent.new(payload: "x")
        Abxbus.EventStore.put_or_merge(e)
        Abxbus.EventStore.index_to_bus(:prof_ets, e.event_id)
        Abxbus.EventStore.update(e.event_id, %{event_status: :started})
        Abxbus.EventStore.get(e.event_id)
        Abxbus.EventStore.put(Map.put(e, :event_status, :completed))
        Abxbus.EventStore.get(e.event_id)
      end
    end)

    # 8. History list: prepend + maybe_trim for list of 1000+
    list = Enum.to_list(1..1000)
    measure("List prepend + Enum.take(1001, 1000)", n, fn ->
      for _ <- 1..n do
        l = ["new" | list]
        _trimmed = Enum.take(l, 1000)
      end
    end)

    IO.puts("\n=== End Profiling ===\n")
  end
end

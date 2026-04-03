defmodule Abxbus.EtsTableSizeTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(SizeEvent, payload: nil)

  defp measure(label, n, fun) do
    fun.()
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 55)} #{Float.round(us_per, 1)} µs/op")
  end

  test "ETS performance vs table size" do
    n = 10_000

    IO.puts("\n=== ETS Performance vs Table Size ===\n")

    events = for _ <- 1..n, do: SizeEvent.new(payload: "x")

    # Test with fresh table (0 existing entries)
    tab = :ets.new(:fresh_tab, [:set, :public, write_concurrency: true])
    measure("Fresh table: 3 inserts per event", n, fn ->
      for e <- events do
        :ets.insert(tab, {e.event_id, e})
        :ets.insert(tab, {"idx_" <> e.event_id, :ref})
        :ets.insert(tab, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    IO.puts("    Table size after: #{:ets.info(tab, :size)}")
    :ets.delete(tab)

    # Test with pre-loaded table (50k entries)
    tab2 = :ets.new(:preloaded_tab, [:set, :public, write_concurrency: true])
    filler = for i <- 1..50_000 do
      e = SizeEvent.new(payload: "filler_#{i}")
      :ets.insert(tab2, {e.event_id, e})
    end
    IO.puts("    Pre-loaded 50k entries")
    measure("50k entries: 3 inserts per event", n, fn ->
      for e <- events do
        :ets.insert(tab2, {e.event_id, e})
        :ets.insert(tab2, {"idx_" <> e.event_id, :ref})
        :ets.insert(tab2, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    IO.puts("    Table size after: #{:ets.info(tab2, :size)}")
    :ets.delete(tab2)

    # Test with shared :abxbus_events table
    existing = :ets.info(:abxbus_events, :size)
    IO.puts("    Shared table has #{existing} entries")
    measure("Shared :abxbus_events: 3 inserts per event", n, fn ->
      for e <- events do
        :ets.insert(:abxbus_events, {e.event_id, e})
        :ets.insert(:abxbus_bus_events, {:test_size, e.event_id})
        :ets.insert(:abxbus_events, {e.event_id, %{e | event_status: :completed}})
      end
    end)

    # Test with integer keys instead of string UUIDs
    tab3 = :ets.new(:int_key_tab, [:set, :public, write_concurrency: true])
    m25 = Map.new(1..25, fn i -> {:"f#{i}", i} end)
    measure("Fresh table: 3 inserts (integer keys, 25-field map)", n, fn ->
      for i <- 1..n do
        :ets.insert(tab3, {i, m25})
        :ets.insert(tab3, {i + n, :ref})
        :ets.insert(tab3, {i, Map.put(m25, :f1, :updated)})
      end
    end)
    :ets.delete(tab3)

    # Test GC impact on GenServer
    {:ok, _} = Abxbus.start_bus(:gc_test, event_concurrency: :bus_serial)
    # Set GenServer to have large heap
    gc_bus_pid = GenServer.whereis(:gc_test)
    :erlang.process_flag(gc_bus_pid, :min_heap_size, 1_000_000)

    measure("Full Abxbus (0 handlers, large GenServer heap)", n, fn ->
      for e <- events, do: Abxbus.emit(:gc_test, e)
      Abxbus.wait_until_idle(:gc_test)
    end)

    IO.puts("")
  end
end

defmodule Abxbus.EtsStructTest do
  use ExUnit.Case, async: false

  defp measure(label, n, fun) do
    # Warmup
    fun.()
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 55)} #{Float.round(us_per, 2)} µs/op")
  end

  test "ETS insert/lookup cost vs struct size" do
    n = 50_000

    IO.puts("\n=== ETS Performance vs Struct Size (#{n} iterations) ===\n")

    # Small struct (1 field)
    tab1 = :ets.new(:t1, [:set, :public, write_concurrency: true])
    measure("ETS insert: 1-field map", n, fn ->
      for i <- 1..n, do: :ets.insert(tab1, {i, %{a: 1}})
    end)
    :ets.delete(tab1)

    tab2 = :ets.new(:t2, [:set, :public, write_concurrency: true])
    measure("ETS insert: 5-field map", n, fn ->
      for i <- 1..n, do: :ets.insert(tab2, {i, %{a: 1, b: 2, c: 3, d: 4, e: 5}})
    end)
    :ets.delete(tab2)

    tab3 = :ets.new(:t3, [:set, :public, write_concurrency: true])
    m25 = Map.new(1..25, fn i -> {:"f#{i}", i} end)
    measure("ETS insert: 25-field map", n, fn ->
      for i <- 1..n, do: :ets.insert(tab3, {i, m25})
    end)
    :ets.delete(tab3)

    tab4 = :ets.new(:t4, [:set, :public, write_concurrency: true])
    m25_with_list = Map.put(m25, :list_field, Enum.to_list(1..10))
    measure("ETS insert: 25-field map + list", n, fn ->
      for i <- 1..n, do: :ets.insert(tab4, {i, m25_with_list})
    end)
    :ets.delete(tab4)

    # Now test insert + lookup
    tab5 = :ets.new(:t5, [:set, :public, write_concurrency: true, read_concurrency: true])
    for i <- 1..n, do: :ets.insert(tab5, {i, m25})
    measure("ETS lookup: 25-field map (#{n} entries)", n, fn ->
      for i <- 1..n do
        :ets.lookup(tab5, i)
      end
    end)
    :ets.delete(tab5)

    # insert_new on fresh table
    tab6 = :ets.new(:t6, [:set, :public, write_concurrency: true])
    measure("ETS insert_new: 25-field map (unique keys)", n, fn ->
      for i <- 1..n, do: :ets.insert_new(tab6, {i, m25})
    end)
    :ets.delete(tab6)

    # Struct copy overhead
    measure("Map.put on 25-field map", n, fn ->
      for _ <- 1..n, do: Map.put(m25, :f1, :updated)
    end)

    measure("Map.merge(25-field, 3 updates)", n, fn ->
      updates = %{f1: :a, f2: :b, f3: :c}
      for _ <- 1..n, do: Map.merge(m25, updates)
    end)

    # Full event struct
    import Abxbus.TestEvents
    defevent(SizeTestEvent, payload: nil)

    events = for _ <- 1..n, do: SizeTestEvent.new(payload: "x")

    tab7 = :ets.new(:t7, [:set, :public, write_concurrency: true])
    measure("ETS insert: full event struct", n, fn ->
      for {e, i} <- Enum.with_index(events) do
        :ets.insert(tab7, {i, e})
      end
    end)
    :ets.delete(tab7)

    tab8 = :ets.new(:t8, [:set, :public, write_concurrency: true])
    measure("ETS insert: full event (by event_id key)", n, fn ->
      for e <- events do
        :ets.insert(tab8, {e.event_id, e})
      end
    end)
    :ets.delete(tab8)

    # UUID generation
    measure("UUID generation (Event.generate_id)", n, fn ->
      for _ <- 1..n, do: Abxbus.Event.generate_id()
    end)

    IO.puts("")
  end
end

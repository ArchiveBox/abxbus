defmodule Abxbus.EtsConcurrencyTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(ConcEvent, payload: nil)

  defp measure(label, n, fun) do
    fun.()
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 60)} #{Float.round(us_per, 1)} µs/op")
  end

  test "ETS concurrency options impact" do
    n = 10_000
    events = for _ <- 1..n, do: ConcEvent.new(payload: "x")

    IO.puts("\n=== ETS Concurrency Options Comparison (#{n} iterations) ===\n")

    # 1. No concurrency
    t1 = :ets.new(:t1, [:set, :public])
    measure("No concurrency options", n, fn ->
      for e <- events do
        :ets.insert(t1, {e.event_id, e})
        :ets.insert(t1, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    :ets.delete(t1)

    # 2. write_concurrency only
    t2 = :ets.new(:t2, [:set, :public, write_concurrency: true])
    measure("write_concurrency: true", n, fn ->
      for e <- events do
        :ets.insert(t2, {e.event_id, e})
        :ets.insert(t2, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    :ets.delete(t2)

    # 3. read_concurrency only
    t3 = :ets.new(:t3, [:set, :public, read_concurrency: true])
    measure("read_concurrency: true", n, fn ->
      for e <- events do
        :ets.insert(t3, {e.event_id, e})
        :ets.insert(t3, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    :ets.delete(t3)

    # 4. Both (current EventStore config)
    t4 = :ets.new(:t4, [:set, :public, read_concurrency: true, write_concurrency: true])
    measure("read_concurrency + write_concurrency (CURRENT)", n, fn ->
      for e <- events do
        :ets.insert(t4, {e.event_id, e})
        :ets.insert(t4, {e.event_id, %{e | event_status: :completed}})
      end
    end)
    :ets.delete(t4)

    # 5. write_concurrency: auto (OTP 25+)
    try do
      t5 = :ets.new(:t5, [:set, :public, write_concurrency: :auto])
      measure("write_concurrency: :auto", n, fn ->
        for e <- events do
          :ets.insert(t5, {e.event_id, e})
          :ets.insert(t5, {e.event_id, %{e | event_status: :completed}})
        end
      end)
      :ets.delete(t5)
    rescue
      _ -> IO.puts("  write_concurrency: :auto not supported")
    end

    IO.puts("")
  end
end

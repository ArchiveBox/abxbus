defmodule Abxbus.BottleneckTest do
  @moduledoc """
  Targeted micro-benchmarks to identify where time is spent in the serial emit path.
  Run with: mix test test/bottleneck_test.exs --timeout 60000
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(BenchEvent, payload: nil)

  defp measure(label, n, fun) do
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    elapsed_us = (t1 - t0) / 1000
    us_per = elapsed_us / n
    IO.puts("  #{String.pad_trailing(label, 45)} #{Float.round(us_per, 1)} µs/op  (#{Float.round(elapsed_us / 1000, 1)}ms total)")
    us_per
  end

  @tag timeout: 60_000
  test "bottleneck breakdown for serial emit path" do
    n = 10_000

    IO.puts("\n=== Bottleneck Breakdown (#{n} iterations) ===\n")

    # 1. UUID generation cost
    measure("1. UUID generation (Event.generate_id)", n, fn ->
      for _ <- 1..n, do: Abxbus.Event.generate_id()
    end)

    # 2. Event.new struct creation
    measure("2. Event.new (struct + UUID + timestamp)", n, fn ->
      for _ <- 1..n, do: BenchEvent.new(payload: "x")
    end)

    # 3. Raw ETS insert
    table = :ets.new(:bench_ets, [:set, :public, write_concurrency: true])
    measure("3. Raw ETS insert (set table)", n, fn ->
      for i <- 1..n do
        :ets.insert(table, {i, %{data: "x"}})
      end
    end)
    :ets.delete(table)

    # 4. ETS insert_new (fast path of put_or_merge)
    measure("4. EventStore.put (direct ETS)", n, fn ->
      for _ <- 1..n do
        event = BenchEvent.new(payload: "x")
        Abxbus.EventStore.put(event)
      end
    end)

    # 5. GenServer.call overhead (noop call)
    {:ok, noop} = GenServer.start_link(NoopServer, [])
    measure("5. GenServer.call roundtrip (noop)", n, fn ->
      for _ <- 1..n, do: GenServer.call(noop, :noop)
    end)
    GenServer.stop(noop)

    # 6. GenServer.cast overhead (noop cast)
    {:ok, noop2} = GenServer.start_link(NoopServer, [])
    measure("6. GenServer.cast (noop)", n, fn ->
      for _ <- 1..n, do: GenServer.cast(noop2, :noop)
      # Drain mailbox to ensure all casts processed
      GenServer.call(noop2, :noop)
    end)
    GenServer.stop(noop2)

    # 7. spawn + monitor + receive DOWN
    measure("7. spawn + monitor + receive DOWN", n, fn ->
      for _ <- 1..n do
        pid = spawn(fn -> :ok end)
        ref = Process.monitor(pid)
        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        end
      end
    end)

    # 8. spawn_link (no monitor, no receive)
    measure("8. spawn (fire-and-forget)", n, fn ->
      for _ <- 1..n do
        spawn(fn -> :ok end)
      end
      Process.sleep(100)
    end)

    # 9. Full handler execution inline (no process spawn)
    measure("9. Inline handler call (fn -> :ok)", n, fn ->
      handler = fn _event -> :ok end
      event = BenchEvent.new(payload: "x")
      for _ <- 1..n, do: handler.(event)
    end)

    # 10. Full emit path through bus (the actual bottleneck)
    {:ok, _} = Abxbus.start_bus(:bench_serial, event_concurrency: :bus_serial)
    counter = :counters.new(1, [:atomics])
    Abxbus.on(:bench_serial, BenchEvent, fn _event ->
      :counters.add(counter, 1, 1)
      :ok
    end, handler_name: "bench_handler")

    measure("10. Full Abxbus.emit (serial, 1 handler)", n, fn ->
      for _ <- 1..n do
        Abxbus.emit(:bench_serial, BenchEvent.new(payload: "x"))
      end
      Abxbus.wait_until_idle(:bench_serial)
    end)

    assert :counters.get(counter, 1) == n

    # 11. resolve_find_waiters cost (should be near-zero with no waiters)
    measure("11. EventStore.resolve_find_waiters (no waiters)", n, fn ->
      event = BenchEvent.new(payload: "x")
      for _ <- 1..n, do: Abxbus.EventStore.resolve_find_waiters(event)
    end)

    # 12. put_or_merge (insert_new fast path)
    measure("12. EventStore.put_or_merge (new events)", n, fn ->
      for _ <- 1..n do
        event = BenchEvent.new(payload: "x")
        Abxbus.EventStore.put_or_merge(event)
      end
    end)

    # 13. GenServer.call {:emit, event} only (no processing)
    {:ok, _} = Abxbus.start_bus(:bench_nohandler, event_concurrency: :bus_serial)
    measure("13. Full emit path (serial, 0 handlers)", n, fn ->
      for _ <- 1..n do
        Abxbus.emit(:bench_nohandler, BenchEvent.new(payload: "x"))
      end
      Abxbus.wait_until_idle(:bench_nohandler)
    end)

    IO.puts("\n=== End Bottleneck Breakdown ===\n")
  end
end

defmodule NoopServer do
  use GenServer

  def init(_), do: {:ok, %{}}
  def handle_call(:noop, _from, state), do: {:reply, :ok, state}
  def handle_cast(:noop, state), do: {:noreply, state}
end

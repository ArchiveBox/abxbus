defmodule Abxbus.PerformanceTest do
  @moduledoc """
  Performance benchmarks matching the Python/TypeScript test suites.

  Measures events/sec, ms/event, and KB/event across different configurations.
  Run with: mix test test/performance_test.exs --timeout 120000
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(PerfEvent, payload: nil)
  defevent(PerfParallelEvent, payload: nil)

  @tag timeout: 120_000
  test "1 bus × 50k events × 1 handler" do
    {:ok, _} = Abxbus.start_bus(:perf1, event_concurrency: :bus_serial)

    counter = :counters.new(1, [:atomics])
    Abxbus.on(:perf1, PerfEvent, fn _event ->
      :counters.add(counter, 1, 1)
      :ok
    end, handler_name: "perf_handler")

    n = 50_000
    mem_before = :erlang.memory(:total)
    t0 = System.monotonic_time(:microsecond)

    for _ <- 1..n do
      Abxbus.emit(:perf1, PerfEvent.new(payload: "x"))
    end

    Abxbus.wait_until_idle(:perf1)
    t1 = System.monotonic_time(:microsecond)
    mem_after = :erlang.memory(:total)

    elapsed_ms = (t1 - t0) / 1000
    ms_per_event = elapsed_ms / n
    events_per_sec = n / (elapsed_ms / 1000)
    kb_per_event = (mem_after - mem_before) / 1024 / n

    IO.puts("")
    IO.puts("=== 1 bus × #{n} events × 1 handler ===")
    IO.puts("  Total:         #{Float.round(elapsed_ms, 1)}ms")
    IO.puts("  Per event:     #{Float.round(ms_per_event, 3)}ms")
    IO.puts("  Events/sec:    #{trunc(events_per_sec)}")
    IO.puts("  KB/event:      #{Float.round(kb_per_event, 3)}")

    assert :counters.get(counter, 1) == n
    assert ms_per_event < 10.0, "Performance regression: #{ms_per_event}ms/event"
  end

  @tag timeout: 120_000
  test "500 buses × 100 events × 1 handler" do
    n_buses = 500
    n_events = 100
    total = n_buses * n_events

    counter = :counters.new(1, [:atomics])

    for i <- 1..n_buses do
      bus = :"perf_mb_#{i}"
      {:ok, _} = Abxbus.start_bus(bus, event_concurrency: :bus_serial)
      Abxbus.on(bus, PerfEvent, fn _event ->
        :counters.add(counter, 1, 1)
        :ok
      end, handler_name: "handler_#{i}")
    end

    mem_before = :erlang.memory(:total)
    t0 = System.monotonic_time(:microsecond)

    for i <- 1..n_buses do
      bus = :"perf_mb_#{i}"
      for _ <- 1..n_events do
        Abxbus.emit(bus, PerfEvent.new(payload: "x"))
      end
    end

    for i <- 1..n_buses do
      Abxbus.wait_until_idle(:"perf_mb_#{i}")
    end

    t1 = System.monotonic_time(:microsecond)
    mem_after = :erlang.memory(:total)

    elapsed_ms = (t1 - t0) / 1000
    ms_per_event = elapsed_ms / total
    events_per_sec = total / (elapsed_ms / 1000)
    kb_per_event = (mem_after - mem_before) / 1024 / total

    IO.puts("")
    IO.puts("=== #{n_buses} buses × #{n_events} events × 1 handler ===")
    IO.puts("  Total:         #{Float.round(elapsed_ms, 1)}ms")
    IO.puts("  Per event:     #{Float.round(ms_per_event, 3)}ms")
    IO.puts("  Events/sec:    #{trunc(events_per_sec)}")
    IO.puts("  KB/event:      #{Float.round(kb_per_event, 3)}")

    assert :counters.get(counter, 1) == total
  end

  @tag timeout: 120_000
  test "1 bus × 1000 events × 50 parallel handlers" do
    {:ok, _} = Abxbus.start_bus(:perf_ph,
      event_concurrency: :bus_serial,
      event_handler_concurrency: :parallel
    )

    n_events = 1000
    n_handlers = 50
    total_handler_calls = n_events * n_handlers

    counter = :counters.new(1, [:atomics])

    for i <- 1..n_handlers do
      Abxbus.on(:perf_ph, PerfParallelEvent, fn _event ->
        :counters.add(counter, 1, 1)
        :ok
      end, handler_name: "ph_#{i}")
    end

    mem_before = :erlang.memory(:total)
    t0 = System.monotonic_time(:microsecond)

    for _ <- 1..n_events do
      Abxbus.emit(:perf_ph, PerfParallelEvent.new(payload: "x"))
    end

    Abxbus.wait_until_idle(:perf_ph)
    t1 = System.monotonic_time(:microsecond)
    mem_after = :erlang.memory(:total)

    elapsed_ms = (t1 - t0) / 1000
    ms_per_handler = elapsed_ms / total_handler_calls
    handlers_per_sec = total_handler_calls / (elapsed_ms / 1000)
    kb_per_handler = (mem_after - mem_before) / 1024 / total_handler_calls

    IO.puts("")
    IO.puts("=== 1 bus × #{n_events} events × #{n_handlers} parallel handlers ===")
    IO.puts("  Total:         #{Float.round(elapsed_ms, 1)}ms")
    IO.puts("  Per handler:   #{Float.round(ms_per_handler, 3)}ms")
    IO.puts("  Handlers/sec:  #{trunc(handlers_per_sec)}")
    IO.puts("  KB/handler:    #{Float.round(kb_per_handler, 3)}")

    assert :counters.get(counter, 1) == total_handler_calls
  end

  @tag timeout: 120_000
  test "1 bus × 10k parallel events × 1 handler" do
    {:ok, _} = Abxbus.start_bus(:perf_par, event_concurrency: :parallel)

    n = 10_000
    counter = :counters.new(1, [:atomics])

    Abxbus.on(:perf_par, PerfEvent, fn _event ->
      :counters.add(counter, 1, 1)
      :ok
    end, handler_name: "par_handler")

    t0 = System.monotonic_time(:microsecond)

    for _ <- 1..n do
      Abxbus.emit(:perf_par, PerfEvent.new(payload: "x"))
    end

    Abxbus.wait_until_idle(:perf_par)
    t1 = System.monotonic_time(:microsecond)

    elapsed_ms = (t1 - t0) / 1000
    ms_per_event = elapsed_ms / n
    events_per_sec = n / (elapsed_ms / 1000)

    IO.puts("")
    IO.puts("=== 1 bus × #{n} parallel events × 1 handler ===")
    IO.puts("  Total:         #{Float.round(elapsed_ms, 1)}ms")
    IO.puts("  Per event:     #{Float.round(ms_per_event, 3)}ms")
    IO.puts("  Events/sec:    #{trunc(events_per_sec)}")

    assert :counters.get(counter, 1) == n
  end
end

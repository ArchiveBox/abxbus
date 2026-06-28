defmodule Abxbus.ScalingTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(ScaleEvent, payload: nil)

  @tag timeout: 120_000
  test "per-event cost at different scales" do
    IO.puts("\n=== Per-Event Cost vs Scale ===\n")

    for n <- [100, 500, 1000, 5000, 10_000, 50_000] do
      {:ok, _} = Abxbus.start_bus(:"scale_#{n}", event_concurrency: :bus_serial)
      counter = :counters.new(1, [:atomics])
      Abxbus.on(:"scale_#{n}", ScaleEvent, fn _e ->
        :counters.add(counter, 1, 1)
        :ok
      end, handler_name: "h")

      t0 = System.monotonic_time(:microsecond)
      for _ <- 1..n, do: Abxbus.emit(:"scale_#{n}", ScaleEvent.new(payload: "x"))
      Abxbus.wait_until_idle(:"scale_#{n}")
      t1 = System.monotonic_time(:microsecond)

      ms_per = (t1 - t0) / 1000 / n
      IO.puts("  #{String.pad_leading("#{n}", 6)} events: #{Float.round(ms_per, 3)} ms/event")

      assert :counters.get(counter, 1) == n
    end

    IO.puts("")
  end
end

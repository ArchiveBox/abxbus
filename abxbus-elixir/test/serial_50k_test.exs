defmodule Abxbus.Serial50kTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(SerialEvent, payload: nil)

  @tag timeout: 120_000
  test "50k serial events" do
    {:ok, _} = Abxbus.start_bus(:serial50k, event_concurrency: :bus_serial)

    counter = :counters.new(1, [:atomics])
    Abxbus.on(:serial50k, SerialEvent, fn _event ->
      :counters.add(counter, 1, 1)
      :ok
    end, handler_name: "serial_handler")

    n = 50_000
    t0 = System.monotonic_time(:microsecond)

    for _ <- 1..n do
      Abxbus.emit(:serial50k, SerialEvent.new(payload: "x"))
    end

    Abxbus.wait_until_idle(:serial50k)
    t1 = System.monotonic_time(:microsecond)

    elapsed_ms = (t1 - t0) / 1000
    ms_per_event = elapsed_ms / n
    events_per_sec = n / (elapsed_ms / 1000)

    IO.puts("")
    IO.puts("=== 1 bus × #{n} serial events × 1 handler (ISOLATED) ===")
    IO.puts("  Total:         #{Float.round(elapsed_ms, 1)}ms")
    IO.puts("  Per event:     #{Float.round(ms_per_event, 3)}ms")
    IO.puts("  Events/sec:    #{trunc(events_per_sec)}")

    assert :counters.get(counter, 1) == n
    assert ms_per_event < 1.0, "Performance regression: #{ms_per_event}ms/event"
  end
end

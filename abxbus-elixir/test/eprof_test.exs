defmodule Abxbus.EprofTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(EprofEvent, payload: nil)

  @tag timeout: 60_000
  test "profile 1000 serial events with eprof" do
    {:ok, _} = Abxbus.start_bus(:eprof_bus, event_concurrency: :bus_serial)

    counter = :counters.new(1, [:atomics])
    Abxbus.on(:eprof_bus, EprofEvent, fn _event ->
      :counters.add(counter, 1, 1)
      :ok
    end, handler_name: "eprof_handler")

    n = 1000
    events = for _ <- 1..n, do: EprofEvent.new(payload: "x")

    # Warmup
    for e <- Enum.take(events, 100), do: Abxbus.emit(:eprof_bus, e)
    Abxbus.wait_until_idle(:eprof_bus)

    # Profile
    :eprof.start()
    :eprof.start_profiling([self(), GenServer.whereis(:eprof_bus)])

    for e <- events, do: Abxbus.emit(:eprof_bus, e)
    Abxbus.wait_until_idle(:eprof_bus)

    :eprof.stop_profiling()
    :eprof.analyze(:total)
    :eprof.stop()

    assert :counters.get(counter, 1) == n + 100
  end
end

defmodule MinimalBus do
  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, [])

  def emit(pid, event), do: GenServer.call(pid, {:emit, event})

  def wait_idle(pid), do: GenServer.call(pid, :wait_idle)

  @impl true
  def init(_) do
    tab = :ets.new(:minimal_events, [:set, :public, write_concurrency: true])
    {:ok, %{tab: tab, count: 0, handler: nil}}
  end

  @impl true
  def handle_call({:emit, event}, _from, state) do
    # Simulate the full inline path: ETS write + handler + ETS update
    :ets.insert(state.tab, {event.event_id, event})

    if state.handler do
      state.handler.(event)
    end

    completed = %{event | event_status: :completed, event_completed_at: System.monotonic_time(:nanosecond)}
    :ets.insert(state.tab, {event.event_id, completed})

    {:reply, event, %{state | count: state.count + 1}}
  end

  def handle_call(:wait_idle, _from, state), do: {:reply, :ok, state}

  def handle_call({:set_handler, fun}, _from, state), do: {:reply, :ok, %{state | handler: fun}}
end

defmodule Abxbus.MinimalGenServerTest do
  use ExUnit.Case, async: false

  import Abxbus.TestEvents
  defevent(MinEvent, payload: nil)

  defp measure(label, n, fun) do
    fun.()
    t0 = System.monotonic_time(:nanosecond)
    fun.()
    t1 = System.monotonic_time(:nanosecond)
    us_per = (t1 - t0) / 1000 / n
    IO.puts("  #{String.pad_trailing(label, 55)} #{Float.round(us_per, 1)} µs/op")
    us_per
  end

  test "minimal GenServer vs full Abxbus" do
    n = 10_000

    IO.puts("\n=== Minimal GenServer Comparison (#{n} iterations) ===\n")

    events = for _ <- 1..n, do: MinEvent.new(payload: "x")

    # 1. Minimal GenServer: call + 2 ETS writes (no handlers)
    {:ok, pid1} = MinimalBus.start_link()
    measure("Minimal GenServer (call + 2 ETS writes)", n, fn ->
      for e <- events, do: MinimalBus.emit(pid1, e)
    end)

    # 2. Minimal GenServer with handler
    {:ok, pid2} = MinimalBus.start_link()
    counter = :counters.new(1, [:atomics])
    GenServer.call(pid2, {:set_handler, fn _e -> :counters.add(counter, 1, 1); :ok end})
    measure("Minimal GenServer (call + handler + 2 ETS writes)", n, fn ->
      for e <- events, do: MinimalBus.emit(pid2, e)
    end)

    # 3. Full Abxbus serial inline (0 handlers)
    {:ok, _} = Abxbus.start_bus(:min_a, event_concurrency: :bus_serial)
    measure("Full Abxbus serial inline (0 handlers)", n, fn ->
      for e <- events, do: Abxbus.emit(:min_a, e)
      Abxbus.wait_until_idle(:min_a)
    end)

    # 4. Full Abxbus serial inline (1 handler)
    {:ok, _} = Abxbus.start_bus(:min_b, event_concurrency: :bus_serial)
    counter2 = :counters.new(1, [:atomics])
    Abxbus.on(:min_b, MinEvent, fn _e -> :counters.add(counter2, 1, 1); :ok end, handler_name: "h")
    measure("Full Abxbus serial inline (1 handler)", n, fn ->
      for e <- events, do: Abxbus.emit(:min_b, e)
      Abxbus.wait_until_idle(:min_b)
    end)

    # 5. Just GenServer.call noop for reference
    {:ok, noop} = GenServer.start_link(NoopServer2, [])
    measure("GenServer.call noop", n, fn ->
      for _ <- 1..n, do: GenServer.call(noop, :noop)
    end)
    GenServer.stop(noop)

    # 6. GenServer.call with 1 ETS insert
    {:ok, pid3} = GenServer.start_link(EtsOnlyServer, [])
    m25 = Map.new(1..25, fn i -> {:"f#{i}", i} end)
    measure("GenServer.call + 1 ETS insert (25-field map)", n, fn ->
      for i <- 1..n, do: GenServer.call(pid3, {:insert, i, m25})
    end)
    GenServer.stop(pid3)

    IO.puts("")
  end
end

defmodule NoopServer2 do
  use GenServer
  def init(_), do: {:ok, %{}}
  def handle_call(:noop, _from, state), do: {:reply, :ok, state}
end

defmodule EtsOnlyServer do
  use GenServer
  def init(_) do
    tab = :ets.new(:ets_only, [:set, :public, write_concurrency: true])
    {:ok, %{tab: tab}}
  end
  def handle_call({:insert, key, val}, _from, state) do
    :ets.insert(state.tab, {key, val})
    {:reply, :ok, state}
  end
end

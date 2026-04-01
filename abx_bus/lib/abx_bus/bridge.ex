defmodule AbxBus.Bridge do
  @moduledoc """
  Behaviour for transport bridges connecting event buses across processes,
  nodes, or external systems.

  A bridge serializes events over a transport (HTTP, Redis, NATS, Postgres,
  JSONL file, SQLite, Unix socket, etc.) and deserializes inbound events
  onto a local bus.

  ## Implementing a bridge

      defmodule MyApp.RedisBridge do
        @behaviour AbxBus.Bridge
        use GenServer

        @impl AbxBus.Bridge
        def start_link(opts) do
          GenServer.start_link(__MODULE__, opts)
        end

        @impl AbxBus.Bridge
        def emit(bridge, event) do
          GenServer.call(bridge, {:emit, event})
        end

        @impl AbxBus.Bridge
        def on(bridge, pattern, handler_fn) do
          GenServer.call(bridge, {:on, pattern, handler_fn})
        end

        @impl AbxBus.Bridge
        def close(bridge) do
          GenServer.stop(bridge)
        end

        # GenServer callbacks...
      end

  ## Bridge lifecycle

    1. `start_link/1` — creates the bridge process + transport connection
    2. `on/3` — registers inbound handlers (auto-starts transport listener)
    3. `emit/2` — sends event over transport
    4. `close/1` — tears down transport + internal bus

  ## Event handling across bridges

  Events crossing a bridge are reset to `:pending` status to allow
  re-routing on the target bus. The `event_path` field prevents loops.
  Parent-child lineage is preserved.
  """

  @doc "Start the bridge process."
  @callback start_link(opts :: keyword()) :: GenServer.on_start()

  @doc "Send an event over the transport."
  @callback emit(bridge :: GenServer.server(), event :: map()) :: :ok | {:error, term()}

  @doc "Register an inbound handler for events matching the pattern."
  @callback on(bridge :: GenServer.server(), pattern :: AbxBus.Types.event_pattern(), handler_fn :: function()) ::
              :ok | {:error, term()}

  @doc "Shut down the bridge and release transport resources."
  @callback close(bridge :: GenServer.server()) :: :ok

  @optional_callbacks []
end

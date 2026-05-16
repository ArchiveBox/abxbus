defmodule Abxbus.Middlewares.WAL do
  @moduledoc """
  Write-Ahead Log middleware -- appends completed events to a JSONL file.

  Since `Jason` is not a dependency, each line is written using `:erlang.term_to_binary/1`
  encoded as Base-64 so the file stays text-friendly, or — when the caller passes
  `format: :inspect` — uses `inspect/1` for human-readable output.

  ## Usage

      # Start an Agent that holds the WAL file path and register it for a bus:
      {:ok, agent} = Abxbus.Middlewares.WAL.start("/tmp/wal.jsonl")
      Abxbus.Middlewares.WAL.register(:my_bus, agent)
      Abxbus.start_bus(:my_bus, middlewares: [Abxbus.Middlewares.WAL])

  Configuration is stored in an ETS table keyed by bus_name, so no dynamic
  module creation is needed (avoiding atom leaks).

  For backwards compatibility, `build/1` still works but now registers the
  agent globally and returns this module.
  """

  @behaviour Abxbus.Middleware

  @wal_config_table :abxbus_wal_config

  # ---------------------------------------------------------------------------
  # Public helpers
  # ---------------------------------------------------------------------------

  @doc """
  Start an Agent that stores the WAL file path (and optional format).

  Returns `{:ok, agent_pid}`.
  """
  def start(path, opts \\ []) do
    format = Keyword.get(opts, :format, :etf_b64)
    Agent.start_link(fn -> %{path: path, format: format} end)
  end

  @doc """
  Register an agent for a specific bus_name. The `on_event_change/3` callback
  will look up the agent from the ETS table when events complete.
  """
  def register(bus_name, agent) do
    ensure_config_table()
    :ets.insert(@wal_config_table, {bus_name, agent})
    :ok
  end

  @doc """
  Unregister the WAL config for a bus.
  """
  def unregister(bus_name) do
    if :ets.whereis(@wal_config_table) != :undefined do
      :ets.delete(@wal_config_table, bus_name)
    end
    :ok
  end

  @doc """
  Build a middleware reference for the given `agent`.

  For backwards compatibility this accepts an agent pid, registers it under
  a global key (`:__wal_default__`), and returns this module. Prefer using
  `register/2` with an explicit bus_name instead.
  """
  def build(agent) do
    ensure_config_table()
    :ets.insert(@wal_config_table, {:__wal_default__, agent})
    __MODULE__
  end

  defp ensure_config_table do
    try do
      :ets.new(@wal_config_table, [:named_table, :public, :set])
    rescue
      ArgumentError -> @wal_config_table
    end
  end

  # ---------------------------------------------------------------------------
  # Behaviour callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def on_event_change(bus_name, event, :completed) do
    agent =
      case :ets.whereis(@wal_config_table) do
        :undefined -> nil
        _tid ->
          case :ets.lookup(@wal_config_table, bus_name) do
            [{_, agent}] -> agent
            [] ->
              # Fall back to global default (set by build/1)
              case :ets.lookup(@wal_config_table, :__wal_default__) do
                [{_, agent}] -> agent
                [] -> nil
              end
          end
      end

    if agent do
      write_event(agent, event)
    else
      :ok
    end
  end

  def on_event_change(_bus_name, _event, _status), do: :ok

  @impl true
  def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

  @impl true
  def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok

  # ---------------------------------------------------------------------------
  # Kept for backwards compatibility with any code calling the 4-arity version
  # ---------------------------------------------------------------------------

  @doc false
  def on_event_change(_bus_name, event, :completed, agent) do
    write_event(agent, event)
  end

  def on_event_change(_bus_name, _event, _status, _agent), do: :ok

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp write_event(agent, event) do
    %{path: path, format: format} = Agent.get(agent, & &1)
    File.mkdir_p!(Path.dirname(path))

    line =
      case format do
        :inspect ->
          inspect(%{
            event_id: event.event_id,
            event_type: to_string(event.event_type),
            event_status: event.event_status
          })

        _etf_b64 ->
          event
          |> Map.take([:event_id, :event_type, :event_status])
          |> Map.update!(:event_type, &to_string/1)
          |> :erlang.term_to_binary()
          |> Base.encode64()
      end

    File.write!(path, line <> "\n", [:append])
  end
end

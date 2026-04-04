defmodule Abxbus.Middlewares.WAL do
  @moduledoc """
  Write-Ahead Log middleware -- appends completed events to a JSONL file.

  Since `Jason` is not a dependency, each line is written using `:erlang.term_to_binary/1`
  encoded as Base-64 so the file stays text-friendly, or — when the caller passes
  `format: :inspect` — uses `inspect/1` for human-readable output.

  ## Usage

      # Start an Agent that holds the WAL file path:
      {:ok, agent} = Abxbus.Middlewares.WAL.start("/tmp/wal.jsonl")

      # Then register a wrapper module that closes over the agent:
      wal_mod = Abxbus.Middlewares.WAL.build(agent)
      Abxbus.start_bus(:my_bus, middlewares: [wal_mod])

  Because the `Abxbus.Middleware` behaviour dispatches to *modules* (not
  instances), `build/2` dynamically defines a one-off module that delegates
  to this module with the right agent reference.
  """

  @behaviour Abxbus.Middleware

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
  Build a dynamically-defined middleware module that delegates to this module
  with the given `agent` pid.

  The generated module satisfies `@behaviour Abxbus.Middleware`.
  """
  def build(agent) do
    mod_name = :"Abxbus.Middlewares.WAL.Instance_#{:erlang.unique_integer([:positive])}"

    Module.create(
      mod_name,
      quote do
        @behaviour Abxbus.Middleware

        @impl true
        def on_event_change(bus_name, event, status) do
          Abxbus.Middlewares.WAL.on_event_change(bus_name, event, status, unquote(agent))
        end

        @impl true
        def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

        @impl true
        def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
      end,
      Macro.Env.location(__ENV__)
    )

    mod_name
  end

  # ---------------------------------------------------------------------------
  # Callback used by dynamically-built modules
  # ---------------------------------------------------------------------------

  @doc false
  def on_event_change(_bus_name, event, :completed, agent) do
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

  def on_event_change(_bus_name, _event, _status, _agent), do: :ok

  # ---------------------------------------------------------------------------
  # Direct behaviour callbacks (no-ops — used only when the module itself is
  # registered as middleware without `build/1`).
  # ---------------------------------------------------------------------------

  @impl true
  def on_event_change(_bus_name, _event, _status), do: :ok

  @impl true
  def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

  @impl true
  def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
end

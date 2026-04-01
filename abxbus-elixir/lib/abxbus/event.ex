defmodule Abxbus.Event do
  @moduledoc """
  Base event behaviour and struct helpers.

  Events are plain data — no process, no GenServer. They flow through the system
  as immutable maps with metadata tracked in an ETS-backed EventStore.

  ## Defining events

      defmodule MyApp.UserCreated do
        use Abxbus.Event, result_type: :string

        defstruct [:username, :email]
      end

  Or with the `defevent` macro for quick definitions:

      import Abxbus.Event
      defevent MyApp.PageLoaded, url: nil, status: 200

  ## Reserved fields

  All fields prefixed with `event_` are reserved for bus metadata. User-defined
  structs must not define fields with this prefix.
  """

  @meta_fields [
    event_id: nil,
    event_type: nil,
    event_version: "1",
    event_result_type: :any,
    event_parent_id: nil,
    event_children: [],
    event_path: [],
    event_status: :pending,
    event_created_at: nil,
    event_started_at: nil,
    event_completed_at: nil,
    event_results: %{},
    event_concurrency: nil,
    event_handler_concurrency: nil,
    event_handler_completion: nil,
    event_timeout: nil,
    event_handler_timeout: nil,
    event_handler_slow_timeout: nil,
    event_slow_timeout: nil,
    event_pending_bus_count: 0,
    event_emitted_by_handler_id: nil
  ]

  @doc false
  def meta_fields, do: @meta_fields

  defmacro __using__(opts) do
    result_type = Keyword.get(opts, :result_type, :any)
    version = Keyword.get(opts, :version, "1")

    quote do
      @behaviour Abxbus.Event

      Module.register_attribute(__MODULE__, :abxbus_event_meta, persist: true)
      Module.put_attribute(__MODULE__, :abxbus_event_meta, true)
      Module.register_attribute(__MODULE__, :abxbus_result_type, persist: true)
      Module.put_attribute(__MODULE__, :abxbus_result_type, unquote(result_type))
      Module.register_attribute(__MODULE__, :abxbus_event_version, persist: true)
      Module.put_attribute(__MODULE__, :abxbus_event_version, unquote(version))

      @before_compile Abxbus.Event
    end
  end

  defmacro __before_compile__(env) do
    user_fields = Module.get_attribute(env.module, :struct) || []

    for {key, _} <- user_fields, String.starts_with?(Atom.to_string(key), "event_") do
      raise CompileError,
        description:
          "Field #{key} in #{inspect(env.module)} starts with 'event_' which is reserved for Abxbus metadata"
    end

    quote do
    end
  end

  @doc """
  Convenience macro to define an event module with struct fields and metadata
  in a single expression.

      defevent MyApp.UserCreated, username: nil, email: nil
      defevent MyApp.PageLoaded, [url: nil], result_type: :map, version: "2"
  """
  defmacro defevent(name, fields_and_opts \\ []) do
    {fields, opts} = split_fields_and_opts(fields_and_opts)

    # Validate no user field starts with event_ (same rule as __before_compile__)
    for {key, _} <- fields do
      if String.starts_with?(Atom.to_string(key), "event_") do
        raise CompileError,
          description: "Field #{key} starts with 'event_' which is reserved for Abxbus metadata"
      end
    end

    result_type = Keyword.get(opts, :result_type, :any)
    version = Keyword.get(opts, :version, "1")

    meta_fields = [
      event_id: nil,
      event_type: nil,
      event_version: version,
      event_result_type: result_type,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_status: :pending,
      event_created_at: nil,
      event_started_at: nil,
      event_completed_at: nil,
      event_results: Macro.escape(%{}),
      event_concurrency: nil,
      event_handler_concurrency: nil,
      event_handler_completion: nil,
      event_timeout: nil,
      event_handler_timeout: nil,
      event_handler_slow_timeout: nil,
      event_slow_timeout: nil,
      event_pending_bus_count: 0,
      event_emitted_by_handler_id: nil
    ]

    escaped_fields =
      Enum.map(fields, fn
        {k, v} when is_map(v) -> {k, Macro.escape(v)}
        other -> other
      end)

    all_fields = Keyword.merge(meta_fields, escaped_fields)

    quote do
      defmodule unquote(name) do
        defstruct unquote(all_fields)

        def new(attrs \\ []) do
          base = struct!(__MODULE__, attrs)

          %{base |
            event_id: base.event_id || Abxbus.Event.generate_id(),
            event_type: __MODULE__,
            event_version: base.event_version || unquote(version),
            event_result_type: base.event_result_type || unquote(result_type),
            event_created_at: base.event_created_at || System.monotonic_time(:nanosecond),
            event_status: :pending
          }
        end

        def event_version, do: unquote(version)
        def event_result_type, do: unquote(result_type)
      end
    end
  end

  defp split_fields_and_opts(list) do
    opts_keys = [:result_type, :version]

    fields =
      Enum.reject(list, fn
        {k, _} -> k in opts_keys
        _ -> false
      end)

    opts =
      Enum.filter(list, fn
        {k, _} -> k in opts_keys
        _ -> false
      end)

    {fields, opts}
  end

  @doc "Create a new event with metadata fields populated."
  def new(module, attrs \\ %{}) when is_atom(module) do
    base = struct!(module, Map.to_list(attrs))

    result_type =
      if function_exported?(module, :event_result_type, 0),
        do: module.event_result_type(),
        else: :any

    version =
      if function_exported?(module, :event_version, 0),
        do: module.event_version(),
        else: "1"

    meta = %{
      event_id: generate_id(),
      event_type: module,
      event_version: version,
      event_result_type: result_type,
      event_created_at: System.monotonic_time(:nanosecond),
      event_status: :pending,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_results: %{},
      event_pending_bus_count: 0,
      event_emitted_by_handler_id: nil
    }

    overridable = [
      :event_concurrency, :event_handler_concurrency, :event_handler_completion,
      :event_timeout, :event_handler_timeout, :event_slow_timeout,
      :event_handler_slow_timeout, :event_version, :event_result_type
    ]
    user_overrides = Map.take(attrs, overridable)

    Map.merge(base, Map.merge(meta, user_overrides))
  end

  @doc """
  Create a fresh pending copy of an event for re-emission.
  Preserves user payload fields; resets all runtime metadata.
  """
  def reset(event) do
    meta_keys = Keyword.keys(@meta_fields)

    # Keep user fields, reset all event_* metadata
    user_fields = Map.drop(event, [:__struct__ | meta_keys])

    base = struct!(event.__struct__, Map.to_list(user_fields))

    %{base |
      event_id: generate_id(),
      event_type: event.event_type,
      event_version: Map.get(event, :event_version, "1"),
      event_result_type: Map.get(event, :event_result_type, :any),
      event_created_at: System.monotonic_time(:nanosecond),
      event_status: :pending,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_results: %{},
      event_pending_bus_count: 0,
      event_emitted_by_handler_id: nil,
      event_concurrency: Map.get(event, :event_concurrency),
      event_handler_concurrency: Map.get(event, :event_handler_concurrency),
      event_handler_completion: Map.get(event, :event_handler_completion),
      event_timeout: Map.get(event, :event_timeout),
      event_handler_timeout: Map.get(event, :event_handler_timeout),
      event_slow_timeout: Map.get(event, :event_slow_timeout),
      event_handler_slow_timeout: Map.get(event, :event_handler_slow_timeout)
    }
  end

  @doc "Generate a time-ordered unique event ID (UUID v7-style)."
  def generate_id do
    ts = System.system_time(:microsecond)
    rand = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    "#{ts}-#{rand}"
  end

  # Callbacks
  @callback handle(map()) :: any()
  @optional_callbacks [handle: 1]
end

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

  @meta_keys Keyword.keys(@meta_fields)

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

    result_type = Module.get_attribute(env.module, :abxbus_result_type) || :any
    version = Module.get_attribute(env.module, :abxbus_event_version) || "1"

    quote do
      def event_version, do: unquote(version)
      def event_result_type, do: unquote(result_type)
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

    # Validate no user field starts with event_ unless it's a known overridable meta field
    known_meta_keys = Keyword.keys(@meta_fields)
    for {key, _} <- fields do
      if String.starts_with?(Atom.to_string(key), "event_") and key not in known_meta_keys do
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
    # Separate metadata overrides from user fields before passing to struct!
    overridable = [
      :event_concurrency, :event_handler_concurrency, :event_handler_completion,
      :event_timeout, :event_handler_timeout, :event_slow_timeout,
      :event_handler_slow_timeout, :event_version, :event_result_type,
      :event_parent_id
    ]
    {override_attrs, user_attrs} = Map.split(attrs, overridable)

    base = struct!(module, Map.to_list(user_attrs))

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

    Map.merge(base, Map.merge(meta, override_attrs))
  end

  @doc """
  Create a fresh pending copy of an event for re-emission.
  Preserves user payload fields; resets all runtime metadata.
  """
  def reset(event) do
    # Keep user fields, reset all event_* metadata
    user_fields = Map.drop(event, [:__struct__ | @meta_keys])

    base = struct!(event.__struct__, Map.to_list(user_fields))

    # Use Map.merge instead of %{base | ...} to handle structs that
    # may not define all event_* keys (e.g. use Abxbus.Event structs)
    Map.merge(base, %{
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
    })
  end

  @doc "Generate a UUIDv7 event ID (time-ordered, cross-runtime compatible)."
  def generate_id do
    # UUIDv7: 48-bit unix_ts_ms | 4-bit version=0111 | 12-bit rand_a | 2-bit var=10 | 62-bit rand_b
    ts_ms = System.system_time(:millisecond)
    <<rand_a::12, rand_b::62, _::6>> = :crypto.strong_rand_bytes(10)

    <<a1::32, a2::16, a3::16, a4::16, a5::48>> =
      <<ts_ms::48, 7::4, rand_a::12, 2::2, rand_b::62>>

    encode_uuid(a1, a2, a3, a4, a5)
  end

  # Direct binary hex encoding — avoids String.downcase/pad_leading/length overhead
  import Bitwise

  defp encode_uuid(a1, a2, a3, a4, a5) do
    <<hex8(a1)::binary, ?-, hex4(a2)::binary, ?-, hex4(a3)::binary, ?-,
      hex4(a4)::binary, ?-, hex12(a5)::binary>>
  end

  defp hex4(n), do: <<hex_char(n >>> 12), hex_char(n >>> 8), hex_char(n >>> 4), hex_char(n)>>
  defp hex8(n), do: <<hex4(n >>> 16)::binary, hex4(n &&& 0xFFFF)::binary>>
  defp hex12(n), do: <<hex8(n >>> 16)::binary, hex4(n &&& 0xFFFF)::binary>>
  defp hex_char(n), do: elem({?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?a, ?b, ?c, ?d, ?e, ?f}, n &&& 0xF)

  # Callbacks
  @callback handle(map()) :: any()
  @optional_callbacks [handle: 1]

  # ── JSON serialization ────────────────────────────────────────────────────

  @doc """
  Serialize an event to a JSON-compatible map with string keys.

  Field names match Python/TS exactly — no aliasing or renaming.
  Atoms are converted to strings, event_results serialized as a list.
  """
  def to_json(event) do
    event
    |> Map.from_struct()
    |> Map.update(:event_results, [], fn results ->
      results
      |> Map.values()
      |> Enum.map(&json_encode_value/1)
    end)
    |> Enum.reduce(%{}, fn {k, v}, acc ->
      Map.put(acc, Atom.to_string(k), json_encode_value(v))
    end)
  end

  @doc """
  Deserialize a JSON map (string keys) back to an event struct.

  If `module` is nil, tries to resolve from `event_type`.
  """
  def from_json(json_map, module \\ nil) do
    atomized =
      Enum.reduce(json_map, %{}, fn {k, v}, acc ->
        key = if is_binary(k), do: safe_to_atom(k), else: k
        Map.put(acc, key, json_decode_value(key, v))
      end)

    atomized = restore_event_type(atomized)
    atomized = restore_event_status(atomized)
    atomized = restore_event_results(atomized)

    target = module || Map.get(atomized, :event_type)

    if is_atom(target) and target != nil and function_exported?(target, :__struct__, 0) do
      struct!(target, Map.to_list(atomized))
    else
      atomized
    end
  end

  @doc "Encode an event to a JSON string."
  def to_json_string(event), do: event |> to_json() |> Abxbus.JSON.encode()

  @doc "Decode a JSON string to an event."
  def from_json_string(json_string, module \\ nil) do
    json_string |> Abxbus.JSON.decode() |> from_json(module)
  end

  defp json_encode_value(nil), do: nil
  defp json_encode_value(true), do: true
  defp json_encode_value(false), do: false
  defp json_encode_value(v) when is_atom(v), do: Atom.to_string(v)
  defp json_encode_value(v) when is_binary(v), do: v
  defp json_encode_value(v) when is_number(v), do: v
  defp json_encode_value(v) when is_list(v), do: Enum.map(v, &json_encode_value/1)
  defp json_encode_value(%{__struct__: _} = v), do: json_encode_value(Map.from_struct(v))
  defp json_encode_value(v) when is_map(v) do
    Enum.reduce(v, %{}, fn {k, val}, acc ->
      Map.put(acc, to_string(k), json_encode_value(val))
    end)
  end
  defp json_encode_value(v), do: inspect(v)

  defp json_decode_value(:event_concurrency, v) when is_binary(v), do: safe_to_atom(v)
  defp json_decode_value(:event_handler_concurrency, v) when is_binary(v), do: safe_to_atom(v)
  defp json_decode_value(:event_handler_completion, v) when is_binary(v), do: safe_to_atom(v)
  defp json_decode_value(:event_result_type, v) when is_binary(v), do: safe_to_atom(v)
  defp json_decode_value(_key, v), do: v

  defp restore_event_type(%{event_type: t} = m) when is_binary(t) do
    Map.put(m, :event_type, safe_to_atom(t))
  end
  defp restore_event_type(m), do: m

  defp restore_event_status(%{event_status: s} = m) when is_binary(s) do
    Map.put(m, :event_status, safe_to_atom(s))
  end
  defp restore_event_status(m), do: m

  defp restore_event_results(%{event_results: results} = m) when is_list(results) do
    restored =
      Enum.reduce(results, %{}, fn result_map, acc ->
        id = result_map["handler_id"] || Map.get(result_map, :handler_id)
        if id, do: Map.put(acc, id, result_map), else: acc
      end)
    Map.put(m, :event_results, restored)
  end
  defp restore_event_results(m), do: m

  defp safe_to_atom(s) when is_binary(s) do
    try do
      String.to_existing_atom(s)
    rescue
      ArgumentError -> s
    end
  end
end

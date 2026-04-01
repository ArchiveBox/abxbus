defmodule AbxBus.Event do
  @moduledoc """
  Base event behaviour and struct helpers.

  Events are plain data — no process, no GenServer. They flow through the system
  as immutable maps with metadata tracked in an ETS-backed EventStore.

  ## Defining events

      defmodule MyApp.UserCreated do
        use AbxBus.Event

        defstruct [:username, :email]
      end

  Events defined this way automatically get all `event_*` metadata fields merged
  into their struct at creation time via `new/1`.

  ## Reserved fields

  All fields prefixed with `event_` are reserved for bus metadata. User-defined
  structs must not define fields with this prefix.
  """

  @type t :: %{
          __struct__: module(),
          event_id: binary() | nil,
          event_type: module() | nil,
          event_parent_id: binary() | nil,
          event_children: [binary()],
          event_path: [binary()],
          event_status: AbxBus.Types.event_status(),
          event_created_at: integer() | nil,
          event_started_at: integer() | nil,
          event_completed_at: integer() | nil,
          event_results: %{binary() => AbxBus.HandlerResult.t()},
          event_concurrency: AbxBus.Types.event_concurrency() | nil,
          event_handler_concurrency: AbxBus.Types.handler_concurrency() | nil,
          event_handler_completion: AbxBus.Types.handler_completion() | nil,
          event_timeout: number() | nil,
          event_handler_timeout: number() | nil,
          event_slow_timeout: number() | nil,
          event_pending_bus_count: non_neg_integer(),
          event_emitted_by_handler_id: binary() | nil
        }

  @meta_fields [
    event_id: nil,
    event_type: nil,
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
    event_slow_timeout: nil,
    event_pending_bus_count: 0,
    event_emitted_by_handler_id: nil
  ]

  defmacro __using__(_opts) do
    meta_fields = Macro.escape(@meta_fields)

    quote do
      @behaviour AbxBus.Event

      # Merge user-defined struct fields with event metadata fields.
      # User calls `defstruct [:foo, :bar]` AFTER `use AbxBus.Event`,
      # and we inject the meta fields via @derive / defoverridable.

      Module.register_attribute(__MODULE__, :abx_event_meta, persist: true)
      Module.put_attribute(__MODULE__, :abx_event_meta, unquote(meta_fields))

      @before_compile AbxBus.Event
    end
  end

  defmacro __before_compile__(env) do
    user_fields = Module.get_attribute(env.module, :struct) || []
    meta = Module.get_attribute(env.module, :abx_event_meta) || @meta_fields

    # Validate no user field starts with event_
    for {key, _} <- user_fields, String.starts_with?(Atom.to_string(key), "event_") do
      raise CompileError,
        description:
          "Field #{key} in #{inspect(env.module)} starts with 'event_' which is reserved for AbxBus metadata"
    end

    # We need to redefine __struct__ to include meta fields.
    # This is handled by the new/1 helper instead, to avoid double-defstruct issues.
    quote do
    end
  end

  @doc "Create a new event with metadata fields populated."
  def new(module, attrs \\ %{}) when is_atom(module) do
    base = struct!(module, Map.to_list(attrs))

    meta = %{
      event_id: generate_id(),
      event_type: module,
      event_created_at: System.monotonic_time(:nanosecond),
      event_status: :pending,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_results: %{},
      event_pending_bus_count: 0
    }

    # Merge: user attrs can override event_concurrency etc. but not event_id
    user_overrides =
      attrs
      |> Map.take([
        :event_concurrency,
        :event_handler_concurrency,
        :event_handler_completion,
        :event_timeout,
        :event_handler_timeout,
        :event_slow_timeout
      ])

    Map.merge(base, Map.merge(meta, user_overrides))
  end

  @doc "Generate a time-ordered unique event ID (UUID v7-style)."
  def generate_id do
    # Monotonic-time prefix for ordering + random suffix for uniqueness
    ts = System.system_time(:microsecond)
    rand = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    "#{ts}-#{rand}"
  end

  # Callbacks
  @callback handle(t()) :: any()
  @optional_callbacks [handle: 1]
end

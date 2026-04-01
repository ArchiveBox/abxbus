defmodule AbxBus.HandlerEntry do
  @moduledoc """
  A registered handler for an event type on a bus.

  Wraps the user-provided function with metadata for timeout, concurrency
  override, retry, and file-path detection.
  """

  @type t :: %__MODULE__{
          id: binary(),
          event_type: module() | :wildcard,
          handler_fn: (AbxBus.Event.t() -> any()),
          handler_name: binary() | nil,
          handler_timeout: number() | nil,
          max_attempts: pos_integer(),
          semaphore_scope: :none | :global | :bus,
          semaphore_name: binary() | nil,
          semaphore_limit: pos_integer(),
          registered_at: integer(),
          bus_name: binary()
        }

  defstruct [
    :id,
    :event_type,
    :handler_fn,
    :handler_name,
    :handler_timeout,
    :semaphore_name,
    :bus_name,
    max_attempts: 1,
    semaphore_scope: :none,
    semaphore_limit: 1,
    registered_at: nil
  ]

  def new(event_type, handler_fn, opts \\ []) do
    name =
      Keyword.get_lazy(opts, :handler_name, fn ->
        case Function.info(handler_fn, :name) do
          {:name, n} -> Atom.to_string(n)
          _ -> "anonymous"
        end
      end)

    %__MODULE__{
      id: AbxBus.Event.generate_id(),
      event_type: event_type,
      handler_fn: handler_fn,
      handler_name: name,
      handler_timeout: Keyword.get(opts, :timeout),
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      semaphore_scope: Keyword.get(opts, :semaphore_scope, :none),
      semaphore_name: Keyword.get(opts, :semaphore_name),
      semaphore_limit: Keyword.get(opts, :semaphore_limit, 1),
      bus_name: Keyword.get(opts, :bus_name, ""),
      registered_at: System.monotonic_time(:nanosecond)
    }
  end
end

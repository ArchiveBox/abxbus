defmodule AbxBus.HandlerEntry do
  @moduledoc """
  A registered handler for an event type on a bus.

  Wraps the user-provided function with metadata for timeout, concurrency
  override, retry, and file-path detection.
  """

  @type t :: %__MODULE__{
          id: binary(),
          event_type: module() | :wildcard,
          handler_fn: (map() -> any()),
          handler_name: binary() | nil,
          handler_timeout: number() | nil,
          handler_slow_timeout: number() | nil,
          handler_file_path: binary() | nil,
          max_attempts: pos_integer(),
          retry_after: number(),
          retry_backoff_factor: number(),
          retry_on_errors: [module()] | nil,
          semaphore_scope: :none | :global | :bus,
          semaphore_name: binary() | nil,
          semaphore_limit: pos_integer(),
          semaphore_timeout: number() | nil,
          semaphore_lax: boolean(),
          registered_at: integer(),
          bus_name: atom() | binary()
        }

  defstruct [
    :id,
    :event_type,
    :handler_fn,
    :handler_name,
    :handler_timeout,
    :handler_slow_timeout,
    :handler_file_path,
    :semaphore_name,
    :semaphore_timeout,
    :bus_name,
    :retry_on_errors,
    max_attempts: 1,
    retry_after: 0,
    retry_backoff_factor: 1.0,
    semaphore_scope: :none,
    semaphore_limit: 1,
    semaphore_lax: true,
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

    file_path =
      if Keyword.get(opts, :detect_file_paths, true) do
        case Function.info(handler_fn, :module) do
          {:module, mod} -> "#{mod}"
          _ -> nil
        end
      end

    %__MODULE__{
      id: AbxBus.Event.generate_id(),
      event_type: event_type,
      handler_fn: handler_fn,
      handler_name: name,
      handler_timeout: Keyword.get(opts, :timeout),
      handler_slow_timeout: Keyword.get(opts, :handler_slow_timeout),
      handler_file_path: file_path,
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      retry_after: Keyword.get(opts, :retry_after, 0),
      retry_backoff_factor: Keyword.get(opts, :retry_backoff_factor, 1.0),
      retry_on_errors: Keyword.get(opts, :retry_on_errors),
      semaphore_scope: Keyword.get(opts, :semaphore_scope, :none),
      semaphore_name: Keyword.get(opts, :semaphore_name),
      semaphore_limit: Keyword.get(opts, :semaphore_limit, 1),
      semaphore_timeout: Keyword.get(opts, :semaphore_timeout),
      semaphore_lax: Keyword.get(opts, :semaphore_lax, true),
      bus_name: Keyword.get(opts, :bus_name, ""),
      registered_at: System.monotonic_time(:nanosecond)
    }
  end
end

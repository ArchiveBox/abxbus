defmodule Abxbus.EventHandler do
  @moduledoc """
  A registered handler for an event type on a bus.

  Wraps the user-provided function with metadata for timeout, concurrency
  override, retry, and file-path detection.

  In the Python version this class is called `EventHandler`.
  """

  @type t :: %__MODULE__{
          id: binary(),
          event_pattern: module() | binary() | :wildcard,
          handler: (map() -> any()),
          handler_name: binary() | nil,
          handler_timeout: number() | nil,
          handler_slow_timeout: number() | nil,
          handler_file_path: binary() | nil,
          handler_registered_at: integer(),
          max_attempts: pos_integer(),
          retry_after: number(),
          retry_backoff_factor: number(),
          retry_on_errors: [module()] | nil,
          semaphore_scope: :none | :global | :bus,
          semaphore_name: binary() | nil,
          semaphore_limit: pos_integer(),
          semaphore_timeout: number() | nil,
          semaphore_lax: boolean(),
          eventbus_name: atom() | binary(),
          eventbus_id: binary() | nil
        }

  defstruct [
    :id,
    :event_pattern,
    :handler,
    :handler_name,
    :handler_timeout,
    :handler_slow_timeout,
    :handler_file_path,
    :handler_registered_at,
    :semaphore_name,
    :semaphore_timeout,
    :eventbus_name,
    :eventbus_id,
    :retry_on_errors,
    max_attempts: 1,
    retry_after: 0,
    retry_backoff_factor: 1.0,
    semaphore_scope: :none,
    semaphore_limit: 1,
    semaphore_lax: true
  ]

  def new(event_pattern, handler, opts \\ []) do
    name =
      Keyword.get_lazy(opts, :handler_name, fn ->
        case Function.info(handler, :name) do
          {:name, n} -> Atom.to_string(n)
          _ -> "anonymous"
        end
      end)

    file_path =
      if Keyword.get(opts, :detect_file_paths, true) do
        case Function.info(handler, :module) do
          {:module, mod} -> "#{mod}"
          _ -> nil
        end
      end

    # ISO datetime string for cross-runtime stability of handler_id
    registered_at = DateTime.utc_now() |> DateTime.to_iso8601()
    eventbus_name = Keyword.get(opts, :eventbus_name, "")
    eventbus_id = Keyword.get(opts, :eventbus_id)

    pattern_str = pattern_to_string(event_pattern)

    handler = %__MODULE__{
      event_pattern: event_pattern,
      handler: handler,
      handler_name: name,
      handler_timeout: Keyword.get(opts, :timeout),
      handler_slow_timeout: Keyword.get(opts, :handler_slow_timeout),
      handler_file_path: file_path,
      handler_registered_at: registered_at,
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      retry_after: Keyword.get(opts, :retry_after, 0),
      retry_backoff_factor: Keyword.get(opts, :retry_backoff_factor, 1.0),
      retry_on_errors: Keyword.get(opts, :retry_on_errors),
      semaphore_scope: Keyword.get(opts, :semaphore_scope, :none),
      semaphore_name: Keyword.get(opts, :semaphore_name),
      semaphore_limit: Keyword.get(opts, :semaphore_limit, 1),
      semaphore_timeout: Keyword.get(opts, :semaphore_timeout),
      semaphore_lax: Keyword.get(opts, :semaphore_lax, true),
      eventbus_name: eventbus_name,
      eventbus_id: eventbus_id
    }

    # Allow explicit id override (matches Python from_callable behavior)
    id =
      Keyword.get_lazy(opts, :id, fn ->
        compute_handler_id(eventbus_id, name, file_path, registered_at, pattern_str)
      end)

    %{handler | id: id}
  end

  @doc """
  Compute a deterministic handler ID matching Python/TS uuidv5 algorithm.
  Seed format: "{eventbus_id}|{handler_name}|{file_path}|{registered_at}|{event_pattern}"
  """
  def compute_handler_id(eventbus_id, handler_name, handler_file_path, handler_registered_at, event_pattern) do
    file_path = handler_file_path || "unknown"
    bus_id = eventbus_id || "unknown"
    seed = "#{bus_id}|#{handler_name}|#{file_path}|#{handler_registered_at}|#{event_pattern}"
    Abxbus.Event.uuid5(Abxbus.Event.handler_id_namespace(), seed)
  end

  @doc "Compute handler ID from a struct (convenience)."
  def compute_handler_id(%__MODULE__{} = entry) do
    compute_handler_id(
      entry.eventbus_id,
      entry.handler_name,
      entry.handler_file_path,
      entry.handler_registered_at,
      pattern_to_string(entry.event_pattern)
    )
  end

  defp pattern_to_string(:wildcard), do: "*"
  defp pattern_to_string(p) when is_atom(p), do: Atom.to_string(p)
  defp pattern_to_string(p) when is_binary(p), do: p

  @doc "Handler label: handler_name#short_id"
  def label(%__MODULE__{handler_name: name, id: id}) do
    short = id |> String.split("-") |> List.last() |> String.slice(0, 8)
    "#{name || "anonymous"}##{short}"
  end
end

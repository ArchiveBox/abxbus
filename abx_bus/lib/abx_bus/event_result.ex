defmodule AbxBus.EventResult do
  @moduledoc """
  Tracks the outcome of a single handler invocation for one event.

  Each handler registered on a bus produces one EventResult per event dispatch.
  Results accumulate in `event.event_results[handler_id]`.

  In the Python version this class is called `EventResult`.
  """

  @type t :: %__MODULE__{
          id: binary(),
          event_id: binary() | nil,
          handler_id: binary(),
          handler_name: binary() | nil,
          handler_file_path: binary() | nil,
          status: AbxBus.Types.handler_result_status(),
          result: any(),
          result_type: atom(),
          error: any(),
          timeout: number() | nil,
          started_at: integer() | nil,
          completed_at: integer() | nil,
          handler_registered_at: integer() | nil,
          eventbus_name: binary() | nil,
          eventbus_id: binary() | nil,
          event_children: [binary()]
        }

  defstruct [
    :id,
    :event_id,
    :handler_id,
    :handler_name,
    :handler_file_path,
    :result,
    :error,
    :timeout,
    :started_at,
    :completed_at,
    :handler_registered_at,
    :eventbus_name,
    :eventbus_id,
    status: :pending,
    result_type: :any,
    event_children: []
  ]

  def new(handler_id, opts \\ []) do
    %__MODULE__{
      id: AbxBus.Event.generate_id(),
      event_id: Keyword.get(opts, :event_id),
      handler_id: handler_id,
      handler_name: Keyword.get(opts, :handler_name),
      handler_file_path: Keyword.get(opts, :handler_file_path),
      status: :pending,
      result_type: Keyword.get(opts, :result_type, :any),
      timeout: Keyword.get(opts, :timeout),
      handler_registered_at: Keyword.get(opts, :handler_registered_at),
      eventbus_name: Keyword.get(opts, :eventbus_name),
      eventbus_id: Keyword.get(opts, :eventbus_id)
    }
  end

  def mark_started(result) do
    %{result | status: :started, started_at: System.monotonic_time(:nanosecond)}
  end

  def mark_completed(result, value) do
    %{result |
      status: :completed,
      result: value,
      completed_at: System.monotonic_time(:nanosecond)
    }
  end

  def mark_error(result, error) do
    %{result |
      status: :error,
      error: error,
      completed_at: System.monotonic_time(:nanosecond)
    }
  end

  def mark_cancelled(result) do
    %{result |
      status: :cancelled,
      error: %AbxBus.EventHandlerCancelledError{},
      completed_at: System.monotonic_time(:nanosecond)
    }
  end

  def mark_aborted(result) do
    %{result |
      status: :error,
      error: %AbxBus.EventHandlerAbortedError{},
      completed_at: System.monotonic_time(:nanosecond)
    }
  end
end

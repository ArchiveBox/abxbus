defmodule AbxBus.EventHandlerTimeoutError do
  defexception message: "Event handler exceeded its timeout"
end

defmodule AbxBus.EventHandlerAbortedError do
  defexception message: "Event handler aborted due to event-level timeout"
end

defmodule AbxBus.EventHandlerCancelledError do
  defexception message: "Event handler was cancelled before it could start"
end

defmodule AbxBus.EventTimeoutError do
  defexception message: "Event exceeded its timeout"
end

defmodule AbxBus.HistoryFullError do
  defexception message: "Event history is full and max_history_drop is false"
end

defmodule AbxBus.HandlerNotFoundError do
  defexception message: "Handler not found"
end

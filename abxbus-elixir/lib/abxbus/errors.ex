defmodule Abxbus.EventHandlerTimeoutError do
  defexception message: "Event handler exceeded its timeout"
end

defmodule Abxbus.EventHandlerAbortedError do
  defexception message: "Event handler aborted due to event-level timeout"
end

defmodule Abxbus.EventHandlerCancelledError do
  defexception message: "Event handler was cancelled before it could start"
end

defmodule Abxbus.EventTimeoutError do
  defexception message: "Event exceeded its timeout"
end

defmodule Abxbus.HistoryFullError do
  defexception message: "Event history is full and max_history_drop is false"
end

defmodule Abxbus.HandlerNotFoundError do
  defexception message: "Handler not found"
end

defmodule Abxbus.EventHandlerResultSchemaError do
  defexception [:message, :expected, :actual]
end

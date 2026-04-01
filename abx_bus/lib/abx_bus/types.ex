defmodule AbxBus.Types do
  @moduledoc """
  Core enumerations and type definitions for AbxBus.

  Three orthogonal concurrency axes:
    1. Event concurrency   – how many events can process simultaneously on/across buses
    2. Handler concurrency  – how many handlers run in parallel for ONE event
    3. Handler completion   – when an event is "done" (:all handlers, or :first non-nil result)
  """

  @type event_concurrency :: :parallel | :bus_serial | :global_serial
  @type handler_concurrency :: :parallel | :serial
  @type handler_completion :: :all | :first

  @type event_id :: binary()
  @type handler_id :: binary()
  @type bus_name :: atom() | binary()

  @type event_status :: :pending | :started | :completed | :error

  @type handler_result_status :: :pending | :started | :completed | :error | :cancelled

  @type timestamp :: integer()
end

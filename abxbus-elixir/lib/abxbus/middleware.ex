defmodule Abxbus.Middleware do
  @moduledoc """
  Behaviour for EventBus middleware (lifecycle interceptors).

  Middlewares receive notifications at key lifecycle transitions:

    * `on_event_change/3`        — event status transitions (pending → started → completed)
    * `on_event_result_change/4` — per-handler result transitions
    * `on_bus_handlers_change/3` — handler registration/unregistration

  ## Implementing a middleware

      defmodule MyApp.LoggerMiddleware do
        @behaviour Abxbus.Middleware

        @impl true
        def on_event_change(bname, event, status) do
          IO.puts("[" <> to_string(bname) <> "] Event " <> event.event_id <> " -> " <> to_string(status))
          :ok
        end

        @impl true
        def on_event_result_change(bname, event, result, status) do
          IO.puts("[" <> to_string(bname) <> "] Handler " <> (result.handler_name || "?") <> " -> " <> to_string(status))
          :ok
        end

        @impl true
        def on_bus_handlers_change(bname, handler, registered?) do
          action = if registered?, do: "registered", else: "unregistered"
          IO.puts("[" <> to_string(bname) <> "] Handler " <> (handler.handler_name || "?") <> " " <> action)
          :ok
        end
      end

  ## Using middlewares

      Abxbus.start_bus(:main, middlewares: [MyApp.LoggerMiddleware])

  ## Status values

  The `status` argument is one of: `:pending`, `:started`, `:completed`.
  Errors are exposed via `result.status == :error` and `result.error` in
  the `:completed` callback.
  """

  @doc "Called when an event transitions status on this bus."
  @callback on_event_change(
              bus_name :: Abxbus.Types.bus_name(),
              event :: map(),
              status :: :pending | :started | :completed
            ) :: :ok | {:error, term()}

  @doc "Called when a handler result transitions status."
  @callback on_event_result_change(
              bus_name :: Abxbus.Types.bus_name(),
              event :: map(),
              result :: Abxbus.EventResult.t(),
              status :: :pending | :started | :completed
            ) :: :ok | {:error, term()}

  @doc "Called when a handler is registered or unregistered."
  @callback on_bus_handlers_change(
              bus_name :: Abxbus.Types.bus_name(),
              handler :: Abxbus.EventHandler.t(),
              registered? :: boolean()
            ) :: :ok | {:error, term()}

  @optional_callbacks [on_event_change: 3, on_event_result_change: 4, on_bus_handlers_change: 3]

  @doc "Dispatch a lifecycle event to all middlewares. Failures are logged but don't crash."
  def dispatch(middlewares, callback, args) do
    for middleware <- middlewares do
      if function_exported?(middleware, callback, length(args)) do
        try do
          case apply(middleware, callback, args) do
            {:error, reason} ->
              require Logger
              Logger.warning("Middleware #{inspect(middleware)}.#{callback} returned error: #{inspect(reason)}")

            _ ->
              :ok
          end
        rescue
          e ->
            require Logger
            Logger.warning("Middleware #{inspect(middleware)}.#{callback} failed with exception: #{inspect(e)}")
        catch
          kind, reason ->
            require Logger
            Logger.warning("Middleware #{inspect(middleware)}.#{callback} failed with #{kind}: #{inspect(reason)}")
        end
      end
    end

    :ok
  end
end

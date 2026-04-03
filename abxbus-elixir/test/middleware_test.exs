defmodule Abxbus.MiddlewareTest do
  use ExUnit.Case, async: false
  import Abxbus.TestEvents

  # ── Event definitions ──────────────────────────────────────────────────────
  defevent(MWTestEvent, payload: "test")
  defevent(MWErrorEvent, payload: "error")
  defevent(MWOrderEvent, payload: "order")
  defevent(MWDeterminismEvent, idx: 0)

  # ── Test middleware modules ────────────────────────────────────────────────

  defmodule EventChangeMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, status) do
      Agent.update(:mw_event_changes, fn log -> log ++ [status] end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, status) do
      Agent.update(:mw_event_changes, fn log -> log ++ [{:result, status}] end)
      :ok
    end

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?) do
      :ok
    end
  end

  defmodule ResultChangeMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, _status), do: :ok

    @impl true
    def on_event_result_change(_bus_name, _event, result, status) do
      Agent.update(:mw_result_changes, fn log -> log ++ [{result.handler_name, status}] end)
      :ok
    end

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule ErrorObserverMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, _status), do: :ok

    @impl true
    def on_event_result_change(_bus_name, _event, result, status) do
      Agent.update(:mw_error_observations, fn log -> log ++ [{status, result.error}] end)
      :ok
    end

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule HandlerChangeMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, _status), do: :ok

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, handler, registered?) do
      Agent.update(:mw_handler_changes, fn log -> log ++ [{handler.handler_name, registered?}] end)
      :ok
    end
  end

  defmodule OrderMiddlewareA do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, status) do
      Agent.update(:mw_ordering, fn log -> log ++ [{:a, status}] end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule OrderMiddlewareB do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, status) do
      Agent.update(:mw_ordering, fn log -> log ++ [{:b, status}] end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule CrashingMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, _status) do
      raise "middleware crash!"
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule DeterminismMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, event, status) do
      Agent.update(:mw_determinism, fn log ->
        Map.update(log, event.event_id, [status], fn existing -> existing ++ [status] end)
      end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  # ── Tests ──────────────────────────────────────────────────────────────────

  describe "middleware lifecycle hooks" do
    test "middleware receives event status transitions" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_event_changes)

      {:ok, _} = Abxbus.start_bus(:mw_event_status, middlewares: [EventChangeMiddleware])

      Abxbus.on(:mw_event_status, MWTestEvent, fn _event -> :ok end)
      Abxbus.emit(:mw_event_status, MWTestEvent.new())
      Abxbus.wait_until_idle(:mw_event_status)

      log = Agent.get(:mw_event_changes, & &1)
      event_statuses = Enum.filter(log, &is_atom/1)

      assert event_statuses == [:pending, :started, :completed]

      Abxbus.stop(:mw_event_status, clear: true)
      Agent.stop(agent)
    end

    test "middleware receives handler result transitions" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_result_changes)

      {:ok, _} = Abxbus.start_bus(:mw_result_status, middlewares: [ResultChangeMiddleware])

      Abxbus.on(:mw_result_status, MWTestEvent, fn _event -> :ok end, handler_name: "test_handler")
      Abxbus.emit(:mw_result_status, MWTestEvent.new())
      Abxbus.wait_until_idle(:mw_result_status)

      log = Agent.get(:mw_result_changes, & &1)
      statuses = Enum.map(log, fn {_name, status} -> status end)

      assert :started in statuses
      assert :completed in statuses

      Abxbus.stop(:mw_result_status, clear: true)
      Agent.stop(agent)
    end

    test "middleware observes handler errors" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_error_observations)

      {:ok, _} = Abxbus.start_bus(:mw_error_obs, middlewares: [ErrorObserverMiddleware])

      Abxbus.on(:mw_error_obs, MWErrorEvent, fn _event ->
        raise "handler boom!"
      end, handler_name: "failing_handler")

      Abxbus.emit(:mw_error_obs, MWErrorEvent.new())
      Abxbus.wait_until_idle(:mw_error_obs)

      log = Agent.get(:mw_error_observations, & &1)
      error_entries = Enum.filter(log, fn {status, _error} -> status == :error end)

      assert length(error_entries) > 0
      {_status, error} = hd(error_entries)
      assert error != nil

      Abxbus.stop(:mw_error_obs, clear: true)
      Agent.stop(agent)
    end

    test "middleware receives handler registration/unregistration" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_handler_changes)

      {:ok, _} = Abxbus.start_bus(:mw_handler_reg, middlewares: [HandlerChangeMiddleware])

      handler_fn = fn _event -> :ok end
      Abxbus.on(:mw_handler_reg, MWTestEvent, handler_fn, handler_name: "my_handler")
      Abxbus.off(:mw_handler_reg, MWTestEvent, handler_fn)

      log = Agent.get(:mw_handler_changes, & &1)

      registered = Enum.filter(log, fn {_name, reg?} -> reg? end)
      unregistered = Enum.filter(log, fn {_name, reg?} -> not reg? end)

      assert length(registered) >= 1
      assert length(unregistered) >= 1

      Abxbus.stop(:mw_handler_reg, clear: true)
      Agent.stop(agent)
    end
  end

  describe "middleware ordering" do
    test "multiple middlewares called in registration order" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_ordering)

      {:ok, _} = Abxbus.start_bus(:mw_order,
        middlewares: [OrderMiddlewareA, OrderMiddlewareB])

      Abxbus.on(:mw_order, MWOrderEvent, fn _event -> :ok end)
      Abxbus.emit(:mw_order, MWOrderEvent.new())
      Abxbus.wait_until_idle(:mw_order)

      log = Agent.get(:mw_ordering, & &1)

      # For each status transition, A should be called before B
      a_pending_idx = Enum.find_index(log, fn entry -> entry == {:a, :pending} end)
      b_pending_idx = Enum.find_index(log, fn entry -> entry == {:b, :pending} end)

      assert a_pending_idx != nil
      assert b_pending_idx != nil
      assert a_pending_idx < b_pending_idx

      a_started_idx = Enum.find_index(log, fn entry -> entry == {:a, :started} end)
      b_started_idx = Enum.find_index(log, fn entry -> entry == {:b, :started} end)

      assert a_started_idx != nil
      assert b_started_idx != nil
      assert a_started_idx < b_started_idx

      Abxbus.stop(:mw_order, clear: true)
      Agent.stop(agent)
    end
  end

  describe "middleware error isolation" do
    test "middleware error does not crash bus" do
      {:ok, result_agent} = Agent.start_link(fn -> [] end, name: :mw_crash_results)

      {:ok, _} = Abxbus.start_bus(:mw_crash, middlewares: [CrashingMiddleware])

      Abxbus.on(:mw_crash, MWTestEvent, fn _event ->
        Agent.update(:mw_crash_results, fn log -> log ++ [:handler_ran] end)
        :ok
      end)

      Abxbus.emit(:mw_crash, MWTestEvent.new())
      Abxbus.wait_until_idle(:mw_crash)

      # The bus should still work despite the middleware crash
      results = Agent.get(:mw_crash_results, & &1)
      assert :handler_ran in results

      Abxbus.stop(:mw_crash, clear: true)
      Agent.stop(result_agent)
    end
  end

  describe "event status determinism" do
    test "event status order is deterministic per event" do
      {:ok, agent} = Agent.start_link(fn -> %{} end, name: :mw_determinism)

      {:ok, _} = Abxbus.start_bus(:mw_determ_bus,
        middlewares: [DeterminismMiddleware],
        event_concurrency: :parallel)

      Abxbus.on(:mw_determ_bus, MWDeterminismEvent, fn _event -> :ok end)

      events =
        for i <- 1..20 do
          Abxbus.emit(:mw_determ_bus, MWDeterminismEvent.new(idx: i))
        end

      Abxbus.wait_until_idle(:mw_determ_bus)

      transitions = Agent.get(:mw_determinism, & &1)

      for event <- events do
        statuses = Map.get(transitions, event.event_id, [])
        assert statuses == [:pending, :started, :completed],
          "Event #{event.event_id} had transitions #{inspect(statuses)}, expected [:pending, :started, :completed]"
      end

      Abxbus.stop(:mw_determ_bus, clear: true)
      Agent.stop(agent)
    end
  end
end

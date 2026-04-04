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

  # ── Additional middleware tests ───────────────────────────────────────────

  defevent(MWHookStatusEvent, payload: "hook_status")
  defevent(MWStringWildcardEvent, payload: "sw")
  defevent(MWMonotonicEvent, payload: "monotonic", event_timeout: 0.5)

  defmodule HookStatusMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, status) do
      Agent.update(:mw_hook_statuses, fn log -> log ++ [{:event, status}] end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, status) do
      Agent.update(:mw_hook_statuses, fn log -> log ++ [{:result, status}] end)
      :ok
    end

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  defmodule HandlerPatternMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, _status), do: :ok

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, handler, registered?) do
      Agent.update(:mw_handler_patterns, fn log ->
        log ++ [{handler.handler_name, registered?}]
      end)
      :ok
    end
  end

  defmodule MonotonicMiddleware do
    @behaviour Abxbus.Middleware

    @impl true
    def on_event_change(_bus_name, _event, status) do
      Agent.update(:mw_monotonic, fn log -> log ++ [status] end)
      :ok
    end

    @impl true
    def on_event_result_change(_bus_name, _event, _result, _status), do: :ok

    @impl true
    def on_bus_handlers_change(_bus_name, _handler, _registered?), do: :ok
  end

  describe "middleware hook statuses never emit error" do
    test "middleware hook statuses never emit error" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_hook_statuses)

      {:ok, _} = Abxbus.start_bus(:mw_hook_status_bus, middlewares: [HookStatusMiddleware])

      Abxbus.on(:mw_hook_status_bus, MWHookStatusEvent, fn _event ->
        raise "handler failure!"
      end, handler_name: "failing_handler")

      Abxbus.emit(:mw_hook_status_bus, MWHookStatusEvent.new())
      Abxbus.wait_until_idle(:mw_hook_status_bus)

      log = Agent.get(:mw_hook_statuses, & &1)

      # Event lifecycle statuses should be :pending, :started, :completed — never :error
      event_statuses = log |> Enum.filter(fn {type, _} -> type == :event end) |> Enum.map(fn {_, s} -> s end)
      assert :error not in event_statuses,
             "Event lifecycle statuses should never include :error, got: #{inspect(event_statuses)}"
      assert event_statuses == [:pending, :started, :completed]

      # The :error status appears only in result changes, not event changes
      result_statuses = log |> Enum.filter(fn {type, _} -> type == :result end) |> Enum.map(fn {_, s} -> s end)
      assert :error in result_statuses,
             "Handler result should have :error status, got: #{inspect(result_statuses)}"

      Abxbus.stop(:mw_hook_status_bus, clear: true)
      Agent.stop(agent)
    end
  end

  describe "middleware hooks cover string and wildcard handler patterns" do
    test "middleware hooks cover string and wildcard handler patterns" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_handler_patterns)
      {:ok, results_agent} = Agent.start_link(fn -> [] end, name: :mw_sw_results)

      {:ok, _} = Abxbus.start_bus(:mw_sw_bus, middlewares: [HandlerPatternMiddleware])

      # Register handler via string pattern
      Abxbus.on(:mw_sw_bus, "MWStringWildcardEvent", fn _event ->
        Agent.update(:mw_sw_results, fn log -> log ++ [:string_handler] end)
        :ok
      end, handler_name: "string_handler")

      # Register handler via wildcard
      Abxbus.on(:mw_sw_bus, "*", fn _event ->
        Agent.update(:mw_sw_results, fn log -> log ++ [:wildcard_handler] end)
        :ok
      end, handler_name: "wildcard_handler")

      handler_log = Agent.get(:mw_handler_patterns, & &1)

      # Both registrations should have triggered on_bus_handlers_change
      registered_names = handler_log
                         |> Enum.filter(fn {_name, reg?} -> reg? end)
                         |> Enum.map(fn {name, _} -> name end)

      assert "string_handler" in registered_names,
             "String handler registration should be observed, got: #{inspect(registered_names)}"
      assert "wildcard_handler" in registered_names,
             "Wildcard handler registration should be observed, got: #{inspect(registered_names)}"

      # Emit event, both handlers should run
      Abxbus.emit(:mw_sw_bus, MWStringWildcardEvent.new())
      Abxbus.wait_until_idle(:mw_sw_bus)

      results = Agent.get(:mw_sw_results, & &1)
      assert :string_handler in results
      assert :wildcard_handler in results

      Abxbus.stop(:mw_sw_bus, clear: true)
      Agent.stop(agent)
      Agent.stop(results_agent)
    end
  end

  describe "middleware event lifecycle monotonic on timeout" do
    test "middleware event lifecycle monotonic on timeout" do
      {:ok, agent} = Agent.start_link(fn -> [] end, name: :mw_monotonic)

      {:ok, _} = Abxbus.start_bus(:mw_monotonic_bus,
        middlewares: [MonotonicMiddleware],
        event_timeout: 0.3
      )

      Abxbus.on(:mw_monotonic_bus, MWMonotonicEvent, fn _event ->
        Process.sleep(500)
        :ok
      end, handler_name: "slow_handler")

      Abxbus.emit(:mw_monotonic_bus, MWMonotonicEvent.new())
      # Wait enough for timeout + cleanup
      Process.sleep(800)
      Abxbus.wait_until_idle(:mw_monotonic_bus)

      log = Agent.get(:mw_monotonic, & &1)

      # Status transitions should be monotonically ordered: pending -> started -> completed
      # No status reversal should occur
      status_order = %{pending: 0, started: 1, completed: 2}
      indices = Enum.map(log, fn s -> Map.get(status_order, s, -1) end)
      # Filter to only known statuses
      known_indices = Enum.filter(indices, &(&1 >= 0))

      # Each successive index should be >= the previous (monotonic)
      pairs = Enum.zip(known_indices, Enum.drop(known_indices, 1))
      for {a, b} <- pairs do
        assert a <= b,
               "Status transitions should be monotonic, got: #{inspect(log)}"
      end

      # Should see at least pending and started
      assert :pending in log, "Should see :pending status"
      assert :started in log, "Should see :started status"
      assert :completed in log, "Should see :completed status, got: #{inspect(log)}"

      Abxbus.stop(:mw_monotonic_bus, clear: true)
      Agent.stop(agent)
    end
  end
end

defmodule Abxbus.EventbusContextTest do
  @moduledoc """
  Tests for handler context functions: current_event_id, current_handler_id,
  in_handler_context?, current_bus!, and context isolation between events
  and through nested emit.

  Port of tests/test_eventbus_dispatch_contextvars.py — adapted for
  Elixir's process dictionary context.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(CtxEventIdEvent)
  defevent(CtxHandlerIdEvent)
  defevent(CtxInHandlerEvent)
  defevent(CtxIsolationEvent, label: nil)
  defevent(CtxNestedParentEvent)
  defevent(CtxNestedChildEvent)
  defevent(CtxOutsideEvent)

  describe "current_event_id" do
    test "current_event_id returns event ID inside handler" do
      {:ok, _} = Abxbus.start_bus(:ctx_eid)

      captured = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:ctx_eid, CtxEventIdEvent, fn _event ->
        eid = Abxbus.current_event_id()
        Agent.update(captured, fn _ -> eid end)
        :ok
      end, handler_name: "eid_handler")

      event = Abxbus.emit(:ctx_eid, CtxEventIdEvent.new())
      Abxbus.await(event)

      captured_id = Agent.get(captured, & &1)
      assert captured_id == event.event_id
    end
  end

  describe "current_handler_id" do
    test "current_handler_id returns handler ID inside handler" do
      {:ok, _} = Abxbus.start_bus(:ctx_hid)

      captured = Agent.start_link(fn -> nil end) |> elem(1)

      handler_entry = Abxbus.on(:ctx_hid, CtxHandlerIdEvent, fn _event ->
        hid = Abxbus.current_handler_id()
        Agent.update(captured, fn _ -> hid end)
        :ok
      end, handler_name: "hid_handler")

      event = Abxbus.emit(:ctx_hid, CtxHandlerIdEvent.new())
      Abxbus.await(event)

      captured_id = Agent.get(captured, & &1)
      assert captured_id == handler_entry.id
    end
  end

  describe "in_handler_context?" do
    test "in_handler_context? returns true inside handler, false outside" do
      {:ok, _} = Abxbus.start_bus(:ctx_inhc)

      # Outside handler: should be false
      assert Abxbus.in_handler_context?() == false

      captured = Agent.start_link(fn -> nil end) |> elem(1)

      Abxbus.on(:ctx_inhc, CtxInHandlerEvent, fn _event ->
        in_ctx = Abxbus.in_handler_context?()
        Agent.update(captured, fn _ -> in_ctx end)
        :ok
      end, handler_name: "inhc_handler")

      event = Abxbus.emit(:ctx_inhc, CtxInHandlerEvent.new())
      Abxbus.await(event)

      # Inside handler: should have been true
      assert Agent.get(captured, & &1) == true
    end
  end

  describe "handler context isolation" do
    test "handler context isolated between events" do
      {:ok, _} = Abxbus.start_bus(:ctx_iso)

      captured = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:ctx_iso, CtxIsolationEvent, fn _event ->
        eid = Abxbus.current_event_id()
        Agent.update(captured, &(&1 ++ [eid]))
        :ok
      end, handler_name: "iso_handler")

      e1 = Abxbus.emit(:ctx_iso, CtxIsolationEvent.new(label: "first"))
      Abxbus.await(e1)

      e2 = Abxbus.emit(:ctx_iso, CtxIsolationEvent.new(label: "second"))
      Abxbus.await(e2)

      ids = Agent.get(captured, & &1)
      assert length(ids) == 2
      assert Enum.at(ids, 0) == e1.event_id
      assert Enum.at(ids, 1) == e2.event_id
      assert Enum.at(ids, 0) != Enum.at(ids, 1),
             "Each handler invocation should see its own event_id"
    end
  end

  describe "nested emit context" do
    test "handler context propagates through nested emit" do
      {:ok, _} = Abxbus.start_bus(:ctx_nest,
        event_concurrency: :bus_serial
      )

      captured = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:ctx_nest, CtxNestedParentEvent, fn _event ->
        parent_bus = Abxbus.current_bus!()
        Agent.update(captured, &Map.put(&1, :parent_bus, parent_bus))

        child = Abxbus.emit(:ctx_nest, CtxNestedChildEvent.new())
        Abxbus.await(child)
        "parent_done"
      end, handler_name: "parent_handler")

      Abxbus.on(:ctx_nest, CtxNestedChildEvent, fn _event ->
        child_bus = Abxbus.current_bus!()
        child_eid = Abxbus.current_event_id()
        Agent.update(captured, &Map.merge(&1, %{
          child_bus: child_bus,
          child_eid: child_eid
        }))
        "child_done"
      end, handler_name: "child_handler")

      parent = Abxbus.emit(:ctx_nest, CtxNestedParentEvent.new())
      Abxbus.wait_until_idle(:ctx_nest)

      state = Agent.get(captured, & &1)

      # Both handlers should see the same bus
      assert state.parent_bus == :ctx_nest
      assert state.child_bus == :ctx_nest

      # Child handler should see its own event_id (not the parent's)
      assert state.child_eid != nil
      assert state.child_eid != parent.event_id
    end
  end

  describe "context outside handler" do
    test "context not available outside handler" do
      # current_event_id returns nil outside handler
      assert Abxbus.current_event_id() == nil

      # current_handler_id returns nil outside handler
      assert Abxbus.current_handler_id() == nil

      # current_bus! raises outside handler
      assert_raise RuntimeError, fn ->
        Abxbus.current_bus!()
      end
    end
  end

  describe "context through forwarding" do
    defevent(CtxFwdEvent)

    test "context propagates through event forwarding" do
      {:ok, _} = Abxbus.start_bus(:ctx_fwd1)
      {:ok, _} = Abxbus.start_bus(:ctx_fwd2)

      captured_bus = Agent.start_link(fn -> nil end) |> elem(1)

      # Forward all events from bus1 to bus2 via wildcard
      Abxbus.on(:ctx_fwd1, "*", fn e -> Abxbus.emit(:ctx_fwd2, e) end,
        handler_name: "fwd_1_2")

      # Handler on bus2 captures current_bus!()
      Abxbus.on(:ctx_fwd2, CtxFwdEvent, fn _event ->
        bus = Abxbus.current_bus!()
        Agent.update(captured_bus, fn _ -> bus end)
        :ok
      end, handler_name: "bus2_handler")

      Abxbus.emit(:ctx_fwd1, CtxFwdEvent.new())
      Abxbus.wait_until_idle(:ctx_fwd1)
      Abxbus.wait_until_idle(:ctx_fwd2)

      assert Agent.get(captured_bus, & &1) == :ctx_fwd2

      Abxbus.stop(:ctx_fwd1, clear: true)
      Abxbus.stop(:ctx_fwd2, clear: true)
    end
  end

  describe "context isolation with concurrent events" do
    defevent(CtxConcEvent, label: nil)

    test "context isolated between concurrent events" do
      {:ok, _} = Abxbus.start_bus(:ctx_conc, event_concurrency: :parallel)

      captured_ids = Agent.start_link(fn -> [] end) |> elem(1)

      Abxbus.on(:ctx_conc, CtxConcEvent, fn _event ->
        eid = Abxbus.current_event_id()
        Process.sleep(20)
        Agent.update(captured_ids, &(&1 ++ [eid]))
        :ok
      end, handler_name: "conc_handler")

      e1 = Abxbus.emit(:ctx_conc, CtxConcEvent.new(label: "first"))
      e2 = Abxbus.emit(:ctx_conc, CtxConcEvent.new(label: "second"))

      Abxbus.wait_until_idle(:ctx_conc)

      ids = Agent.get(captured_ids, & &1)
      assert length(ids) == 2
      assert e1.event_id in ids
      assert e2.event_id in ids
      assert Enum.at(ids, 0) != Enum.at(ids, 1),
             "Each concurrent handler should see its own event_id"

      Abxbus.stop(:ctx_conc, clear: true)
    end
  end
end

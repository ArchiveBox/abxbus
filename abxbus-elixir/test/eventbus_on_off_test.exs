defmodule Abxbus.EventbusOnOffTest do
  @moduledoc """
  Tests for handler registration and deregistration.

  Port of tests/test_eventbus_on_off.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(OOStoreEvent)
  defevent(OORmTypeEvent)
  defevent(OORmIdEvent)
  defevent(OORmFnEvent)
  defevent(OONameEvent)
  defevent(OOFieldsEvent)

  describe "on stores handler" do
    test "on stores handler entry with correct id" do
      {:ok, _} = Abxbus.start_bus(:oo_store)

      {:ok, agent} = Agent.start_link(fn -> 0 end)

      entry = Abxbus.on(:oo_store, OOStoreEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end)

      assert is_binary(entry.id)
      assert byte_size(entry.id) > 0

      Abxbus.emit(:oo_store, OOStoreEvent.new())
      Abxbus.wait_until_idle(:oo_store)

      assert Agent.get(agent, & &1) == 1
    end
  end

  describe "off removes handlers" do
    test "off removes handler by event type" do
      {:ok, _} = Abxbus.start_bus(:oo_rm_type)

      {:ok, agent} = Agent.start_link(fn -> 0 end)

      Abxbus.on(:oo_rm_type, OORmTypeEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end, handler_name: "h1")

      Abxbus.on(:oo_rm_type, OORmTypeEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end, handler_name: "h2")

      Abxbus.off(:oo_rm_type, OORmTypeEvent)

      Abxbus.emit(:oo_rm_type, OORmTypeEvent.new())
      Abxbus.wait_until_idle(:oo_rm_type)

      assert Agent.get(agent, & &1) == 0,
             "No handlers should run after off by event type"
    end

    test "off removes handler by handler_id" do
      {:ok, _} = Abxbus.start_bus(:oo_rm_id)

      {:ok, agent} = Agent.start_link(fn -> 0 end)

      entry = Abxbus.on(:oo_rm_id, OORmIdEvent, fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end)

      Abxbus.off(:oo_rm_id, handler_id: entry.id)

      Abxbus.emit(:oo_rm_id, OORmIdEvent.new())
      Abxbus.wait_until_idle(:oo_rm_id)

      assert Agent.get(agent, & &1) == 0,
             "Handler should not run after off by handler_id"
    end

    test "off removes handler by function reference" do
      {:ok, _} = Abxbus.start_bus(:oo_rm_fn)

      {:ok, agent} = Agent.start_link(fn -> 0 end)

      handler_fn = fn _e ->
        Agent.update(agent, &(&1 + 1))
        :ok
      end

      Abxbus.on(:oo_rm_fn, OORmFnEvent, handler_fn)

      Abxbus.off(:oo_rm_fn, OORmFnEvent, handler_fn)

      Abxbus.emit(:oo_rm_fn, OORmFnEvent.new())
      Abxbus.wait_until_idle(:oo_rm_fn)

      assert Agent.get(agent, & &1) == 0,
             "Handler should not run after off by function reference"
    end
  end

  describe "handler metadata" do
    test "handler_name stored in entry" do
      {:ok, _} = Abxbus.start_bus(:oo_name)

      entry = Abxbus.on(:oo_name, OONameEvent, fn _e -> :ok end,
        handler_name: "my_handler")

      assert entry.handler_name == "my_handler"
    end

    test "on returns EventHandler struct with all fields" do
      {:ok, _} = Abxbus.start_bus(:oo_fields)

      entry = Abxbus.on(:oo_fields, OOFieldsEvent, fn _e -> :ok end,
        handler_name: "full_check")

      assert %Abxbus.EventHandler{} = entry
      assert is_binary(entry.id)
      assert entry.event_pattern == OOFieldsEvent
      assert is_function(entry.handler)
      assert entry.handler_name == "full_check"
    end
  end
end

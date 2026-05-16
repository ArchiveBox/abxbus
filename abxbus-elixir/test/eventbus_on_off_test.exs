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

  describe "deterministic handler IDs" do
    test "handler_id is deterministic uuidv5 hash of metadata" do
      id =
        Abxbus.EventHandler.compute_handler_id(
          "018f8e40-1234-7000-8000-000000001234",
          "pkg.module.handler",
          "~/project/app.py:123",
          "2025-01-02T03:04:05.678901000Z",
          "StandaloneEvent"
        )

      assert id == "19ea9fe8-cfbe-541e-8a35-2579e4e9efff"
    end

    test "compute_handler_id matches across same metadata" do
      args = [
        "018f8e40-1234-7000-8000-000000001234",
        "pkg.module.handler",
        "~/project/app.py:123",
        "2025-01-02T03:04:05.678901000Z",
        "StandaloneEvent"
      ]

      id1 = apply(Abxbus.EventHandler, :compute_handler_id, args)
      id2 = apply(Abxbus.EventHandler, :compute_handler_id, args)

      assert id1 == id2
      assert is_binary(id1)
    end

    test "compute_handler_id differs for different metadata" do
      id1 =
        Abxbus.EventHandler.compute_handler_id(
          "018f8e40-aaaa-7000-8000-000000000001",
          "module.handler_one",
          "~/project/a.py:1",
          "2025-01-02T03:04:05.678901000Z",
          "EventA"
        )

      id2 =
        Abxbus.EventHandler.compute_handler_id(
          "018f8e40-bbbb-7000-8000-000000000002",
          "module.handler_two",
          "~/project/b.py:2",
          "2025-01-02T03:04:05.678901000Z",
          "EventB"
        )

      assert id1 != id2
    end
  end
end

defmodule Abxbus.EventBusFindTest do
  @moduledoc """
  Tests for the find() query mechanism: past/future search windows,
  child_of filtering, where predicates, and cross-bus lineage.

  Port of tests/test_eventbus_find.py (forwarded lineage cases).
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(FindParentEvent, target_id: nil)
  defevent(FindChildEvent, target_id: nil)
  defevent(FindFutureEvent)

  describe "find past events" do
    test "finds a completed event by type" do
      {:ok, _} = Abxbus.start_bus(:find1)

      Abxbus.on(:find1, FindParentEvent, fn _event -> :ok end)

      event = Abxbus.emit(:find1, FindParentEvent.new(target_id: "abc"))
      Abxbus.wait_until_idle(:find1)

      found = Abxbus.find(FindParentEvent, past: true, event_id: event.event_id)
      assert found != nil
      assert found.event_id == event.event_id
    end

    test "finds nothing when no match" do
      defevent(NeverEmittedEvent)
      found = Abxbus.find(NeverEmittedEvent, past: true, future: false)
      assert found == nil
    end
  end

  describe "find with child_of" do
    test "finds child events by parent" do
      {:ok, _} = Abxbus.start_bus(:find_co, event_concurrency: :bus_serial)

      Abxbus.on(:find_co, FindParentEvent, fn _event ->
        child = Abxbus.emit(:find_co, FindChildEvent.new(target_id: "child_data"))
        Abxbus.await(child)
        :ok
      end)

      Abxbus.on(:find_co, FindChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:find_co, FindParentEvent.new())
      Abxbus.wait_until_idle(:find_co)

      found = Abxbus.find(FindChildEvent, child_of: parent, past: true)
      assert found != nil
      assert found.event_parent_id == parent.event_id
    end
  end

  describe "find with where predicate" do
    test "custom filter function works" do
      {:ok, _} = Abxbus.start_bus(:find_w)

      Abxbus.on(:find_w, FindParentEvent, fn _event -> :ok end)

      _ev1 = Abxbus.emit(:find_w, FindParentEvent.new(target_id: "match_me"))
      _ev2 = Abxbus.emit(:find_w, FindParentEvent.new(target_id: "not_this"))

      Abxbus.wait_until_idle(:find_w)

      found = Abxbus.find(FindParentEvent,
        past: true,
        where: fn event -> event.target_id == "match_me" end
      )

      assert found != nil
      assert found.target_id == "match_me"
    end
  end

  describe "find future events" do
    test "waits for future event and returns it" do
      {:ok, _} = Abxbus.start_bus(:find_fut)

      Abxbus.on(:find_fut, FindFutureEvent, fn _event -> :ok end)

      # Start a finder that will wait
      finder_task =
        Task.async(fn ->
          Abxbus.find(FindFutureEvent, past: false, future: 5.0)
        end)

      # Give finder time to register
      Process.sleep(10)

      # Emit the event
      event = Abxbus.emit(:find_fut, FindFutureEvent.new())

      found = Task.await(finder_task, 6000)
      assert found != nil
      assert found.event_id == event.event_id
    end

    test "returns nil on future timeout" do
      found = Abxbus.find(FindFutureEvent, past: false, future: 0.01)
      assert found == nil
    end
  end

  describe "cross-bus lineage find" do
    test "child_of works across forwarded buses" do
      {:ok, _} = Abxbus.start_bus(:find_main)
      {:ok, _} = Abxbus.start_bus(:find_auth)

      # Forward ParentEvent from main to auth
      Abxbus.on(:find_main, "*", fn e -> Abxbus.emit(:find_auth, e) end, handler_name: "fwd")

      Abxbus.on(:find_auth, FindParentEvent, fn event ->
        # Dispatch child on auth bus
        child = Abxbus.emit(:find_auth, FindChildEvent.new(target_id: "from_auth"))
        Abxbus.await(child)
        :ok
      end)

      Abxbus.on(:find_auth, FindChildEvent, fn _event -> :ok end)

      parent = Abxbus.emit(:find_main, FindParentEvent.new())

      Abxbus.wait_until_idle(:find_main)
      Abxbus.wait_until_idle(:find_auth)

      # Find child on auth bus using parent from main bus
      found = Abxbus.find(FindChildEvent,
        child_of: parent,
        past: 5
      )

      assert found != nil
      assert found.event_parent_id == parent.event_id
    end
  end
end

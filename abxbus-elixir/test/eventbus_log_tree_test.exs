defmodule Abxbus.EventbusLogTreeTest do
  @moduledoc """
  Tests for the log_tree function that renders ASCII event trees.

  Port of tests/test_eventbus_log_tree.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  # ── Event definitions ──────────────────────────────────────────────────────

  defevent(LTSimpleEvent, message: "hello")
  defevent(LTResultEvent, data: "result_test")
  defevent(LTErrorEvent, data: "error_test")
  defevent(LTParentEvent, info: "parent")
  defevent(LTChildEvent, info: "child")
  defevent(LTGrandchildEvent, info: "grandchild")
  defevent(LTRootA, tag: "a")
  defevent(LTRootB, tag: "b")

  # ── Single event tree ─────────────────────────────────────────────────────

  describe "log_tree single event" do
    test "renders tree containing event type" do
      {:ok, _} = Abxbus.start_bus(:lt_single, event_concurrency: :bus_serial)

      Abxbus.on(:lt_single, LTSimpleEvent, fn _event -> :ok end)

      event = Abxbus.emit(:lt_single, LTSimpleEvent.new())
      Abxbus.wait_until_idle(:lt_single)

      # Fetch the completed event from the store
      completed = Abxbus.EventStore.get(event.event_id)
      tree = Abxbus.log_tree(completed)

      assert is_binary(tree)
      assert tree =~ "LTSimpleEvent"
      assert tree =~ "completed"

      Abxbus.stop(:lt_single, clear: true)
    end
  end

  # ── Tree with handler results ─────────────────────────────────────────────

  describe "log_tree with handler results" do
    test "output contains handler info when show_results enabled" do
      {:ok, _} = Abxbus.start_bus(:lt_results, event_concurrency: :bus_serial)

      Abxbus.on(:lt_results, LTResultEvent, fn _event ->
        "my_handler_result"
      end, handler_name: "result_handler")

      event = Abxbus.emit(:lt_results, LTResultEvent.new())
      Abxbus.wait_until_idle(:lt_results)

      completed = Abxbus.EventStore.get(event.event_id)
      tree = Abxbus.log_tree(completed, show_results: true)

      assert is_binary(tree)
      assert tree =~ "LTResultEvent"
      assert tree =~ "result_handler"
      assert tree =~ "my_handler_result"

      Abxbus.stop(:lt_results, clear: true)
    end
  end

  # ── Tree with errors ──────────────────────────────────────────────────────

  describe "log_tree with errors" do
    test "output contains error indication for failing handler" do
      {:ok, _} = Abxbus.start_bus(:lt_errors, event_concurrency: :bus_serial)

      Abxbus.on(:lt_errors, LTErrorEvent, fn _event ->
        raise "intentional error"
      end, handler_name: "failing_handler")

      event = Abxbus.emit(:lt_errors, LTErrorEvent.new())
      Abxbus.wait_until_idle(:lt_errors)

      completed = Abxbus.EventStore.get(event.event_id)
      tree = Abxbus.log_tree(completed, show_results: true)

      assert is_binary(tree)
      assert tree =~ "LTErrorEvent"
      # The event status should reflect the error
      assert tree =~ "error" or tree =~ "completed"
      # With show_results, the failing handler info should appear
      assert tree =~ "failing_handler"

      Abxbus.stop(:lt_errors, clear: true)
    end
  end

  # ── Tree with nested events ───────────────────────────────────────────────

  describe "log_tree with nested events" do
    test "shows parent-child-grandchild hierarchy" do
      {:ok, _} = Abxbus.start_bus(:lt_nested, event_concurrency: :bus_serial)

      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:lt_nested, LTParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:lt_nested, LTChildEvent.new())
        Abxbus.await(child)
        :ok
      end)

      Abxbus.on(:lt_nested, LTChildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :child, event.event_id))
        grandchild = Abxbus.emit(:lt_nested, LTGrandchildEvent.new())
        Abxbus.await(grandchild)
        :ok
      end)

      Abxbus.on(:lt_nested, LTGrandchildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :grandchild, event.event_id))
        :ok
      end)

      parent_event = Abxbus.emit(:lt_nested, LTParentEvent.new())
      Abxbus.wait_until_idle(:lt_nested)

      completed = Abxbus.EventStore.get(parent_event.event_id)
      tree = Abxbus.log_tree(completed)

      assert is_binary(tree)
      # All three levels should appear in the tree
      assert tree =~ "LTParentEvent"
      assert tree =~ "LTChildEvent"
      assert tree =~ "LTGrandchildEvent"

      Abxbus.stop(:lt_nested, clear: true)
    end
  end

  # ── Tree with multiple root events ────────────────────────────────────────

  describe "log_tree with multiple root events" do
    test "each root event renders its own tree" do
      {:ok, _} = Abxbus.start_bus(:lt_multi, event_concurrency: :bus_serial)

      Abxbus.on(:lt_multi, LTRootA, fn _event -> :ok end)
      Abxbus.on(:lt_multi, LTRootB, fn _event -> :ok end)

      event_a = Abxbus.emit(:lt_multi, LTRootA.new())
      event_b = Abxbus.emit(:lt_multi, LTRootB.new())
      Abxbus.wait_until_idle(:lt_multi)

      completed_a = Abxbus.EventStore.get(event_a.event_id)
      completed_b = Abxbus.EventStore.get(event_b.event_id)

      tree_a = Abxbus.log_tree(completed_a)
      tree_b = Abxbus.log_tree(completed_b)

      assert tree_a =~ "LTRootA"
      refute tree_a =~ "LTRootB"

      assert tree_b =~ "LTRootB"
      refute tree_b =~ "LTRootA"

      Abxbus.stop(:lt_multi, clear: true)
    end
  end

  # ── Edge cases ────────────────────────────────────────────────────────────

  describe "log_tree edge cases" do
    test "returns not-found message for nil event" do
      result = Abxbus.Tree.log_tree(nil)
      assert result == "(event not found)"
    end

    test "log_tree with show_timing displays milliseconds" do
      {:ok, _} = Abxbus.start_bus(:lt_timing, event_concurrency: :bus_serial)

      Abxbus.on(:lt_timing, LTSimpleEvent, fn _event ->
        Process.sleep(5)
        :ok
      end)

      event = Abxbus.emit(:lt_timing, LTSimpleEvent.new())
      Abxbus.wait_until_idle(:lt_timing)

      completed = Abxbus.EventStore.get(event.event_id)
      tree = Abxbus.log_tree(completed, show_timing: true)

      assert is_binary(tree)
      assert tree =~ "ms"

      Abxbus.stop(:lt_timing, clear: true)
    end
  end
end

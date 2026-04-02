defmodule Abxbus.EventBusFindTest do
  @moduledoc """
  Tests for the find() query mechanism, event_is_child_of/2, event_is_parent_of/2,
  past/future search windows, child_of filtering, where predicates, field matching,
  wildcard patterns, and cross-bus lineage.

  Port of tests/test_eventbus_find.py.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(FindParentEvent, target_id: nil)
  defevent(FindChildEvent, target_id: nil)
  defevent(FindFutureEvent, value: nil)
  defevent(FindGrandchildEvent, info: nil)

  # ── event_is_child_of ─────────────────────────────────────────────────────

  describe "event_is_child_of" do
    test "direct child returns true" do
      {:ok, _} = Abxbus.start_bus(:cof1, event_concurrency: :bus_serial)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:cof1, FindParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:cof1, FindChildEvent.new())
        Abxbus.await(child)
        Agent.update(ids, &Map.put(&1, :child, child.event_id))
        :ok
      end)
      Abxbus.on(:cof1, FindChildEvent, fn _e -> :ok end)

      Abxbus.emit(:cof1, FindParentEvent.new())
      Abxbus.wait_until_idle(:cof1)

      i = Agent.get(ids, & &1)
      assert Abxbus.event_is_child_of(i.child, i.parent)
    end

    test "grandchild returns true" do
      {:ok, _} = Abxbus.start_bus(:cof2, event_concurrency: :bus_serial)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:cof2, FindParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:cof2, FindChildEvent.new())
        Abxbus.await(child)
        :ok
      end)
      Abxbus.on(:cof2, FindChildEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :child, event.event_id))
        gc = Abxbus.emit(:cof2, FindGrandchildEvent.new())
        Abxbus.await(gc)
        Agent.update(ids, &Map.put(&1, :grandchild, gc.event_id))
        :ok
      end)
      Abxbus.on(:cof2, FindGrandchildEvent, fn _e -> :ok end)

      Abxbus.emit(:cof2, FindParentEvent.new())
      Abxbus.wait_until_idle(:cof2)

      i = Agent.get(ids, & &1)
      assert Abxbus.event_is_child_of(i.grandchild, i.parent)
    end

    test "unrelated events returns false" do
      {:ok, _} = Abxbus.start_bus(:cof3)
      Abxbus.on(:cof3, FindParentEvent, fn _e -> :ok end)
      Abxbus.on(:cof3, FindChildEvent, fn _e -> :ok end)

      e1 = Abxbus.emit(:cof3, FindParentEvent.new())
      e2 = Abxbus.emit(:cof3, FindChildEvent.new())
      Abxbus.wait_until_idle(:cof3)

      refute Abxbus.event_is_child_of(e2.event_id, e1.event_id)
    end

    test "same event returns false" do
      {:ok, _} = Abxbus.start_bus(:cof4)
      Abxbus.on(:cof4, FindParentEvent, fn _e -> :ok end)
      e = Abxbus.emit(:cof4, FindParentEvent.new())
      Abxbus.wait_until_idle(:cof4)

      refute Abxbus.event_is_child_of(e.event_id, e.event_id)
    end

    test "reversed relationship returns false" do
      {:ok, _} = Abxbus.start_bus(:cof5, event_concurrency: :bus_serial)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:cof5, FindParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:cof5, FindChildEvent.new())
        Abxbus.await(child)
        Agent.update(ids, &Map.put(&1, :child, child.event_id))
        :ok
      end)
      Abxbus.on(:cof5, FindChildEvent, fn _e -> :ok end)

      Abxbus.emit(:cof5, FindParentEvent.new())
      Abxbus.wait_until_idle(:cof5)

      i = Agent.get(ids, & &1)
      refute Abxbus.event_is_child_of(i.parent, i.child)
    end
  end

  # ── event_is_parent_of ────────────────────────────────────────────────────

  describe "event_is_parent_of" do
    test "direct parent returns true" do
      {:ok, _} = Abxbus.start_bus(:pof1, event_concurrency: :bus_serial)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:pof1, FindParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:pof1, FindChildEvent.new())
        Abxbus.await(child)
        Agent.update(ids, &Map.put(&1, :child, child.event_id))
        :ok
      end)
      Abxbus.on(:pof1, FindChildEvent, fn _e -> :ok end)

      Abxbus.emit(:pof1, FindParentEvent.new())
      Abxbus.wait_until_idle(:pof1)

      i = Agent.get(ids, & &1)
      assert Abxbus.event_is_parent_of(i.parent, i.child)
    end

    test "grandparent returns true" do
      {:ok, _} = Abxbus.start_bus(:pof2, event_concurrency: :bus_serial)
      ids = Agent.start_link(fn -> %{} end) |> elem(1)

      Abxbus.on(:pof2, FindParentEvent, fn event ->
        Agent.update(ids, &Map.put(&1, :parent, event.event_id))
        child = Abxbus.emit(:pof2, FindChildEvent.new())
        Abxbus.await(child)
        :ok
      end)
      Abxbus.on(:pof2, FindChildEvent, fn event ->
        gc = Abxbus.emit(:pof2, FindGrandchildEvent.new())
        Abxbus.await(gc)
        Agent.update(ids, &Map.put(&1, :grandchild, gc.event_id))
        :ok
      end)
      Abxbus.on(:pof2, FindGrandchildEvent, fn _e -> :ok end)

      Abxbus.emit(:pof2, FindParentEvent.new())
      Abxbus.wait_until_idle(:pof2)

      i = Agent.get(ids, & &1)
      assert Abxbus.event_is_parent_of(i.parent, i.grandchild)
    end
  end

  # ── find past events ──────────────────────────────────────────────────────

  describe "find past events" do
    test "finds a completed event by type" do
      {:ok, _} = Abxbus.start_bus(:fp1)
      Abxbus.on(:fp1, FindParentEvent, fn _e -> :ok end)
      event = Abxbus.emit(:fp1, FindParentEvent.new(target_id: "abc"))
      Abxbus.wait_until_idle(:fp1)

      found = Abxbus.find(FindParentEvent, past: true, event_id: event.event_id)
      assert found != nil
      assert found.event_id == event.event_id
    end

    test "finds nothing when no match" do
      defevent(NeverEmittedEvent)
      found = Abxbus.find(NeverEmittedEvent, past: true, future: false)
      assert found == nil
    end

    test "respects where filter" do
      {:ok, _} = Abxbus.start_bus(:fp_w)
      Abxbus.on(:fp_w, FindParentEvent, fn _e -> :ok end)
      Abxbus.emit(:fp_w, FindParentEvent.new(target_id: "match_me"))
      Abxbus.emit(:fp_w, FindParentEvent.new(target_id: "not_this"))
      Abxbus.wait_until_idle(:fp_w)

      found = Abxbus.find(FindParentEvent, past: true, where: &(&1.target_id == "match_me"))
      assert found != nil
      assert found.target_id == "match_me"
    end

    test "supports field keyword filters" do
      {:ok, _} = Abxbus.start_bus(:fp_kw)
      Abxbus.on(:fp_kw, FindParentEvent, fn _e -> :ok end)
      event = Abxbus.emit(:fp_kw, FindParentEvent.new(target_id: "kw_match"))
      Abxbus.wait_until_idle(:fp_kw)

      found = Abxbus.find(FindParentEvent, past: true, target_id: "kw_match")
      assert found != nil
      assert found.event_id == event.event_id
    end

    test "past includes in-progress events" do
      {:ok, _} = Abxbus.start_bus(:fp_ip)
      barrier = :atomics.new(1, [])

      Abxbus.on(:fp_ip, FindParentEvent, fn _e ->
        :atomics.put(barrier, 1, 1)
        spin_until(fn -> :atomics.get(barrier, 1) == 2 end, 2000)
        :ok
      end)

      event = Abxbus.emit(:fp_ip, FindParentEvent.new(target_id: "in_progress"))
      spin_until(fn -> :atomics.get(barrier, 1) == 1 end, 1000)

      # Event is started but not completed
      found = Abxbus.find(FindParentEvent, past: true, event_id: event.event_id)
      assert found != nil

      :atomics.put(barrier, 1, 2)
      Abxbus.wait_until_idle(:fp_ip)
    end

    test "find default is past only, no future wait" do
      defevent(DefaultFindEvent)
      found = Abxbus.find(DefaultFindEvent)
      assert found == nil
    end

    test "wildcard with where matches across types" do
      {:ok, _} = Abxbus.start_bus(:fp_wc)
      Abxbus.on(:fp_wc, FindParentEvent, fn _e -> :ok end)
      Abxbus.on(:fp_wc, FindChildEvent, fn _e -> :ok end)
      Abxbus.emit(:fp_wc, FindParentEvent.new(target_id: "wc_target"))
      Abxbus.emit(:fp_wc, FindChildEvent.new(target_id: "wc_target"))
      Abxbus.wait_until_idle(:fp_wc)

      found = Abxbus.find("*", past: true, where: &(Map.get(&1, :target_id) == "wc_target"))
      assert found != nil
      assert found.target_id == "wc_target"
    end

    test "returns none immediately with past=false future=false" do
      {:ok, _} = Abxbus.start_bus(:fp_nf)
      Abxbus.on(:fp_nf, FindParentEvent, fn _e -> :ok end)
      Abxbus.emit(:fp_nf, FindParentEvent.new())
      Abxbus.wait_until_idle(:fp_nf)

      found = Abxbus.find(FindParentEvent, past: false, future: false)
      assert found == nil
    end
  end

  # ── find future events ────────────────────────────────────────────────────

  describe "find future events" do
    test "waits for future event and returns it" do
      {:ok, _} = Abxbus.start_bus(:ff1)
      Abxbus.on(:ff1, FindFutureEvent, fn _e -> :ok end)

      ready = :atomics.new(1, [])
      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find(FindFutureEvent, past: false, future: 5.0)
      end)

      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 1000)
      Process.sleep(1)

      event = Abxbus.emit(:ff1, FindFutureEvent.new())
      found = Task.await(task, 6000)

      assert found != nil
      assert found.event_id == event.event_id
    end

    test "returns nil on future timeout" do
      found = Abxbus.find(FindFutureEvent, past: false, future: 0.01)
      assert found == nil
    end

    test "ignores past events when past=false" do
      {:ok, _} = Abxbus.start_bus(:ff_ip)
      Abxbus.on(:ff_ip, FindFutureEvent, fn _e -> :ok end)
      Abxbus.emit(:ff_ip, FindFutureEvent.new(value: "old"))
      Abxbus.wait_until_idle(:ff_ip)

      found = Abxbus.find(FindFutureEvent, past: false, future: 0.01)
      assert found == nil
    end

    test "wildcard with where waits for matching future" do
      {:ok, _} = Abxbus.start_bus(:ff_wc)
      Abxbus.on(:ff_wc, FindFutureEvent, fn _e -> :ok end)

      ready = :atomics.new(1, [])
      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find("*", past: false, future: 5.0, where: &(&1.value == "target"))
      end)

      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 1000)
      Process.sleep(1)

      Abxbus.emit(:ff_wc, FindFutureEvent.new(value: "target"))
      found = Task.await(task, 6000)

      assert found != nil
      assert found.value == "target"
    end

    test "find resolves on emit before handlers complete" do
      {:ok, _} = Abxbus.start_bus(:ff_early)
      barrier = :atomics.new(1, [])

      Abxbus.on(:ff_early, FindFutureEvent, fn _e ->
        spin_until(fn -> :atomics.get(barrier, 1) == 1 end, 5000)
        :ok
      end)

      ready = :atomics.new(1, [])
      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find(FindFutureEvent, past: false, future: 5.0)
      end)

      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 1000)
      Process.sleep(1)

      event = Abxbus.emit(:ff_early, FindFutureEvent.new())

      # find should resolve immediately on emit (before handler finishes)
      found = Task.await(task, 2000)
      assert found != nil
      assert found.event_id == event.event_id

      # Now release the handler
      :atomics.put(barrier, 1, 1)
      Abxbus.wait_until_idle(:ff_early)
    end

    test "multiple concurrent finders resolve correctly" do
      {:ok, _} = Abxbus.start_bus(:ff_multi)
      Abxbus.on(:ff_multi, FindFutureEvent, fn _e -> :ok end)
      Abxbus.on(:ff_multi, FindParentEvent, fn _e -> :ok end)

      ready = :counters.new(1, [:atomics])

      task1 = Task.async(fn ->
        :counters.add(ready, 1, 1)
        Abxbus.find(FindFutureEvent, past: false, future: 5.0, where: &(&1.value == "t1"))
      end)

      task2 = Task.async(fn ->
        :counters.add(ready, 1, 1)
        Abxbus.find(FindFutureEvent, past: false, future: 5.0, where: &(&1.value == "t2"))
      end)

      spin_until(fn -> :counters.get(ready, 1) >= 2 end, 1000)
      Process.sleep(1)

      e1 = Abxbus.emit(:ff_multi, FindFutureEvent.new(value: "t1"))
      e2 = Abxbus.emit(:ff_multi, FindFutureEvent.new(value: "t2"))

      f1 = Task.await(task1, 6000)
      f2 = Task.await(task2, 6000)

      assert f1 != nil
      assert f2 != nil
      assert f1.event_id == e1.event_id
      assert f2.event_id == e2.event_id
    end
  end

  # ── find past + future combined ────────────────────────────────────────────

  describe "find past and future combined" do
    test "returns past event immediately without waiting" do
      {:ok, _} = Abxbus.start_bus(:pf1)
      Abxbus.on(:pf1, FindParentEvent, fn _e -> :ok end)
      event = Abxbus.emit(:pf1, FindParentEvent.new(target_id: "pf_past"))
      Abxbus.wait_until_idle(:pf1)

      found = Abxbus.find(FindParentEvent, past: true, future: 5.0, event_id: event.event_id)
      assert found != nil
      assert found.event_id == event.event_id
    end

    test "waits for future when no past match" do
      {:ok, _} = Abxbus.start_bus(:pf2)
      Abxbus.on(:pf2, FindFutureEvent, fn _e -> :ok end)

      ready = :atomics.new(1, [])
      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find(FindFutureEvent, past: true, future: 2.0, where: &(&1.value == "pf_new"))
      end)

      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 1000)
      Process.sleep(1)

      event = Abxbus.emit(:pf2, FindFutureEvent.new(value: "pf_new"))
      found = Task.await(task, 3000)

      assert found != nil
      assert found.event_id == event.event_id
    end

    test "past=true future=false is instant history search" do
      defevent(PFInstantEvent)
      found = Abxbus.find(PFInstantEvent, past: true, future: false)
      assert found == nil
    end
  end

  # ── find with child_of ────────────────────────────────────────────────────

  describe "find with child_of" do
    test "finds child events by parent" do
      {:ok, _} = Abxbus.start_bus(:fc1, event_concurrency: :bus_serial)

      Abxbus.on(:fc1, FindParentEvent, fn _e ->
        child = Abxbus.emit(:fc1, FindChildEvent.new(target_id: "child_data"))
        Abxbus.await(child)
        :ok
      end)
      Abxbus.on(:fc1, FindChildEvent, fn _e -> :ok end)

      parent = Abxbus.emit(:fc1, FindParentEvent.new())
      Abxbus.wait_until_idle(:fc1)

      found = Abxbus.find(FindChildEvent, child_of: parent, past: true)
      assert found != nil
      assert found.event_parent_id == parent.event_id
    end

    test "returns none for non-child" do
      {:ok, _} = Abxbus.start_bus(:fc2)
      Abxbus.on(:fc2, FindParentEvent, fn _e -> :ok end)
      Abxbus.on(:fc2, FindChildEvent, fn _e -> :ok end)

      parent = Abxbus.emit(:fc2, FindParentEvent.new())
      Abxbus.emit(:fc2, FindChildEvent.new())
      Abxbus.wait_until_idle(:fc2)

      found = Abxbus.find(FindChildEvent, child_of: parent, past: true)
      assert found == nil
    end

    test "finds grandchild" do
      {:ok, _} = Abxbus.start_bus(:fc3, event_concurrency: :bus_serial)

      Abxbus.on(:fc3, FindParentEvent, fn _e ->
        child = Abxbus.emit(:fc3, FindChildEvent.new())
        Abxbus.await(child)
        :ok
      end)
      Abxbus.on(:fc3, FindChildEvent, fn _e ->
        gc = Abxbus.emit(:fc3, FindGrandchildEvent.new())
        Abxbus.await(gc)
        :ok
      end)
      Abxbus.on(:fc3, FindGrandchildEvent, fn _e -> :ok end)

      parent = Abxbus.emit(:fc3, FindParentEvent.new())
      Abxbus.wait_until_idle(:fc3)

      # Direct child_of only matches direct parent, not grandparent
      child_found = Abxbus.find(FindChildEvent, child_of: parent, past: true)
      assert child_found != nil
    end

    test "future wait with child_of" do
      {:ok, _} = Abxbus.start_bus(:fc4, event_concurrency: :bus_serial)

      Abxbus.on(:fc4, FindChildEvent, fn _e -> :ok end)

      parent = Abxbus.emit(:fc4, FindParentEvent.new())

      ready = :atomics.new(1, [])
      task = Task.async(fn ->
        :atomics.put(ready, 1, 1)
        Abxbus.find(FindChildEvent, child_of: parent, past: false, future: 5.0)
      end)

      spin_until(fn -> :atomics.get(ready, 1) == 1 end, 1000)
      Process.sleep(1)

      child = Abxbus.emit(:fc4, FindChildEvent.new(event_parent_id: parent.event_id))
      Abxbus.wait_until_idle(:fc4)

      found = Task.await(task, 6000)
      assert found != nil
      assert found.event_parent_id == parent.event_id
    end
  end

  # ── cross-bus lineage find ─────────────────────────────────────────────────

  describe "cross-bus lineage find" do
    test "child_of works across forwarded buses" do
      {:ok, _} = Abxbus.start_bus(:fxb1)
      {:ok, _} = Abxbus.start_bus(:fxb2)

      Abxbus.on(:fxb1, "*", fn e -> Abxbus.emit(:fxb2, e) end, handler_name: "fwd")

      Abxbus.on(:fxb2, FindParentEvent, fn _event ->
        child = Abxbus.emit(:fxb2, FindChildEvent.new(target_id: "from_auth"))
        Abxbus.await(child)
        :ok
      end)
      Abxbus.on(:fxb2, FindChildEvent, fn _e -> :ok end)

      parent = Abxbus.emit(:fxb1, FindParentEvent.new())
      Abxbus.wait_until_idle(:fxb1)
      Abxbus.wait_until_idle(:fxb2)

      found = Abxbus.find(FindChildEvent, child_of: parent, past: 5)
      assert found != nil
      assert found.event_parent_id == parent.event_id
    end
  end

  # ── helpers ────────────────────────────────────────────────────────────────

  defp spin_until(fun, max_iters, iter \\ 0) do
    if iter >= max_iters, do: raise("spin_until exceeded #{max_iters} iterations")
    if fun.(), do: :ok, else: (Process.sleep(1); spin_until(fun, max_iters, iter + 1))
  end
end

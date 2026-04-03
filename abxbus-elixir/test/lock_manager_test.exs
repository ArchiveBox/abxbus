defmodule Abxbus.LockManagerTest do
  @moduledoc """
  Tests for the LockManager module that controls event and handler concurrency.

  Port of tests/test_lock_manager.py.
  """

  use ExUnit.Case, async: false
  alias Abxbus.LockManager

  # ── Event concurrency resolution ──────────────────────────────────────────

  describe "resolve_event_concurrency" do
    test "uses event override when set" do
      event = %{event_concurrency: :parallel}
      bus_config = %{event_concurrency: :bus_serial}

      assert LockManager.resolve_event_concurrency(event, bus_config) == :parallel
    end

    test "falls back to bus config when event override is nil" do
      event = %{event_concurrency: nil}
      bus_config = %{event_concurrency: :global_serial}

      assert LockManager.resolve_event_concurrency(event, bus_config) == :global_serial
    end

    test "falls back to default :bus_serial when both are nil" do
      event = %{event_concurrency: nil}
      bus_config = %{}

      assert LockManager.resolve_event_concurrency(event, bus_config) == :bus_serial
    end
  end

  # ── Handler concurrency resolution ────────────────────────────────────────

  describe "resolve_handler_concurrency" do
    test "uses event override when set" do
      event = %{event_handler_concurrency: :serial}
      bus_config = %{event_handler_concurrency: :parallel}

      assert LockManager.resolve_handler_concurrency(event, bus_config) == :serial
    end

    test "falls back to bus config when event override is nil" do
      event = %{event_handler_concurrency: nil}
      bus_config = %{event_handler_concurrency: :serial}

      assert LockManager.resolve_handler_concurrency(event, bus_config) == :serial
    end

    test "falls back to default :parallel when both are nil" do
      event = %{event_handler_concurrency: nil}
      bus_config = %{}

      assert LockManager.resolve_handler_concurrency(event, bus_config) == :parallel
    end
  end

  # ── Event timeout resolution ──────────────────────────────────────────────

  describe "resolve_event_timeout" do
    test "uses event override when set" do
      event = %{event_timeout: 5.0}
      bus_config = %{event_timeout: 30.0}

      assert LockManager.resolve_event_timeout(event, bus_config) == 5.0
    end

    test "falls back to bus config when event override is nil" do
      event = %{event_timeout: nil}
      bus_config = %{event_timeout: 30.0}

      assert LockManager.resolve_event_timeout(event, bus_config) == 30.0
    end

    test "returns nil when neither event nor bus config has timeout" do
      event = %{event_timeout: nil}
      bus_config = %{}

      assert LockManager.resolve_event_timeout(event, bus_config) == nil
    end
  end

  # ── Handler completion resolution ─────────────────────────────────────────

  describe "resolve_handler_completion" do
    test "uses event override when set" do
      event = %{event_handler_completion: :first}
      bus_config = %{event_handler_completion: :all}

      assert LockManager.resolve_handler_completion(event, bus_config) == :first
    end

    test "falls back to bus config when event override is nil" do
      event = %{event_handler_completion: nil}
      bus_config = %{event_handler_completion: :first}

      assert LockManager.resolve_handler_completion(event, bus_config) == :first
    end

    test "falls back to default :all when both are nil" do
      event = %{event_handler_completion: nil}
      bus_config = %{}

      assert LockManager.resolve_handler_completion(event, bus_config) == :all
    end
  end

  # ── Global serial lock ────────────────────────────────────────────────────

  describe "global serial lock" do
    test "acquire and release" do
      # First acquire should succeed
      assert :ok == LockManager.try_acquire_global()

      # Second acquire from a different process should return :busy
      task =
        Task.async(fn ->
          LockManager.try_acquire_global()
        end)

      assert :busy == Task.await(task)

      # Release the lock
      LockManager.release_global()

      # Spin-wait until the async release is processed and lock is acquirable
      result =
        Enum.reduce_while(1..100, :busy, fn _, _ ->
          Process.sleep(5)
          task2 = Task.async(fn -> LockManager.try_acquire_global() end)
          case Task.await(task2) do
            :ok -> {:halt, :ok}
            :busy -> {:cont, :busy}
          end
        end)

      assert result == :ok, "Lock should be acquirable after release"

      # Clean up: the acquiring task already exited, monitor-based auto-release handles it.
      Process.sleep(10)
    end

    test "blocking acquire waits until lock is released" do
      # Acquire the lock in this process
      assert :ok == LockManager.try_acquire_global()

      # Start a task that will block on acquire
      task =
        Task.async(fn ->
          LockManager.acquire_global()
        end)

      # Give the task time to start waiting
      Process.sleep(20)

      # Release from this process
      LockManager.release_global()

      # The task should now complete with :ok
      assert :ok == Task.await(task, 2000)

      # Clean up: the task process died after completing, auto-releasing the lock
      Process.sleep(10)
    end

    test "lock auto-released when holder process dies" do
      # Spawn a process that acquires the lock then exits
      {pid, ref} =
        spawn_monitor(fn ->
          LockManager.acquire_global()
          # Process exits immediately after acquiring
        end)

      # Wait for the process to die
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        1000 -> flunk("Spawned process did not exit")
      end

      # Allow the DOWN message to be processed by LockManager
      Process.sleep(20)

      # Lock should now be available
      assert :ok == LockManager.try_acquire_global()

      # Clean up
      LockManager.release_global()
      Process.sleep(10)
    end
  end
end

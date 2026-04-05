defmodule Abxbus.EventResultTest do
  use ExUnit.Case, async: false
  import Abxbus.TestEvents

  defevent(ERSimpleEvent)
  defevent(ERErrorEvent)
  defevent(ERMultiEvent)
  defevent(ERFirstEvent)
  defevent(ERFirstNilEvent)
  defevent(EREventResultEvent)
  defevent(ERTimestampEvent)
  defevent(ERHandlerNameEvent)
  defevent(ERHandlerIdEvent)
  defevent(ERNoHandlersEvent)

  describe "handler result storage" do
    test "handler result stored in event_results map" do
      {:ok, _} = Abxbus.start_bus(:er_store)

      Abxbus.on(:er_store, ERSimpleEvent, fn _e -> "hello" end,
        handler_name: "hello_handler")

      event = Abxbus.emit(:er_store, ERSimpleEvent.new())
      Abxbus.wait_until_idle(:er_store)

      stored = Abxbus.EventStore.get(event.event_id)
      assert map_size(stored.event_results) == 1

      result = stored.event_results |> Map.values() |> hd()
      assert result.status == :completed
      assert result.result == "hello"
    end

    test "handler error stored in event_results" do
      {:ok, _} = Abxbus.start_bus(:er_error)

      Abxbus.on(:er_error, ERErrorEvent, fn _e ->
        raise "boom"
      end, handler_name: "error_handler")

      event = Abxbus.emit(:er_error, ERErrorEvent.new())
      Abxbus.wait_until_idle(:er_error)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()
      assert result.status == :error
    end
  end

  describe "result accessors" do
    test "event_results_list returns handler results" do
      {:ok, _} = Abxbus.start_bus(:er_list,
        event_handler_concurrency: :serial)

      Abxbus.on(:er_list, ERMultiEvent, fn _e -> "a" end,
        handler_name: "handler_a")
      Abxbus.on(:er_list, ERMultiEvent, fn _e -> "b" end,
        handler_name: "handler_b")

      event = Abxbus.emit(:er_list, ERMultiEvent.new())
      Abxbus.wait_until_idle(:er_list)

      results = Abxbus.event_results_list(event)
      assert "a" in results
      assert "b" in results
    end

    test "first returns first non-nil result" do
      {:ok, _} = Abxbus.start_bus(:er_first,
        event_handler_completion: :first,
        event_handler_concurrency: :serial)

      Abxbus.on(:er_first, ERFirstEvent, fn _e -> "winner" end,
        handler_name: "winner_handler")

      event = Abxbus.emit(:er_first, ERFirstEvent.new())
      result = Abxbus.first(event)

      assert result == "winner"
    end

    test "first skips nil results" do
      {:ok, _} = Abxbus.start_bus(:er_first_nil,
        event_handler_completion: :first,
        event_handler_concurrency: :serial)

      Abxbus.on(:er_first_nil, ERFirstNilEvent, fn _e -> nil end,
        handler_name: "nil_handler")
      Abxbus.on(:er_first_nil, ERFirstNilEvent, fn _e -> "actual" end,
        handler_name: "actual_handler")

      event = Abxbus.emit(:er_first_nil, ERFirstNilEvent.new())
      result = Abxbus.first(event)

      assert result == "actual"
    end

    test "event_result returns first handler result" do
      {:ok, _} = Abxbus.start_bus(:er_ev_result,
        event_handler_concurrency: :serial)

      Abxbus.on(:er_ev_result, EREventResultEvent, fn _e -> "the_result" end,
        handler_name: "result_handler")

      event = Abxbus.emit(:er_ev_result, EREventResultEvent.new())
      result = Abxbus.event_result(event)

      assert result == "the_result"
    end
  end

  describe "result metadata" do
    test "EventResult has correct timestamps" do
      {:ok, _} = Abxbus.start_bus(:er_ts)

      Abxbus.on(:er_ts, ERTimestampEvent, fn _e -> :ok end,
        handler_name: "ts_handler")

      event = Abxbus.emit(:er_ts, ERTimestampEvent.new())
      Abxbus.wait_until_idle(:er_ts)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.started_at != nil
      assert result.completed_at != nil
      assert result.completed_at >= result.started_at
    end

    test "handler_name appears in EventResult" do
      {:ok, _} = Abxbus.start_bus(:er_hname)

      Abxbus.on(:er_hname, ERHandlerNameEvent, fn _e -> :ok end,
        handler_name: "my_handler")

      event = Abxbus.emit(:er_hname, ERHandlerNameEvent.new())
      Abxbus.wait_until_idle(:er_hname)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.handler_name == "my_handler"
    end

    test "handler metadata includes handler_id" do
      {:ok, _} = Abxbus.start_bus(:er_hid)

      entry = Abxbus.on(:er_hid, ERHandlerIdEvent, fn _e -> :ok end,
        handler_name: "id_handler")

      event = Abxbus.emit(:er_hid, ERHandlerIdEvent.new())
      Abxbus.wait_until_idle(:er_hid)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.handler_id == entry.id
    end
  end

  describe "handler_file_path detection" do
    test "handler_file_path detected in EventResult" do
      {:ok, _} = Abxbus.start_bus(:er_filepath)

      Abxbus.on(:er_filepath, ERNoHandlersEvent, fn _e -> :ok end,
        handler_name: "filepath_handler")

      event = Abxbus.emit(:er_filepath, ERNoHandlersEvent.new())
      Abxbus.wait_until_idle(:er_filepath)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      # handler_file_path should be set (non-nil) when detect_file_paths is enabled
      assert result.handler_file_path != nil, "handler_file_path should be detected"
      assert is_binary(result.handler_file_path), "handler_file_path should be a string"
    end
  end

  describe "EventResult status transitions" do
    test "EventResult status transitions from pending to started to completed" do
      result = Abxbus.EventResult.new("handler_1", handler_name: "test")
      assert result.status == :pending

      result = Abxbus.EventResult.mark_started(result)
      assert result.status == :started
      assert result.started_at != nil

      result = Abxbus.EventResult.mark_completed(result, :done)
      assert result.status == :completed
      assert result.result == :done
      assert result.completed_at != nil
    end

    test "EventResult mark_error stores error" do
      result = Abxbus.EventResult.new("handler_2", handler_name: "err_test")
      assert result.status == :pending

      error = %RuntimeError{message: "something went wrong"}
      result = Abxbus.EventResult.mark_error(result, error)

      assert result.status == :error
      assert result.error == error
      assert result.completed_at != nil
    end
  end

  describe "edge cases" do
    test "event with no handlers has empty results" do
      {:ok, _} = Abxbus.start_bus(:er_empty)

      event = Abxbus.emit(:er_empty, ERNoHandlersEvent.new())
      Abxbus.wait_until_idle(:er_empty)

      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_results == %{}
    end
  end

  # ── Additional result tests ──────────────────────────────────────────────

  defevent(ERStartedTimingEvent)
  defevent(ERNoCastEvent)
  defevent(ERHandlerIdMatchEvent)

  describe "handler started timing" do
    test "handler started timing after lock entry" do
      {:ok, _} = Abxbus.start_bus(:er_started_timing,
        event_handler_concurrency: :serial)

      barrier = :atomics.new(1, [])

      entry1 = Abxbus.on(:er_started_timing, ERStartedTimingEvent, fn _event ->
        :atomics.put(barrier, 1, 1)
        spin_wait(fn -> :atomics.get(barrier, 1) == 2 end, 5000)
        "first_done"
      end, handler_name: "first_handler")

      entry2 = Abxbus.on(:er_started_timing, ERStartedTimingEvent, fn _event ->
        "second_done"
      end, handler_name: "second_handler")

      event = Abxbus.emit(:er_started_timing, ERStartedTimingEvent.new())

      # Wait for first handler to start
      spin_wait(fn -> :atomics.get(barrier, 1) == 1 end, 2000)

      stored = Abxbus.EventStore.get(event.event_id)
      h1_result = Map.get(stored.event_results, entry1.id)
      h2_result = Map.get(stored.event_results, entry2.id)

      assert h1_result != nil
      assert h1_result.status == :started,
             "First handler should be :started, got: #{inspect(h1_result.status)}"

      assert h2_result == nil or h2_result.status == :pending,
             "Second handler should be :pending or nil, got: #{inspect(h2_result)}"

      # Release
      :atomics.put(barrier, 1, 2)
      Abxbus.wait_until_idle(:er_started_timing)

      Abxbus.stop(:er_started_timing, clear: true)
    end
  end

  describe "no casting when no result_type" do
    test "no casting when no result_type" do
      {:ok, _} = Abxbus.start_bus(:er_no_cast)

      Abxbus.on(:er_no_cast, ERNoCastEvent, fn _event ->
        %{raw: "data"}
      end, handler_name: "raw_handler")

      event = Abxbus.emit(:er_no_cast, ERNoCastEvent.new())
      Abxbus.wait_until_idle(:er_no_cast)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.result == %{raw: "data"},
             "Result should be the raw map unchanged, got: #{inspect(result.result)}"

      Abxbus.stop(:er_no_cast, clear: true)
    end
  end

  describe "handler_id matches registered entry" do
    test "handler_id matches registered entry id" do
      {:ok, _} = Abxbus.start_bus(:er_hid_match)

      entry = Abxbus.on(:er_hid_match, ERHandlerIdMatchEvent, fn _event -> :ok end,
        handler_name: "match_handler")

      event = Abxbus.emit(:er_hid_match, ERHandlerIdMatchEvent.new())
      Abxbus.wait_until_idle(:er_hid_match)

      stored = Abxbus.EventStore.get(event.event_id)
      result = stored.event_results |> Map.values() |> hd()

      assert result.handler_id == entry.id,
             "handler_id should match entry.id; got result.handler_id=#{inspect(result.handler_id)}, entry.id=#{inspect(entry.id)}"

      Abxbus.stop(:er_hid_match, clear: true)
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp spin_wait(fun, max_ms, elapsed \\ 0) do
    if elapsed >= max_ms, do: raise("spin_wait exceeded #{max_ms}ms")
    if fun.(), do: :ok, else: (Process.sleep(1); spin_wait(fun, max_ms, elapsed + 1))
  end
end

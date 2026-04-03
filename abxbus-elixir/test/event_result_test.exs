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

  describe "edge cases" do
    test "event with no handlers has empty results" do
      {:ok, _} = Abxbus.start_bus(:er_empty)

      event = Abxbus.emit(:er_empty, ERNoHandlersEvent.new())
      Abxbus.wait_until_idle(:er_empty)

      stored = Abxbus.EventStore.get(event.event_id)
      assert stored.event_results == %{}
    end
  end
end

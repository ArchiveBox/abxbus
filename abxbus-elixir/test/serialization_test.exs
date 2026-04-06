defmodule Abxbus.SerializationTest do
  @moduledoc """
  Tests for JSON serialization/deserialization of events.
  Matches Python test_eventbus_serialization.py and TS base_event.test.ts toJSON/fromJSON tests.
  """

  use ExUnit.Case, async: false

  import Abxbus.TestEvents

  defevent(SerUserAction, action: "test", user_id: "u1")
  defevent(SerSystemEvent, name: "startup", severity: "info")

  describe "toJSON" do
    test "event serializes to map with string keys matching Python/TS field names" do
      {:ok, _} = Abxbus.start_bus(:ser_to_json)

      Abxbus.on(:ser_to_json, SerUserAction, fn _e -> "handled" end, handler_name: "ser_handler")
      event = Abxbus.emit(:ser_to_json, SerUserAction.new(action: "login", user_id: "u123"))
      Abxbus.wait_until_idle(:ser_to_json)

      stored = Abxbus.EventStore.get(event.event_id)
      json = Abxbus.to_json(stored)

      # All keys are strings
      assert Enum.all?(Map.keys(json), &is_binary/1)

      # Core event_* fields present with correct values
      assert json["event_id"] == event.event_id
      assert json["event_type"] == Atom.to_string(SerUserAction)
      assert json["event_version"] == "1"
      assert json["event_status"] == "completed"
      assert is_binary(json["event_created_at"]) or is_integer(json["event_created_at"])

      # User payload fields present
      assert json["action"] == "login"
      assert json["user_id"] == "u123"

      # event_path is a list of strings
      assert is_list(json["event_path"])
      assert length(json["event_path"]) > 0

      # event_results serialized as list
      assert is_list(json["event_results"])

      Abxbus.stop(:ser_to_json, clear: true)
    end

    test "event with no handlers serializes cleanly" do
      {:ok, _} = Abxbus.start_bus(:ser_no_handlers)

      event = Abxbus.emit(:ser_no_handlers, SerSystemEvent.new(name: "boot"))
      Abxbus.wait_until_idle(:ser_no_handlers)

      stored = Abxbus.EventStore.get(event.event_id)
      json = Abxbus.to_json(stored)

      assert json["event_results"] == []
      assert json["name"] == "boot"

      Abxbus.stop(:ser_no_handlers, clear: true)
    end

    test "event_parent_id and event_emitted_by_handler_id serialize as null when nil" do
      event = SerUserAction.new()
      json = Abxbus.to_json(event)

      assert json["event_parent_id"] == nil
      assert json["event_emitted_by_handler_id"] == nil
    end

    test "event_path serializes as list of strings" do
      {:ok, _} = Abxbus.start_bus(:ser_path)

      event = Abxbus.emit(:ser_path, SerUserAction.new())
      Abxbus.wait_until_idle(:ser_path)

      stored = Abxbus.EventStore.get(event.event_id)
      json = Abxbus.to_json(stored)

      assert is_list(json["event_path"])
      assert Enum.all?(json["event_path"], &is_binary/1)

      Abxbus.stop(:ser_path, clear: true)
    end
  end

  describe "fromJSON" do
    test "round-trips event through toJSON/fromJSON" do
      {:ok, _} = Abxbus.start_bus(:ser_roundtrip)

      event = Abxbus.emit(:ser_roundtrip, SerUserAction.new(action: "signup", user_id: "u456"))
      Abxbus.wait_until_idle(:ser_roundtrip)

      stored = Abxbus.EventStore.get(event.event_id)
      json = Abxbus.to_json(stored)
      restored = Abxbus.from_json(json, SerUserAction)

      assert restored.event_id == stored.event_id
      assert restored.event_type == stored.event_type
      assert restored.action == "signup"
      assert restored.user_id == "u456"
      assert restored.event_status == stored.event_status

      Abxbus.stop(:ser_roundtrip, clear: true)
    end

    test "fromJSON preserves event_parent_id null" do
      json = %{
        "event_id" => "test-id-123",
        "event_type" => Atom.to_string(SerUserAction),
        "event_parent_id" => nil,
        "action" => "test",
        "user_id" => "u1",
        "event_status" => "pending",
        "event_version" => "1",
        "event_result_type" => "any",
        "event_children" => [],
        "event_path" => [],
        "event_results" => [],
        "event_created_at" => nil,
        "event_started_at" => nil,
        "event_completed_at" => nil,
        "event_concurrency" => nil,
        "event_handler_concurrency" => nil,
        "event_handler_completion" => nil,
        "event_timeout" => nil,
        "event_handler_timeout" => nil,
        "event_handler_slow_timeout" => nil,
        "event_slow_timeout" => nil,
        "event_pending_bus_count" => 0,
        "event_emitted_by_handler_id" => nil
      }

      restored = Abxbus.from_json(json, SerUserAction)
      assert restored.event_parent_id == nil
      assert restored.event_id == "test-id-123"
      assert restored.event_status == :pending
    end
  end

  describe "toJSON string" do
    test "to_json_string produces valid JSON" do
      event = SerUserAction.new(action: "encode_test", user_id: "u789")
      json_str = Abxbus.to_json_string(event)

      assert is_binary(json_str)
      assert String.starts_with?(json_str, "{")
      assert String.ends_with?(json_str, "}")
      assert String.contains?(json_str, "\"event_id\"")
      assert String.contains?(json_str, "\"action\"")
      assert String.contains?(json_str, "encode_test")
    end

    test "from_json_string round-trips through string encoding" do
      event = SerUserAction.new(action: "string_roundtrip", user_id: "u101")
      json_str = Abxbus.to_json_string(event)
      restored = Abxbus.from_json_string(json_str, SerUserAction)

      assert restored.event_id == event.event_id
      assert restored.action == "string_roundtrip"
      assert restored.user_id == "u101"
    end
  end

  describe "cross-runtime field compatibility" do
    test "all Python/TS event_* fields present in serialized output" do
      event = SerUserAction.new()
      json = Abxbus.to_json(event)

      # These are the exact fields from TS BaseEvent.toJSON()
      expected_event_fields = [
        "event_id", "event_type", "event_version", "event_result_type",
        "event_timeout", "event_slow_timeout",
        "event_concurrency", "event_handler_concurrency",
        "event_handler_completion", "event_handler_slow_timeout",
        "event_handler_timeout",
        "event_parent_id", "event_path", "event_emitted_by_handler_id",
        "event_pending_bus_count",
        "event_status", "event_created_at", "event_started_at",
        "event_completed_at", "event_results",
        "event_children"
      ]

      for field <- expected_event_fields do
        assert Map.has_key?(json, field),
               "Missing field #{field} in serialized output. Keys: #{inspect(Map.keys(json))}"
      end
    end

    test "no field name aliasing or renaming" do
      event = SerUserAction.new(action: "no_alias")
      json = Abxbus.to_json(event)

      # Verify field names are exact — no camelCase, no snake_case conversion
      refute Map.has_key?(json, "eventId")
      refute Map.has_key?(json, "eventType")
      refute Map.has_key?(json, "eventStatus")

      assert Map.has_key?(json, "event_id")
      assert Map.has_key?(json, "event_type")
      assert Map.has_key?(json, "event_status")
    end
  end
end

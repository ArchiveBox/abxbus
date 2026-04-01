defmodule Abxbus.TestEvents do
  @moduledoc "Event definitions used across test suites."

  defmacro defevent(name, fields \\ []) do
    # Re-export the Abxbus.Event.defevent macro behavior
    meta_fields = [
      event_id: nil,
      event_type: nil,
      event_version: "1",
      event_result_type: :any,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_status: :pending,
      event_created_at: nil,
      event_started_at: nil,
      event_completed_at: nil,
      event_results: Macro.escape(%{}),
      event_concurrency: nil,
      event_handler_concurrency: nil,
      event_handler_completion: nil,
      event_timeout: nil,
      event_handler_timeout: nil,
      event_handler_slow_timeout: nil,
      event_slow_timeout: nil,
      event_pending_bus_count: 0,
      event_emitted_by_handler_id: nil
    ]

    escaped_fields =
      Enum.map(fields, fn
        {k, v} when is_map(v) -> {k, Macro.escape(v)}
        other -> other
      end)

    all_fields = Keyword.merge(meta_fields, escaped_fields)

    quote do
      defmodule unquote(name) do
        defstruct unquote(all_fields)

        def new(attrs \\ []) do
          base = struct!(__MODULE__, attrs)

          %{base |
            event_id: base.event_id || Abxbus.Event.generate_id(),
            event_type: __MODULE__,
            event_created_at: base.event_created_at || System.monotonic_time(:nanosecond),
            event_status: :pending
          }
        end
      end
    end
  end
end

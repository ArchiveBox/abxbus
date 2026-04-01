defmodule AbxBus.TestEvents do
  @moduledoc "Event definitions used across test suites."

  # ── Helper to create simple event structs with metadata fields ──────────────

  defmacro defevent(name, fields \\ []) do
    meta_fields = [
      event_id: nil,
      event_type: nil,
      event_parent_id: nil,
      event_children: [],
      event_path: [],
      event_status: :pending,
      event_created_at: nil,
      event_started_at: nil,
      event_completed_at: nil,
      event_results: %{},
      event_concurrency: nil,
      event_handler_concurrency: nil,
      event_handler_completion: nil,
      event_timeout: nil,
      event_handler_timeout: nil,
      event_slow_timeout: nil,
      event_pending_bus_count: 0,
      event_emitted_by_handler_id: nil
    ]

    all_fields = Keyword.merge(meta_fields, fields)

    quote do
      defmodule unquote(name) do
        defstruct unquote(all_fields)

        def new(attrs \\ []) do
          base = struct!(__MODULE__, attrs)

          %{base |
            event_id: base.event_id || AbxBus.Event.generate_id(),
            event_type: __MODULE__,
            event_created_at: base.event_created_at || System.monotonic_time(:nanosecond),
            event_status: :pending
          }
        end
      end
    end
  end
end

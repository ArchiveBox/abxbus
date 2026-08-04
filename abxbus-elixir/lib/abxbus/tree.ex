defmodule Abxbus.Tree do
  @moduledoc """
  Utilities for inspecting event lineage trees.

  Provides relationship checks and a pretty-printed ASCII tree renderer
  that shows event execution order, timing, status, and parent-child structure.
  """

  alias Abxbus.EventStore

  @doc """
  Check whether `child` is a descendant of `parent` (direct or transitive).

      Abxbus.Tree.child_of?(child_event, parent_event)
      #=> true
  """
  def child_of?(child, parent) do
    child_id = if is_map(child), do: child.event_id, else: child
    parent_id = if is_map(parent), do: parent.event_id, else: parent

    do_child_of?(child_id, parent_id)
  end

  defp do_child_of?(nil, _parent_id), do: false
  defp do_child_of?(child_id, parent_id) when child_id == parent_id, do: false

  defp do_child_of?(child_id, parent_id) do
    case EventStore.get(child_id) do
      nil -> false
      %{event_parent_id: nil} -> false
      %{event_parent_id: ^parent_id} -> true
      %{event_parent_id: mid} -> do_child_of?(mid, parent_id)
    end
  end

  @doc """
  Check whether `parent` is an ancestor of `child` (direct or transitive).
  Symmetric to `child_of?/2`.
  """
  def parent_of?(parent, child), do: child_of?(child, parent)

  @doc """
  Render an ASCII tree of the event hierarchy rooted at `event`.

  ## Options

    * `:max_depth` — maximum depth to render (default: 10)
    * `:show_timing` — include timing info (default: true)
    * `:show_results` — include handler results (default: false)

  ## Example output

      ┌ UserCreated [completed] 12ms
      │ bus: auth#a1b2 → worker#c3d4
      ├── AuthCheck [completed] 3ms
      │   handler: validate_user → :ok
      └── SendEmail [completed] 8ms
          handler: send_welcome → :ok
  """
  def log_tree(event, opts \\ []) do
    event = if is_map(event), do: event, else: EventStore.get(event)
    max_depth = Keyword.get(opts, :max_depth, 10)
    show_timing = Keyword.get(opts, :show_timing, true)
    show_results = Keyword.get(opts, :show_results, false)

    if event == nil do
      "(event not found)"
    else
      render_node(event, "", true, max_depth, 0, show_timing, show_results)
      |> IO.iodata_to_binary()
    end
  end

  defp render_node(event, prefix, is_last, max_depth, depth, show_timing, show_results) do
    if depth > max_depth do
      [prefix, branch_char(is_last), "... (max depth)\n"]
    else
      type_name = format_type(event.event_type)
      status = format_status(event.event_status)
      timing = if show_timing, do: format_timing(event), else: ""

      header = [prefix, branch_char(is_last), type_name, " [", status, "]", timing, "\n"]

      child_prefix = [prefix, if(is_last, do: "    ", else: "│   ")]

      # Bus path
      path_line =
        if event.event_path != [] do
          [child_prefix, "bus: ", Enum.join(event.event_path, " → "), "\n"]
        else
          []
        end

      # Handler results
      result_lines =
        if show_results and map_size(event.event_results) > 0 do
          event.event_results
          |> Enum.map(fn {_id, r} ->
            result_str =
              case r.status do
                :completed -> "→ #{inspect(r.result)}"
                :error -> "✗ #{inspect(r.error)}"
                other -> "#{other}"
              end

            [child_prefix, "handler: ", r.handler_name || "?", " ", result_str, "\n"]
          end)
        else
          []
        end

      # Children
      children_ids = EventStore.children_of(event.event_id)
      children = Enum.map(children_ids, &EventStore.get/1) |> Enum.reject(&is_nil/1)

      children_lines =
        children
        |> Enum.with_index()
        |> Enum.map(fn {child, idx} ->
          child_is_last = idx == length(children) - 1
          render_node(child, child_prefix, child_is_last, max_depth, depth + 1, show_timing, show_results)
        end)

      [header, path_line, result_lines, children_lines]
    end
  end

  defp branch_char(true), do: "└── "
  defp branch_char(false), do: "├── "

  defp format_type(nil), do: "?"
  defp format_type(mod) when is_atom(mod), do: mod |> Module.split() |> List.last()
  defp format_type(other), do: inspect(other)

  defp format_status(:pending), do: "pending"
  defp format_status(:started), do: "started"
  defp format_status(:completed), do: "completed"
  defp format_status(:error), do: "error"
  defp format_status(other), do: to_string(other)

  defp format_timing(event) do
    case {event.event_started_at, event.event_completed_at} do
      {nil, _} -> ""
      {_, nil} -> " (running...)"
      {s, c} ->
        ms = div(c - s, 1_000_000)
        " #{ms}ms"
    end
  end
end

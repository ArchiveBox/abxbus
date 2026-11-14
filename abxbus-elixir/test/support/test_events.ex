defmodule Abxbus.TestEvents do
  @moduledoc """
  Test helper that re-exports `Abxbus.Event.defevent/2` so tests use
  the same macro (and validations) as production code.
  """

  defmacro defevent(name, fields \\ []) do
    quote do
      require Abxbus.Event
      Abxbus.Event.defevent(unquote(name), unquote(fields))
    end
  end
end

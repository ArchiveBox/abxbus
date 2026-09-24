defmodule Abxbus.JSON do
  @moduledoc """
  Minimal JSON encoder/decoder for event serialization.

  Handles the types found in abxbus events: strings, numbers, booleans,
  nil, lists, and maps with string keys. No external dependencies.

  If Jason is available, delegates to it. Otherwise uses a built-in
  implementation sufficient for event round-tripping.
  """

  @doc "Encode an Elixir term to a JSON string."
  def encode(term) do
    if Code.ensure_loaded?(Jason) do
      Jason.encode!(term)
    else
      encode_value(term)
    end
  end

  @doc "Decode a JSON string to an Elixir term."
  def decode(string) when is_binary(string) do
    if Code.ensure_loaded?(Jason) do
      Jason.decode!(string)
    else
      {value, _rest} = decode_value(String.trim(string))
      value
    end
  end

  # ── Encoder ──────────────────────────────────────────────────────────────

  defp encode_value(nil), do: "null"
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"
  defp encode_value(v) when is_integer(v), do: Integer.to_string(v)
  defp encode_value(v) when is_float(v), do: Float.to_string(v)

  defp encode_value(v) when is_binary(v) do
    "\"" <> escape_string(v) <> "\""
  end

  defp encode_value(v) when is_atom(v), do: encode_value(Atom.to_string(v))

  defp encode_value(v) when is_list(v) do
    inner = v |> Enum.map(&encode_value/1) |> Enum.join(",")
    "[" <> inner <> "]"
  end

  defp encode_value(v) when is_map(v) do
    inner =
      v
      |> Enum.map(fn {k, val} ->
        encode_value(to_string(k)) <> ":" <> encode_value(val)
      end)
      |> Enum.join(",")

    "{" <> inner <> "}"
  end

  defp encode_value(v), do: encode_value(inspect(v))

  defp escape_string(s) do
    s
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
    |> String.replace("\r", "\\r")
    |> String.replace("\t", "\\t")
  end

  # ── Decoder ──────────────────────────────────────────────────────────────

  defp decode_value(<<"null", rest::binary>>), do: {nil, rest}
  defp decode_value(<<"true", rest::binary>>), do: {true, rest}
  defp decode_value(<<"false", rest::binary>>), do: {false, rest}

  defp decode_value(<<"\"", rest::binary>>), do: decode_string(rest, "")

  defp decode_value(<<"[", rest::binary>>), do: decode_array(String.trim_leading(rest), [])

  defp decode_value(<<"{", rest::binary>>), do: decode_object(String.trim_leading(rest), %{})

  defp decode_value(<<c, _::binary>> = s) when c in [?-, ?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9] do
    decode_number(s)
  end

  defp decode_value(s), do: {nil, s}

  defp decode_string(<<"\\\"", rest::binary>>, acc), do: decode_string(rest, acc <> "\"")
  defp decode_string(<<"\\\\", rest::binary>>, acc), do: decode_string(rest, acc <> "\\")
  defp decode_string(<<"\\n", rest::binary>>, acc), do: decode_string(rest, acc <> "\n")
  defp decode_string(<<"\\r", rest::binary>>, acc), do: decode_string(rest, acc <> "\r")
  defp decode_string(<<"\\t", rest::binary>>, acc), do: decode_string(rest, acc <> "\t")
  defp decode_string(<<"\"", rest::binary>>, acc), do: {acc, rest}
  defp decode_string(<<c, rest::binary>>, acc), do: decode_string(rest, acc <> <<c>>)

  defp decode_array(<<"}", rest::binary>>, acc), do: {Enum.reverse(acc), rest}
  defp decode_array(<<"]", rest::binary>>, acc), do: {Enum.reverse(acc), rest}

  defp decode_array(s, acc) do
    {value, rest} = decode_value(String.trim_leading(s))
    rest = rest |> String.trim_leading() |> skip_comma()
    decode_array(String.trim_leading(rest), [value | acc])
  end

  defp decode_object(<<"}", rest::binary>>, acc), do: {acc, rest}

  defp decode_object(s, acc) do
    {key, rest} = decode_value(String.trim_leading(s))
    rest = rest |> String.trim_leading() |> skip_colon()
    {value, rest} = decode_value(String.trim_leading(rest))
    rest = rest |> String.trim_leading() |> skip_comma()
    decode_object(String.trim_leading(rest), Map.put(acc, key, value))
  end

  defp decode_number(s) do
    {num_str, rest} = take_number_chars(s, "")
    if String.contains?(num_str, ".") or String.contains?(num_str, "e") or String.contains?(num_str, "E") do
      {String.to_float(num_str), rest}
    else
      {String.to_integer(num_str), rest}
    end
  end

  defp take_number_chars(<<c, rest::binary>>, acc) when c in [?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?., ?-, ?+, ?e, ?E] do
    take_number_chars(rest, acc <> <<c>>)
  end
  defp take_number_chars(rest, acc), do: {acc, rest}

  defp skip_comma(<<",", rest::binary>>), do: rest
  defp skip_comma(rest), do: rest

  defp skip_colon(<<":", rest::binary>>), do: rest
  defp skip_colon(rest), do: rest
end

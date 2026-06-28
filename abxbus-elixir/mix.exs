defmodule Abxbus.MixProject do
  use Mix.Project

  def project do
    [
      app: :abxbus,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      name: "Abxbus",
      description: "An OTP-native event bus with queue-jump, multi-bus forwarding, and lineage tracking"
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Abxbus.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    []
  end
end

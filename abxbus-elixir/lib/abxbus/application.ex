defmodule Abxbus.Application do
  @moduledoc """
  OTP Application for Abxbus.

  Supervision tree:

      Abxbus.Application
      ├── Abxbus.EventStore          (GenServer — creates ETS tables)
      ├── Abxbus.LockManager         (GenServer — global serial lock + named semaphores)
      ├── Abxbus.BusRegistry         (Registry — name resolution for buses)
      └── Abxbus.BusSupervisor       (DynamicSupervisor — spawns BusServer children)
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Abxbus.EventStore,
      Abxbus.LockManager,
      {Registry, keys: :unique, name: Abxbus.BusRegistry},
      {DynamicSupervisor, name: Abxbus.BusSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: Abxbus.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

defmodule AbxBus.Application do
  @moduledoc """
  OTP Application for AbxBus.

  Supervision tree:

      AbxBus.Application
      ├── AbxBus.EventStore          (GenServer — creates ETS tables)
      ├── AbxBus.LockManager         (GenServer — global serial lock + named semaphores)
      ├── AbxBus.BusRegistry         (Registry — name resolution for buses)
      └── AbxBus.BusSupervisor       (DynamicSupervisor — spawns BusServer children)
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      AbxBus.EventStore,
      AbxBus.LockManager,
      {Registry, keys: :unique, name: AbxBus.BusRegistry},
      {DynamicSupervisor, name: AbxBus.BusSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: AbxBus.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

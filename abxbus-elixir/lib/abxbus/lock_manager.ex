defmodule Abxbus.LockManager do
  @moduledoc """
  Centralized concurrency policy enforcement.

  In Python, this required re-entrant locks with ContextVar depth tracking.
  In Elixir/OTP, the concurrency model is fundamentally different:

  - **Bus-serial**: Enforced by the BusServer GenServer itself — it simply doesn't
    dequeue the next event until the current one completes. No lock needed.
  - **Global-serial**: A single GenServer that grants exclusive access. Buses
    `call` to acquire and `cast` to release.
  - **Parallel**: No coordination needed — events spawn freely.
  - **Handler semaphores**: For `@retry(semaphore_scope='global')` patterns,
    a named semaphore GenServer limits concurrent handler invocations.

  ## Queue-jump and "re-entrancy"

  The Python library needs re-entrant locks because queue-jump processes an event
  while the parent event's lock is held. In Elixir, we don't hold locks —
  BusServer simply receives a `:jump_queue` cast and spawns the child event's
  worker directly, bypassing the normal "is processing empty?" check.
  The parent handler is blocked in `receive` in its own process (not holding
  any GenServer state), so there's no deadlock.
  """

  use GenServer

  # ── Global serial lock ──────────────────────────────────────────────────────

  defmodule State do
    defstruct holder: nil, waiters: :queue.new()
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Acquire the global serial lock. Blocks until available.
  Returns `:ok` when acquired.
  """
  def acquire_global do
    GenServer.call(__MODULE__, :acquire, :infinity)
  end

  @doc """
  Try to acquire the global serial lock without blocking.
  Returns `:ok` if acquired, `:busy` if held by another.
  """
  def try_acquire_global do
    GenServer.call(__MODULE__, :try_acquire)
  end

  @doc "Release the global serial lock. Synchronous to ensure ordering with subsequent acquire attempts."
  def release_global do
    GenServer.call(__MODULE__, :release)
  end

  # ── Named semaphores (for handler-level concurrency limits) ────────────────

  @doc """
  Acquire a named semaphore with a given concurrency limit.
  Blocks until a slot is available.
  """
  def acquire_semaphore(name, limit \\ 1) do
    GenServer.call(__MODULE__, {:acquire_semaphore, name, limit}, :infinity)
  end

  @doc "Release a named semaphore slot. Caller pid identifies which holder to remove."
  def release_semaphore(name, caller_pid \\ self()) do
    GenServer.cast(__MODULE__, {:release_semaphore, name, caller_pid})
  end

  # ── Concurrency policy resolution ──────────────────────────────────────────

  @doc """
  Resolve the effective event concurrency mode for an event on a bus.
  Event-level override > bus default.
  """
  def resolve_event_concurrency(event, bus_config) do
    event_override = Map.get(event, :event_concurrency)
    bus_default = Map.get(bus_config, :event_concurrency, :bus_serial)
    event_override || bus_default
  end

  @doc """
  Resolve the effective handler concurrency mode.
  Event-level override > bus default.
  """
  def resolve_handler_concurrency(event, bus_config) do
    event_override = Map.get(event, :event_handler_concurrency)
    bus_default = Map.get(bus_config, :event_handler_concurrency, :parallel)
    event_override || bus_default
  end

  @doc """
  Resolve the effective handler completion mode.
  Event-level override > bus default.
  """
  def resolve_handler_completion(event, bus_config) do
    event_override = Map.get(event, :event_handler_completion)
    bus_default = Map.get(bus_config, :event_handler_completion, :all)
    event_override || bus_default
  end

  @doc """
  Resolve effective handler timeout. Priority chain (not minimum):
  handler-specific > event-level > bus-level.
  If handler has its own timeout, that wins unconditionally.
  """
  def resolve_handler_timeout(handler_entry, event, bus_config) do
    handler_entry.handler_timeout ||
      Map.get(event, :event_handler_timeout) ||
      Map.get(bus_config, :event_handler_timeout)
  end

  @doc """
  Resolve effective event timeout. Event-level > bus-level.
  """
  def resolve_event_timeout(event, bus_config) do
    Map.get(event, :event_timeout) || Map.get(bus_config, :event_timeout)
  end

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    {:ok, %{global: %State{}, semaphores: %{}, sem_monitors: %{}}}
  end

  # Global serial lock — monitor holder to auto-release on crash
  @impl true
  def handle_call(:acquire, {caller_pid, _} = from, %{global: global} = state) do
    case global.holder do
      nil ->
        mon = Process.monitor(caller_pid)
        {:reply, :ok, %{state | global: %{global | holder: {from, mon}}}}

      _held ->
        new_waiters = :queue.in(from, global.waiters)
        {:noreply, %{state | global: %{global | waiters: new_waiters}}}
    end
  end

  def handle_call(:try_acquire, {caller_pid, _} = from, %{global: global} = state) do
    case global.holder do
      nil ->
        mon = Process.monitor(caller_pid)
        {:reply, :ok, %{state | global: %{global | holder: {from, mon}}}}

      _held ->
        {:reply, :busy, state}
    end
  end

  # Named semaphores — monitor holders so slots auto-release on crash/kill
  def handle_call({:acquire_semaphore, name, limit}, {caller_pid, _} = from, state) do
    sem = Map.get(state.semaphores, name, %{holders: [], waiters: :queue.new(), limit: limit})

    if length(sem.holders) < sem.limit do
      mon_ref = Process.monitor(caller_pid)
      holder = {from, mon_ref}
      new_sem = %{sem | holders: [holder | sem.holders]}
      # Track monitor -> {semaphore_name} for :DOWN cleanup
      monitors = Map.put(state[:sem_monitors] || %{}, mon_ref, name)
      {:reply, :ok, %{state | semaphores: Map.put(state.semaphores, name, new_sem), sem_monitors: monitors}}
    else
      new_sem = %{sem | waiters: :queue.in(from, sem.waiters)}
      {:noreply, %{state | semaphores: Map.put(state.semaphores, name, new_sem)}}
    end
  end

  @impl true
  def handle_call(:release, _from, %{global: global} = state) do
    # Demonitor current holder
    case global.holder do
      {_, mon} when is_reference(mon) -> Process.demonitor(mon, [:flush])
      _ -> :ok
    end

    case :queue.out(global.waiters) do
      {{:value, {waiter_pid, _} = next_from}, rest} ->
        mon = Process.monitor(waiter_pid)
        GenServer.reply(next_from, :ok)
        {:reply, :ok, %{state | global: %{global | holder: {next_from, mon}, waiters: rest}}}

      {:empty, _} ->
        {:reply, :ok, %{state | global: %State{}}}
    end
  end

  def handle_cast({:release_semaphore, name, caller_pid}, state) do
    case Map.get(state.semaphores, name) do
      nil ->
        {:noreply, state}

      sem ->
        # Find and remove the specific holder matching caller_pid
        {removed, new_holders} =
          case Enum.split_with(sem.holders, fn {{pid, _}, _mon} -> pid == caller_pid end) do
            {[match | _rest_matches], remaining} -> {match, remaining}
            {[], holders} -> {nil, holders}
          end

        # Demonitor the released holder and clean up monitor tracking
        monitors = state[:sem_monitors] || %{}
        monitors =
          case removed do
            {_, mon_ref} ->
              Process.demonitor(mon_ref, [:flush])
              Map.delete(monitors, mon_ref)
            _ ->
              monitors
          end

        # Promote next waiter if available
        {new_holders, new_waiters, monitors} =
          case :queue.out(sem.waiters) do
            {{:value, {waiter_pid, _} = next_from}, rest} ->
              mon_ref = Process.monitor(waiter_pid)
              GenServer.reply(next_from, :ok)
              {[{next_from, mon_ref} | new_holders], rest, Map.put(monitors, mon_ref, name)}

            {:empty, _} ->
              {new_holders, sem.waiters, monitors}
          end

        new_sem = %{sem | holders: new_holders, waiters: new_waiters}
        {:noreply, %{state | semaphores: Map.put(state.semaphores, name, new_sem), sem_monitors: monitors}}
    end
  end

  # Auto-release locks when holder process dies
  @impl true
  def handle_info({:DOWN, mon_ref, :process, _pid, _reason}, state) do
    # Check if it's the global lock holder
    state =
      case state.global.holder do
        {_, ^mon_ref} ->
          # Global lock holder died — release and promote next waiter
          case :queue.out(state.global.waiters) do
            {{:value, {waiter_pid, _} = next_from}, rest} ->
              new_mon = Process.monitor(waiter_pid)
              GenServer.reply(next_from, :ok)
              %{state | global: %{state.global | holder: {next_from, new_mon}, waiters: rest}}
            {:empty, _} ->
              %{state | global: %State{}}
          end
        _ ->
          state
      end

    # Check if it's a semaphore holder
    monitors = state[:sem_monitors] || %{}

    case Map.pop(monitors, mon_ref) do
      {nil, _} ->
        {:noreply, state}

      {sem_name, monitors} ->
        state = %{state | sem_monitors: monitors}

        case Map.get(state.semaphores, sem_name) do
          nil ->
            {:noreply, state}

          sem ->
            # Remove the dead holder by matching on monitor ref
            new_holders = Enum.reject(sem.holders, fn
              {_, ^mon_ref} -> true
              _ -> false
            end)

            # Promote next waiter if slot freed
            {new_holders, new_waiters, monitors} =
              if length(new_holders) < sem.limit do
                case :queue.out(sem.waiters) do
                  {{:value, {waiter_pid, _} = next_from}, rest} ->
                    new_mon = Process.monitor(waiter_pid)
                    GenServer.reply(next_from, :ok)
                    {[{next_from, new_mon} | new_holders], rest, Map.put(state[:sem_monitors] || %{}, new_mon, sem_name)}

                  {:empty, _} ->
                    {new_holders, sem.waiters, state[:sem_monitors] || %{}}
                end
              else
                {new_holders, sem.waiters, state[:sem_monitors] || %{}}
              end

            new_sem = %{sem | holders: new_holders, waiters: new_waiters}
            {:noreply, %{state | semaphores: Map.put(state.semaphores, sem_name, new_sem), sem_monitors: monitors}}
        end
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}
end

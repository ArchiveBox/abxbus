defmodule AbxBus.LockManager do
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

  @doc "Release the global serial lock."
  def release_global do
    GenServer.cast(__MODULE__, :release)
  end

  # ── Named semaphores (for handler-level concurrency limits) ────────────────

  @doc """
  Acquire a named semaphore with a given concurrency limit.
  Blocks until a slot is available.
  """
  def acquire_semaphore(name, limit \\ 1) do
    GenServer.call(__MODULE__, {:acquire_semaphore, name, limit}, :infinity)
  end

  @doc "Release a named semaphore slot."
  def release_semaphore(name) do
    GenServer.cast(__MODULE__, {:release_semaphore, name})
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
  Resolve effective handler timeout. Tightest (smallest non-nil) wins.
  Handler-specific > event-level > bus-level.
  """
  def resolve_handler_timeout(handler_entry, event, bus_config) do
    candidates = [
      handler_entry.handler_timeout,
      Map.get(event, :event_handler_timeout),
      Map.get(bus_config, :event_handler_timeout)
    ]

    candidates
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      timeouts -> Enum.min(timeouts)
    end
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
    {:ok, %{global: %State{}, semaphores: %{}}}
  end

  # Global serial lock
  @impl true
  def handle_call(:acquire, from, %{global: global} = state) do
    case global.holder do
      nil ->
        {:reply, :ok, %{state | global: %{global | holder: from}}}

      _held ->
        new_waiters = :queue.in(from, global.waiters)
        {:noreply, %{state | global: %{global | waiters: new_waiters}}}
    end
  end

  def handle_call(:try_acquire, from, %{global: global} = state) do
    case global.holder do
      nil ->
        {:reply, :ok, %{state | global: %{global | holder: from}}}

      _held ->
        {:reply, :busy, state}
    end
  end

  # Named semaphores
  def handle_call({:acquire_semaphore, name, limit}, from, state) do
    sem = Map.get(state.semaphores, name, %{holders: [], waiters: :queue.new(), limit: limit})

    if length(sem.holders) < sem.limit do
      new_sem = %{sem | holders: [from | sem.holders]}
      {:reply, :ok, %{state | semaphores: Map.put(state.semaphores, name, new_sem)}}
    else
      new_sem = %{sem | waiters: :queue.in(from, sem.waiters)}
      {:noreply, %{state | semaphores: Map.put(state.semaphores, name, new_sem)}}
    end
  end

  @impl true
  def handle_cast(:release, %{global: global} = state) do
    case :queue.out(global.waiters) do
      {{:value, next_from}, rest} ->
        GenServer.reply(next_from, :ok)
        {:noreply, %{state | global: %{global | holder: next_from, waiters: rest}}}

      {:empty, _} ->
        {:noreply, %{state | global: %State{}}}
    end
  end

  def handle_cast({:release_semaphore, name}, state) do
    case Map.get(state.semaphores, name) do
      nil ->
        {:noreply, state}

      sem ->
        # Remove one holder (the caller)
        new_holders = tl(sem.holders)

        # Promote next waiter if available
        {new_holders, new_waiters} =
          case :queue.out(sem.waiters) do
            {{:value, next_from}, rest} ->
              GenServer.reply(next_from, :ok)
              {[next_from | new_holders], rest}

            {:empty, _} ->
              {new_holders, sem.waiters}
          end

        new_sem = %{sem | holders: new_holders, waiters: new_waiters}
        {:noreply, %{state | semaphores: Map.put(state.semaphores, name, new_sem)}}
    end
  end
end

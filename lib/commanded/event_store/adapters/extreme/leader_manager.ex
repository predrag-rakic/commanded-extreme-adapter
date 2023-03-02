defmodule Commanded.EventStore.Adapters.Extreme.LeaderManager do
  @moduledoc false

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.LeaderSupervisor
  alias Commanded.EventStore.Adapters.Extreme.Config

  defmodule State do
    @moduledoc false

    defstruct [
      :leader_conn_name,
      :leader_conn_pid,
      :leader_supervisor_name,
      :name,
      :spear_config,
      :spear_conn_name
    ]
  end

  @doc """
  Start a process to create and connect a persistent connection to the Event Store
  """
  def start_link(config) do
    spear_config = Keyword.fetch!(config, :spear)
    adapter_name = Keyword.fetch!(config, :adapter_name)

    leader_conn_name = Config.leader_conn_name(adapter_name)
    leader_supervisor_name = Config.leader_supervisor_name(adapter_name)
    name = Config.leader_manager_name(adapter_name)
    spear_conn_name = Config.spear_conn_name(adapter_name)

    state = %State{
      leader_conn_name: leader_conn_name,
      leader_supervisor_name: leader_supervisor_name,
      leader_conn_pid: nil,
      name: name,
      spear_config: spear_config,
      spear_conn_name: spear_conn_name
    }

    Logger.debug(fn -> describe(state) <> " start_link" end)

    GenServer.start_link(__MODULE__, state, name: name)
  end

  @impl GenServer
  def init(%State{} = state) do
    Logger.debug(fn -> describe(state) <> " init" end)

    {:ok, state, {:continue, :start_leader_connection}}
  end

  @impl GenServer
  def handle_continue(
        :start_leader_connection,
        %State{} = state
      ) do
    Logger.debug(fn -> describe(state) <> " handle_continue" end)

    conn_config =
      get_leader_conn_config(state)
      # TODO: REMOVE, ONLY FOR TESTING!
      |> Keyword.put(:port, 2113)

    {:ok, pid} =
      LeaderSupervisor.start_leader_connection(state.leader_supervisor_name, conn_config)

    {:noreply, %State{state | leader_conn_pid: pid}}
  end

  @impl GenServer
  def handle_call(
        :start_leader_connection,
        _from,
        %State{} = state
      ) do
    Logger.debug(fn -> describe(state) <> " start_leader_connection" end)

    case state.leader_conn_pid do
      nil ->
        Logger.debug(fn -> describe(state) <> " starting" end)

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderSupervisor.start_leader_connection(state.leader_supervisor_name, conn_config)

        {:reply, :ok, %State{state | leader_conn_pid: pid}}

      _ ->
        Logger.debug(fn -> describe(state) <> " already started" end)
        {:reply, :ok, state}
    end
  end

  @impl GenServer
  def handle_call(
        :refresh_leader_connection,
        _from,
        %State{} = state
      ) do
    Logger.debug(fn -> describe(state) <> " refresh_leader_connection" end)

    case state.leader_conn_pid do
      nil ->
        Logger.debug(fn -> describe(state) <> " starting" end)

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderSupervisor.start_leader_connection(state.leader_supervisor_name, conn_config)

        {:reply, :ok, %State{state | leader_conn_pid: pid}}

      leader_conn_pid ->
        Logger.debug(fn -> describe(state) <> " restarting" end)

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderSupervisor.refresh_leader_connection(
            state.leader_supervisor_name,
            conn_config,
            leader_conn_pid
          )

        {:reply, :ok, %State{state | leader_conn_pid: pid}}
    end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, %State{} = state) do
    Logger.debug(fn -> describe(state) <> " down due to: #{inspect(reason)}" end)

    {:stop, {:shutdown, reason}, state}
  end

  def start_leader_connection(leader_manager_name) do
    GenServer.call(leader_manager_name, :start_leader_connection)
  end

  def refresh_leader_connection(leader_manager_name) do
    GenServer.call(leader_manager_name, :refresh_leader_connection)
  end

  defp describe(%State{name: name}) do
    "Extreme event store leader manager #{inspect(name)} (#{inspect(self())})"
  end

  defp get_leader_conn_config(%State{
         leader_conn_name: leader_conn_name,
         spear_config: spear_config,
         spear_conn_name: spear_conn_name
       }) do
    {:ok, info} = Spear.cluster_info(spear_conn_name)

    leader = info |> Enum.find(&(&1.state == :Leader))

    spear_config
    |> Keyword.put(:name, leader_conn_name)
    |> Keyword.put(:host, leader.address)
    |> Keyword.put(:port, leader.port)
  end
end

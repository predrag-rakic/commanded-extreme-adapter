defmodule Commanded.EventStore.Adapters.Extreme.LeaderConnectionManager do
  @moduledoc """
  The LeaderConnectionManager holds the static eventstoredb / spear configuration as well
  as the logic for getting the leader information from the database.
  """

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.LeaderConnectionSupervisor
  alias Commanded.EventStore.Adapters.Extreme.Config

  ## Client API

  @doc """
  blocks until the leader connection is started, returns :ok.

  should be called with a name generate by `Config.leader_conn_manager_name/1`.
  """
  @spec start_leader_connection(atom | pid | {atom, any} | {:via, atom, any}) :: :ok
  def start_leader_connection(leader_conn_manager_name) do
    GenServer.call(leader_conn_manager_name, :start_leader_connection)
  end

  @doc """
  Ensure that the leader connection is connected to the current leader, blocks until it is and returns :ok.

  Should be called with a name generate by `Config.leader_conn_manager_name/1`.
  """
  @spec refresh_leader_connection(atom | pid | {atom, any} | {:via, atom, any}) :: :ok
  def refresh_leader_connection(leader_conn_manager_name) do
    GenServer.call(leader_conn_manager_name, :refresh_leader_connection)
  end

  ## Server callbacks

  defmodule State do
    @moduledoc false

    defstruct [
      :leader_conn_name,
      :leader_conn_pid,
      :leader_conn_supervisor_name,
      :name,
      :spear_config,
      :spear_conn_name
    ]
  end

  def start_link(config) do
    spear_config = Keyword.fetch!(config, :spear)
    adapter_name = Keyword.fetch!(config, :adapter_name)

    leader_conn_name = Config.leader_conn_name(adapter_name)
    leader_conn_supervisor_name = Config.leader_conn_supervisor_name(adapter_name)
    leader_conn_manager_name = Config.leader_conn_manager_name(adapter_name)
    spear_conn_name = Config.spear_conn_name(adapter_name)

    state = %State{
      leader_conn_name: leader_conn_name,
      leader_conn_supervisor_name: leader_conn_supervisor_name,
      leader_conn_pid: nil,
      name: leader_conn_manager_name,
      spear_config: spear_config,
      spear_conn_name: spear_conn_name
    }

    Logger.debug(describe(state) <> " start_link")

    GenServer.start_link(__MODULE__, state, name: leader_conn_manager_name)
  end

  @impl GenServer
  def init(%State{} = state) do
    Logger.debug(describe(state) <> " init")

    {:ok, state, {:continue, :start_leader_connection}}
  end

  @impl GenServer
  def handle_continue(
        :start_leader_connection,
        %State{} = state
      ) do
    Logger.debug(describe(state) <> " handle_continue")

    conn_config =
      get_leader_conn_config(state)
      # TODO: REMOVE, ONLY FOR TESTING!
      |> Keyword.put(:port, 2113)

    {:ok, pid} =
      LeaderConnectionSupervisor.start_leader_connection(
        state.leader_conn_supervisor_name,
        conn_config
      )

    {:noreply, %State{state | leader_conn_pid: pid}}
  end

  @impl GenServer
  def handle_call(
        :start_leader_connection,
        _from,
        %State{} = state
      ) do
    Logger.debug(describe(state) <> " start_leader_connection")

    case state.leader_conn_pid do
      nil ->
        Logger.debug(describe(state) <> " starting")

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderConnectionSupervisor.start_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, %State{state | leader_conn_pid: pid}}

      _ ->
        Logger.debug(describe(state) <> " already started")
        {:reply, :ok, state}
    end
  end

  @impl GenServer
  def handle_call(
        :refresh_leader_connection,
        _from,
        %State{} = state
      ) do
    Logger.debug(describe(state) <> " refresh_leader_connection")

    case state.leader_conn_pid do
      nil ->
        Logger.debug(describe(state) <> " starting")

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderConnectionSupervisor.start_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, %State{state | leader_conn_pid: pid}}

      leader_conn_pid ->
        Logger.debug(describe(state) <> " restarting")

        conn_config = get_leader_conn_config(state)

        {:ok, pid} =
          LeaderConnectionSupervisor.refresh_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config,
            leader_conn_pid
          )

        {:reply, :ok, %State{state | leader_conn_pid: pid}}
    end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, %State{} = state) do
    Logger.debug(describe(state) <> " down due to: #{inspect(reason)}")

    {:stop, {:shutdown, reason}, state}
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

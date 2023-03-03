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
      :host,
      :leader_conn_name,
      :leader_conn_pid,
      :leader_conn_supervisor_name,
      :name,
      :port,
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
      host: nil,
      leader_conn_name: leader_conn_name,
      leader_conn_supervisor_name: leader_conn_supervisor_name,
      name: leader_conn_manager_name,
      port: nil,
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

    [host, port] = get_leader_info(state)

    # # START TEST OVERRIDE
    # # You can uncomment this block to make sure the leader connection is
    # # connected to the wrong node at the start, to force a refresh.
    # {:ok, info} = Spear.cluster_info(state.spear_conn_name)
    # follower = info |> Enum.find(&(&1.state != :Leader))
    # port = follower.port
    # # END TEST OVERRIDE

    conn_config = make_leader_conn_config(state, host, port)

    :ok =
      LeaderConnectionSupervisor.start_leader_connection(
        state.leader_conn_supervisor_name,
        conn_config
      )

    {:noreply, %State{state | host: host, port: port}}
  end

  @impl GenServer
  def handle_call(
        :start_leader_connection,
        _from,
        %State{} = state
      ) do
    Logger.debug(describe(state) <> " start_leader_connection")

    case state.host do
      nil ->
        Logger.debug(describe(state) <> " starting")

        [host, port] = get_leader_info(state)
        conn_config = make_leader_conn_config(state, host, port)

        :ok =
          LeaderConnectionSupervisor.start_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, %State{state | host: host, port: port}}

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

    [host, port] = get_leader_info(state)
    conn_config = make_leader_conn_config(state, host, port)

    case {state.host, state.port} do
      {nil, nil} ->
        Logger.debug(describe(state) <> " starting")

        :ok =
          LeaderConnectionSupervisor.start_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, %State{state | host: host, port: port}}

      {^host, ^port} ->
        Logger.debug(describe(state) <> " configuration up to date")

        # since it is likely that multiple subscribers would call refresh at the
        # same time, we check if a previous refresh was successful rather than
        # always restart the connection.
        :ok =
          LeaderConnectionSupervisor.start_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, state}

      {_, _} ->
        Logger.debug(describe(state) <> " restarting")

        :ok =
          LeaderConnectionSupervisor.refresh_leader_connection(
            state.leader_conn_supervisor_name,
            conn_config
          )

        {:reply, :ok, %State{state | host: host, port: port}}
    end

    {:reply, :ok, state}
  end

  defp describe(%State{name: name}) do
    "Extreme event store leader manager #{inspect(name)} (#{inspect(self())})"
  end

  defp get_leader_info(%State{} = state) do
    {:ok, info} = Spear.cluster_info(state.spear_conn_name)

    leader = info |> Enum.find(&(&1.state == :Leader))

    [leader.address, leader.port]
  end

  defp make_leader_conn_config(%State{} = state, host, port) do
    state.spear_config
    |> Keyword.put(:name, state.leader_conn_name)
    |> Keyword.put(:host, host)
    |> Keyword.put(:port, port)
  end
end

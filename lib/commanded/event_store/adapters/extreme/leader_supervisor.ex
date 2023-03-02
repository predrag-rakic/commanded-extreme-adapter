defmodule Commanded.EventStore.Adapters.Extreme.LeaderSupervisor do
  @moduledoc false

  use DynamicSupervisor

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.Config

  def start_link(config) do
    Logger.debug(fn -> "LeaderSupervisor (#{inspect(self())}) start_link" end)

    name = Keyword.fetch!(config, :adapter_name) |> Config.leader_supervisor_name()
    DynamicSupervisor.start_link(__MODULE__, config, name: name)
  end

  @impl true
  def init(_config) do
    Logger.debug(fn -> "LeaderSupervisor (#{inspect(self())}) init" end)

    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_leader_connection(leader_supervisor_name, conn_config) do
    Logger.debug(fn ->
      "LeaderSupervisor (#{inspect(self())}) start_leader_connection #{inspect(conn_config)}"
    end)

    case DynamicSupervisor.start_child(
           leader_supervisor_name,
           %{
             id: LeaderConn,
             start: {Spear.Connection, :start_link, [conn_config]},
             restart: :permanent,
             shutdown: 5000,
             type: :worker
           }
         ) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      x ->
        x
    end
  end

  def refresh_leader_connection(leader_supervisor_name, conn_config, leader_conn_pid) do
    Logger.debug(fn ->
      "LeaderSupervisor (#{inspect(self())}) refresh_leader_connection #{inspect(conn_config)}"
    end)

    DynamicSupervisor.terminate_child(leader_supervisor_name, leader_conn_pid)

    start_leader_connection(leader_supervisor_name, conn_config)
  end
end

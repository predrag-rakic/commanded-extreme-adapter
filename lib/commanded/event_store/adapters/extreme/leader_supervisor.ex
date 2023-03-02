defmodule Commanded.EventStore.Adapters.Extreme.LeaderSupervisor do
  @moduledoc false

  use Supervisor

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.Config

  def start_link(config) do
    name = Keyword.fetch!(config, :adapter_name) |> Config.leader_supervisor_name()

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    adapter_name = Keyword.fetch!(config, :adapter_name)
    spear_conn_name = Config.spear_conn_name(adapter_name)

    {:ok, info} = Spear.cluster_info(spear_conn_name) |> IO.inspect(label: "cluster_info")

    leader = info |> Enum.find(&(&1.state == :Leader)) |> IO.inspect(label: "leader")

    leader_conn_name = Config.leader_conn_name(adapter_name)

    conn_config =
      Keyword.get(config, :spear)
      |> Keyword.put(:name, leader_conn_name)
      # |> Keyword.put(:host, leader.address)
      # |> Keyword.put(:port, leader.port)
      |> IO.inspect(label: "conn_config")

    Supervisor.init(
      [
        %{
          id: Conn,
          start: {Spear.Connection, :start_link, [conn_config]},
          restart: :permanent,
          shutdown: 5000,
          type: :worker
        }
      ],
      strategy: :one_for_one
    )
  end
end

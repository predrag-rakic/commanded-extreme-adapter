defmodule Commanded.EventStore.Adapters.Extreme.Supervisor do
  @moduledoc false

  use Supervisor

  alias Commanded.EventStore.Adapters.Extreme.Config
  alias Commanded.EventStore.Adapters.Extreme.EventPublisher
  alias Commanded.EventStore.Adapters.Extreme.LeaderConnectionManager
  alias Commanded.EventStore.Adapters.Extreme.LeaderSupervisor
  alias Commanded.EventStore.Adapters.Extreme.SubscriptionsSupervisor

  def start_link(config) do
    name = Keyword.fetch!(config, :adapter_name) |> Config.supervisor_name()

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    all_stream = Config.all_stream(config)
    serializer = Config.serializer(config)

    adapter_name = Keyword.fetch!(config, :adapter_name)

    event_publisher_name = Module.concat([adapter_name, EventPublisher])
    subscriptions_name = Module.concat([adapter_name, SubscriptionsSupervisor])

    pubsub_name = Config.pubsub_name(adapter_name)
    spear_conn_name = Config.spear_conn_name(adapter_name)
    leader_supervisor_name = Config.leader_supervisor_name(adapter_name)

    conn_config =
      Keyword.get(config, :spear)
      |> Keyword.put(:name, spear_conn_name)

    children = [
      {Registry, keys: :duplicate, name: pubsub_name, partitions: 1},
      %{
        id: Conn,
        start: {Spear.Connection, :start_link, [conn_config]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      },
      %{
        id: EventPublisher,
        start:
          {EventPublisher, :start_link,
           [
             {spear_conn_name, pubsub_name, all_stream, serializer},
             [name: event_publisher_name]
           ]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      },
      {SubscriptionsSupervisor, name: subscriptions_name},
      %{
        id: leader_supervisor_name,
        start: {LeaderSupervisor, :start_link, [config]},
        restart: :permanent
      },
      {LeaderConnectionManager, config}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Commanded.EventStore.Adapters.Extreme.Supervisor do
  @moduledoc false

  use Supervisor

  # alias Commanded.EventStore.Adapters.Extreme.Client
  alias Commanded.EventStore.Adapters.Extreme.Config
  alias Commanded.EventStore.Adapters.Extreme.EventPublisher
  alias Commanded.EventStore.Adapters.Extreme.SubscriptionsSupervisor

  def start_link(config) do
    event_store = Keyword.fetch!(config, :event_store)
    name = Module.concat([event_store, Supervisor])

    Supervisor.start_link(__MODULE__, config, name: name)
  end

  @impl Supervisor
  def init(config) do
    all_stream = Config.all_stream(config)
    serializer = Config.serializer(config)

    event_store = Keyword.fetch!(config, :event_store)
    event_publisher_name = Module.concat([event_store, EventPublisher])
    pubsub_name = Module.concat([event_store, PubSub])
    subscriptions_name = Module.concat([event_store, SubscriptionsSupervisor])

    # client_name = Module.concat([event_store, Client]) |> IO.inspect(label: :client_name)

    # conn_name = Module.concat([event_store, Conn])

    conn_config =
      Keyword.get(config, :spear)
      |> Keyword.put(:name, event_store)

    children = [
      # {Registry, keys: :duplicate, name: pubsub_name, partitions: 1},
      %{
        id: Conn,
        start: {Spear.Connection, :start_link, [conn_config]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      }
      # %{
      #   id: EventPublisher,
      #   start:
      #     {EventPublisher, :start_link,
      #      [
      #        {event_store, pubsub_name, all_stream, serializer},
      #        [name: event_publisher_name]
      #      ]},
      #   restart: :permanent,
      #   shutdown: 5000,
      #   type: :worker
      # },
      # {SubscriptionsSupervisor, name: subscriptions_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

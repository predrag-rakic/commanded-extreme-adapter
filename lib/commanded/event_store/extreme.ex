defmodule Commanded.EventStore.Adapters.Extreme do
  @moduledoc """
  Adapter to use [Event Store](https://eventstore.com/), via the Extreme TCP
  client, with Commanded.

  Please check the [Getting started](getting-started.html) guide to learn more.
  """

  @behaviour Commanded.EventStore.Adapter

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.Config
  alias Commanded.EventStore.Adapters.Extreme.Mapper
  alias Commanded.EventStore.Adapters.Extreme.Subscription
  alias Commanded.EventStore.Adapters.Extreme.SubscriptionsSupervisor
  alias Commanded.EventStore.EventData
  alias Commanded.EventStore.RecordedEvent
  alias Commanded.EventStore.SnapshotData
  alias Commanded.EventStore.TypeProvider

  require Spear.Records.Streams, as: Streams
  require Spear.Records.Shared, as: Shared

  @impl Commanded.EventStore.Adapter
  def child_spec(application, config) do
    event_store =
      case Keyword.get(config, :name) do
        nil -> Module.concat([application, Extreme])
        name -> Module.concat([name, Extreme])
      end

    # Rename `prefix` config to `stream_prefix`
    config =
      case Keyword.pop(config, :prefix) do
        {nil, config} -> config
        {prefix, config} -> Keyword.put(config, :stream_prefix, prefix)
      end

    child_spec = [
      Supervisor.child_spec(
        {Commanded.EventStore.Adapters.Extreme.Supervisor,
         Keyword.put(config, :event_store, event_store)},
        id: event_store
      )
    ]

    adapter_meta = %{
      all_stream: Config.all_stream(config),
      event_store: event_store,
      stream_prefix: Config.stream_prefix(config),
      serializer: Config.serializer(config)
    }

    {:ok, child_spec, adapter_meta}
  end

  @impl Commanded.EventStore.Adapter
  def append_to_stream(adapter_meta, stream_uuid, expected_version, events) do
    stream = stream_name(adapter_meta, stream_uuid)

    Logger.debug(fn ->
      "Extreme event store attempting to append to stream " <>
        inspect(stream) <> " " <> inspect(length(events)) <> " event(s)"
    end)

    add_to_stream(adapter_meta, stream, expected_version, events)
  end

  @impl Commanded.EventStore.Adapter
  def stream_forward(
        adapter_meta,
        stream_uuid,
        start_version \\ 0,
        chunk_size \\ 128
      ) do
    stream_name = stream_name(adapter_meta, stream_uuid)
    from = normalize_start_version(start_version)

    execute_read(adapter_meta, stream_name, from, :forwards, chunk_size)
  end

  @impl Commanded.EventStore.Adapter
  @spec subscribe(map, any) :: :ok | {:error, {:already_registered, pid}}
  def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, "$all")

  @impl Commanded.EventStore.Adapter
  def subscribe(adapter_meta, stream_uuid) do
    event_store = server_name(adapter_meta)
    pubsub_name = Module.concat([event_store, PubSub])

    with {:ok, _} <- Registry.register(pubsub_name, stream_uuid, []) do
      :ok
    end
  end

  @impl Commanded.EventStore.Adapter
  def subscribe_to(adapter_meta, :all, subscription_name, subscriber, start_from, opts) do
    event_store = server_name(adapter_meta)
    stream = Map.fetch!(adapter_meta, :all_stream)
    serializer = serializer(adapter_meta)
    opts = subscription_options(opts, start_from)

    SubscriptionsSupervisor.start_subscription(
      event_store,
      stream,
      subscription_name,
      subscriber,
      serializer,
      opts
    )
  end

  @impl Commanded.EventStore.Adapter
  def subscribe_to(adapter_meta, stream_uuid, subscription_name, subscriber, start_from, opts) do
    event_store = server_name(adapter_meta)
    stream = stream_name(adapter_meta, stream_uuid)
    serializer = serializer(adapter_meta)
    opts = subscription_options(opts, start_from)

    SubscriptionsSupervisor.start_subscription(
      event_store,
      stream,
      subscription_name,
      subscriber,
      serializer,
      opts
    )
  end

  @impl Commanded.EventStore.Adapter
  def ack_event(_adapter_meta, subscription, %RecordedEvent{event_number: event_number}) do
    Subscription.ack(subscription, event_number)
  end

  @impl Commanded.EventStore.Adapter
  def unsubscribe(adapter_meta, subscription) do
    event_store = server_name(adapter_meta)
    SubscriptionsSupervisor.stop_subscription(event_store, subscription)
  end

  @impl Commanded.EventStore.Adapter
  def delete_subscription(adapter_meta, :all, subscription_name) do
    event_store = server_name(adapter_meta)
    stream = Map.fetch!(adapter_meta, :all_stream)

    delete_persistent_subscription(event_store, stream, subscription_name)
  end

  @impl Commanded.EventStore.Adapter
  def delete_subscription(adapter_meta, stream_uuid, subscription_name) do
    event_store = server_name(adapter_meta)
    stream = stream_name(adapter_meta, stream_uuid)

    delete_persistent_subscription(event_store, stream, subscription_name)
  end

  @impl Commanded.EventStore.Adapter
  def read_snapshot(adapter_meta, source_uuid) do
    stream_name = snapshot_stream_name(adapter_meta, source_uuid)

    Logger.debug(fn ->
      "Extreme event store read snapshot from stream: " <> inspect(stream_name)
    end)

    case execute_read(adapter_meta, stream_name, :end, :backwards, 1) do
      {:error, :stream_not_found} ->
        {:error, :snapshot_not_found}

      stream ->
        snapshot =
          stream
          |> Stream.take(1)
          |> Enum.to_list()
          |> List.first()
          |> to_snapshot_data()

        {:ok, snapshot}
    end
  end

  @impl Commanded.EventStore.Adapter
  def record_snapshot(adapter_meta, %SnapshotData{} = snapshot) do
    event_data = to_event_data(snapshot)
    stream = snapshot_stream_name(adapter_meta, snapshot.source_uuid)

    Logger.debug(fn -> "Extreme event store record snapshot to stream: " <> inspect(stream) end)

    add_to_stream(adapter_meta, stream, :any_version, [event_data])
  end

  @impl Commanded.EventStore.Adapter
  def delete_snapshot(adapter_meta, source_uuid) do
    server = server_name(adapter_meta)
    stream_name = snapshot_stream_name(adapter_meta, source_uuid)

    Spear.delete_stream(server, stream_name, expect: :any)
  end

  defp stream_name(adapter_meta, stream_uuid),
    do: Map.fetch!(adapter_meta, :stream_prefix) <> "-" <> stream_uuid

  defp snapshot_stream_name(adapter_meta, source_uuid),
    do: Map.fetch!(adapter_meta, :stream_prefix) <> "snapshot-" <> source_uuid

  defp normalize_start_version(0), do: 0
  defp normalize_start_version(start_version), do: start_version - 1

  defp to_snapshot_data(%RecordedEvent{} = event) do
    %RecordedEvent{data: snapshot} = event

    data =
      snapshot.source_type
      |> String.to_existing_atom()
      |> struct(snapshot.data)
      |> Commanded.Serialization.JsonDecoder.decode()

    %SnapshotData{snapshot | data: data, created_at: event.created_at}
  end

  defp to_event_data(%SnapshotData{} = snapshot) do
    %EventData{
      event_type: TypeProvider.to_string(snapshot),
      data: snapshot
    }
  end

  defp add_to_stream(adapter_meta, stream_name, expected_version, events) do
    server = server_name(adapter_meta)
    serializer = serializer(adapter_meta)

    events = write_events_request(events, serializer)

    expect =
      case expected_version do
        :no_stream -> :empty
        :any_version -> :any
        :stream_exists -> :exists
        0 -> :empty
        x -> x - 1
      end

    case Spear.append(events, server, stream_name, expect: expect) do
      {:error, %Spear.ExpectationViolation{} = e} ->
        Logger.debug("add_to_stream: #{inspect(e)}")

        case expected_version do
          :no_stream -> {:error, :stream_exists}
          :stream_exists -> {:error, :stream_not_found}
          _ -> {:error, :wrong_expected_version}
        end

      {:error, reason} ->
        {:error, reason}

      :ok ->
        :ok
    end
  end

  @spec execute_read(map, :all | binary, any, any, any) ::
          Enumerable.t()
          | {:error, :stream_not_found}
  defp execute_read(
         adapter_meta,
         stream_name,
         from,
         direction,
         chunk_size
       ) do
    server = server_name(adapter_meta)
    serializer = serializer(adapter_meta)

    stream =
      Spear.stream!(
        server,
        stream_name,
        from: from,
        direction: direction,
        raw?: true,
        chunk_size: chunk_size
      )

    case stream |> Stream.take(1) |> Enum.to_list() do
      [] ->
        {:error, :stream_not_found}

      _ ->
        stream
        |> Stream.map(fn x ->
          Mapper.to_recorded_event_spear(x, serializer)
        end)
    end
  end

  defp add_causation_id(metadata, causation_id),
    do: add_to_metadata(metadata, "$causationId", causation_id)

  defp add_correlation_id(metadata, correlation_id),
    do: add_to_metadata(metadata, "$correlationId", correlation_id)

  defp add_to_metadata(metadata, key, value) when is_nil(metadata),
    do: add_to_metadata(%{}, key, value)

  defp add_to_metadata(metadata, _key, value) when is_nil(value), do: metadata

  defp add_to_metadata(metadata, key, value), do: Map.put(metadata, key, value)

  defp write_events_request(events, serializer) do
    Enum.map(events, fn event ->
      metadata =
        event.metadata
        |> add_causation_id(event.causation_id)
        |> add_correlation_id(event.correlation_id)

      Streams.append_req(
        content:
          {:proposed_message,
           Streams.append_req_proposed_message(
             custom_metadata: serializer.serialize(metadata),
             data: serializer.serialize(event.data),
             id: Shared.uuid(value: {:string, Commanded.UUID.uuid4()}),
             metadata: %{
               "type" => event.event_type,
               # TODO: real content type?
               "content-type" => "application/json"
             }
           )}
      )
    end)
  end

  defp delete_persistent_subscription(server, stream, name) do
    Logger.debug(fn ->
      "Attempting to delete persistent subscription named #{inspect(name)} on stream #{inspect(stream)}"
    end)

    case Spear.delete_persistent_subscription(server, stream, name, []) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp subscription_options(opts, start_from) do
    Keyword.put(opts, :start_from, start_from)
  end

  defp serializer(adapter_meta), do: Map.fetch!(adapter_meta, :serializer)
  defp server_name(adapter_meta), do: Map.fetch!(adapter_meta, :event_store)
end

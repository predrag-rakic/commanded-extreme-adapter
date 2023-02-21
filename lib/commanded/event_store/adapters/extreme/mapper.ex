defmodule Commanded.EventStore.Adapters.Extreme.Mapper do
  @moduledoc false

  require Spear.Records.Streams, as: Streams

  alias Commanded.EventStore.RecordedEvent
  alias Extreme.Msg, as: ExMsg

  def to_recorded_event(%ExMsg.ResolvedIndexedEvent{event: event, link: nil}, serializer),
    do: to_recorded_event(event, event.event_number + 1, serializer)

  def to_recorded_event(%ExMsg.ResolvedIndexedEvent{event: event, link: link}, serializer),
    do: to_recorded_event(event, link.event_number + 1, serializer)

  def to_recorded_event(%ExMsg.ResolvedEvent{event: event, link: nil}, serializer),
    do: to_recorded_event(event, event.event_number + 1, serializer)

  def to_recorded_event(%ExMsg.ResolvedEvent{event: event, link: link}, serializer),
    do: to_recorded_event(event, link.event_number + 1, serializer)

  def to_recorded_event(%ExMsg.EventRecord{} = event, serializer),
    do: to_recorded_event(event, event.event_number + 1, serializer)

  def to_recorded_event(%ExMsg.EventRecord{} = event, event_number, serializer) do
    %ExMsg.EventRecord{
      event_id: event_id,
      event_type: event_type,
      event_number: stream_version,
      created_epoch: created_epoch,
      data: data,
      metadata: metadata
    } = event

    data = serializer.deserialize(data, type: event_type)

    metadata =
      case metadata do
        none when none in [nil, ""] -> %{}
        metadata -> serializer.deserialize(metadata, [])
      end

    {causation_id, metadata} = Map.pop(metadata, "$causationId")
    {correlation_id, metadata} = Map.pop(metadata, "$correlationId")

    %RecordedEvent{
      event_id: UUID.binary_to_string!(event_id),
      event_number: event_number,
      stream_id: to_stream_id(event),
      stream_version: stream_version + 1,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: to_date_time(created_epoch)
    }
  end

  defp to_stream_id(%ExMsg.EventRecord{event_stream_id: event_stream_id}) do
    event_stream_id
    |> String.split("-")
    |> Enum.drop(1)
    |> Enum.join("-")
  end

  defp to_stream_id({:"event_store.client.StreamIdentifier", event_stream_id}) do
    event_stream_id
    |> String.split("-")
    |> Enum.drop(1)
    |> Enum.join("-")
  end

  defp to_date_time(millis_since_epoch) do
    DateTime.from_unix!(millis_since_epoch, :millisecond)
  end

  defp to_date_time_nanos(nanos_since_epoch) do
    DateTime.from_unix!(nanos_since_epoch, :nanosecond)
  end

  def to_recorded_event_spear({_type, {:event, _event}} = read_response, serializer) do
    # Spear.Event.from_read_response()

    event =
      read_response
      # |> IO.inspect(label: "pre")
      |> from_read_response()
      # |> IO.inspect(label: "post")
      |> Streams.read_resp_read_event_recorded_event()
      |> Map.new()

    # |> IO.inspect(label: "event")

    %{
      id: id,
      # event_type: event_type,
      # event_number: stream_version,
      # created_epoch: created_epoch,
      data: data,
      metadata: metadata,
      custom_metadata: custom_metadata,
      stream_revision: stream_revision,
      stream_identifier: stream_identifier
    } = event

    event_type = Map.get(metadata, "type")
    data = serializer.deserialize(data, type: event_type)

    custom_metadata =
      case custom_metadata do
        none when none in [nil, ""] -> %{}
        custom_metadata -> serializer.deserialize(custom_metadata, [])
      end

    {causation_id, custom_metadata} = Map.pop(custom_metadata, "$causationId")
    {correlation_id, custom_metadata} = Map.pop(custom_metadata, "$correlationId")

    {x, ""} = Map.get(metadata, "created") |> Integer.parse()

    %RecordedEvent{
      event_id: Spear.Uuid.from_proto(id),
      # TODO: not possible to get all stream position if you are not reading all stream?
      event_number: stream_revision,
      stream_id: to_stream_id(stream_identifier),
      stream_version: stream_revision + 1,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: custom_metadata,
      created_at: to_date_time_nanos(x)
    }

    # |> IO.inspect(label: "RecordedEvent")
  end

  def from_read_response(
        Streams.read_resp(
          content:
            {:event,
             Streams.read_resp_read_event(
               link: :undefined,
               event: Streams.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    event
  end
end

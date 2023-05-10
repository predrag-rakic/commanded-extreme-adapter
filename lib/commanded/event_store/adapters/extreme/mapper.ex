defmodule Commanded.EventStore.Adapters.Extreme.Mapper do
  @moduledoc false

  require Spear.Records.Streams, as: Streams
  require Spear.Records.Persistent, as: Persistent

  alias Commanded.EventStore.RecordedEvent

  defp to_stream_id({:"event_store.client.StreamIdentifier", event_stream_id}) do
    event_stream_id
    |> String.split("-")
    |> Enum.drop(1)
    |> Enum.join("-")
  end

  defp to_date_time_nanos(nanos_since_epoch) do
    DateTime.from_unix!(nanos_since_epoch, :nanosecond)
  end

  def to_recorded_event_spear({_type, {:event, _event}} = read_response, serializer) do
    {event, event_number} =
      read_response
      |> from_read_response()

    event_type = Map.get(event.metadata, "type")
    data = serializer.deserialize(event.data, type: event_type)

    custom_metadata =
      case event.custom_metadata do
        none when none in [nil, ""] -> %{}
        custom_metadata -> serializer.deserialize(custom_metadata, [])
      end

    {causation_id, custom_metadata} = Map.pop(custom_metadata, "$causationId")
    {correlation_id, custom_metadata} = Map.pop(custom_metadata, "$correlationId")

    {x, ""} = Map.get(event.metadata, "created") |> Integer.parse()

    %RecordedEvent{
      event_id: Spear.Uuid.from_proto(event.id),
      # TODO: not possible to get all stream position if you are not reading all stream?
      event_number: event_number,
      stream_id: to_stream_id(event.stream_identifier),
      stream_version: event.stream_revision + 1,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: custom_metadata,
      created_at: to_date_time_nanos(x)
    }
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
    event =
      event
      |> Streams.read_resp_read_event_recorded_event()
      |> Map.new()

    {event, event.stream_revision + 1}
  end

  def from_read_response(
        Streams.read_resp(
          content:
            {:event,
             Streams.read_resp_read_event(
               link: Streams.read_resp_read_event_recorded_event() = link,
               event: Streams.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    event =
      event
      |> Streams.read_resp_read_event_recorded_event()
      |> Map.new()

    link =
      link
      |> Streams.read_resp_read_event_recorded_event()
      |> Map.new()

    {event, link.stream_revision + 1}
  end

  def from_read_response(
        Persistent.read_resp(
          content:
            {:event,
             Persistent.read_resp_read_event(
               link: :undefined,
               event: Persistent.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    event =
      event
      |> Persistent.read_resp_read_event_recorded_event()
      |> Map.new()

    {event, event.stream_revision + 1}
  end

  def from_read_response(
        Persistent.read_resp(
          content:
            {:event,
             Persistent.read_resp_read_event(
               link: Persistent.read_resp_read_event_recorded_event() = link,
               event: Persistent.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    event =
      event
      |> Persistent.read_resp_read_event_recorded_event()
      |> Map.new()

    link =
      link
      |> Persistent.read_resp_read_event_recorded_event()
      |> Map.new()

    {event, link.stream_revision + 1}
  end

  def persistent_read_response_to_map(
        Persistent.read_resp(
          content:
            {:event,
             Persistent.read_resp_read_event(
               link: :undefined,
               event: Persistent.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    %{
      event:
        event
        |> Persistent.read_resp_read_event_recorded_event()
        |> Map.new()
    }
  end

  def persistent_read_response_to_map(
        Persistent.read_resp(
          content:
            {:event,
             Persistent.read_resp_read_event(
               link: Persistent.read_resp_read_event_recorded_event() = link,
               event: Persistent.read_resp_read_event_recorded_event() = event
             )}
        )
      ) do
    %{
      link:
        link
        |> Persistent.read_resp_read_event_recorded_event()
        |> Map.new(),
      event:
        event
        |> Persistent.read_resp_read_event_recorded_event()
        |> Map.new()
    }
  end
end

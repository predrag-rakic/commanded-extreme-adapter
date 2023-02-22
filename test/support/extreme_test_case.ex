defmodule Commanded.ExtremeTestCase do
  use ExUnit.CaseTemplate

  setup do
    {:ok, event_store_meta} = start_event_store()

    [event_store_meta: event_store_meta]
  end

  def start_event_store(config \\ []) do
    uuid = Commanded.UUID.uuid4() |> String.replace("-", "_")

    config =
      Keyword.merge(
        [
          serializer: Commanded.Serialization.JsonSerializer,
          stream_prefix: "commandedtest_" <> uuid,
          spear: [
            connection_string: "esdb://localhost:2113"
          ]
        ],
        config
      )

    {:ok, child_spec, event_store_meta} =
      Commanded.EventStore.Adapters.Extreme.child_spec(ExtremeApplication, config)

    for child <- child_spec do
      start_supervised!(child)
    end

    {:ok, event_store_meta}
  end
end

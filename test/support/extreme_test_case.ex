defmodule Commanded.ExtremeTestCase do
  use ExUnit.CaseTemplate

  alias Commanded.EventStore.Adapters.Extreme

  setup do
    {:ok, event_store_meta} = start_event_store()

    [event_store_meta: event_store_meta]
  end

  def start_event_store(config \\ []) do
    config =
      Keyword.merge(
        [
          serializer: Commanded.Serialization.JsonSerializer,
          stream_prefix: "commandedtest" <> UUID.uuid4(:hex),
          spear: [
            connection_string: "esdb://localhost:2113"
          ]
        ],
        config
      )

    # {:ok, child_spec, event_store_meta} = Extreme.child_spec(ExtremeApplication, config)
    # child = List.first(child_spec)
    # for child <- child_spec do
    # # IO.inspect(child, label: "child")
    # pid = start_supervised!(child) |> IO.inspect(label: "start_supervised!(child)")
    # end

    # {:ok, event_store_meta |> Map.put(:pid, pid)}

    {:ok, child_spec, event_store_meta} = Extreme.child_spec(ExtremeApplication, config)

    for child <- child_spec do
      start_supervised!(child)
    end

    {:ok, event_store_meta}
  end
end

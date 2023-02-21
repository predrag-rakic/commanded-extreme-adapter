defmodule Commanded.EventStore.Adapters.Extreme.EventStorePrefixTest do
  alias Commanded.EventStore.Adapters.Extreme
  alias Commanded.ExtremeTestCase

  use Commanded.EventStore.EventStorePrefixTestCase, event_store: Extreme

  def start_event_store(config) do
    uuid = UUID.uuid4() |> String.replace("-", "")

    config =
      Keyword.update!(config, :prefix, fn prefix ->
        "commandedtest" <> prefix <> uuid
      end)
      |> IO.inspect(label: "config")

    ExtremeTestCase.start_event_store(config)
  end
end

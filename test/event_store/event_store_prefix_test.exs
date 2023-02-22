defmodule Commanded.EventStore.Adapters.Extreme.EventStorePrefixTest do
  alias Commanded.EventStore.Adapters.Extreme
  alias Commanded.ExtremeTestCase

  use Commanded.EventStore.EventStorePrefixTestCase, event_store: Extreme

  def start_event_store(config) do
    uuid = Commanded.UUID.uuid4() |> String.replace("-", "_")

    config =
      Keyword.update!(config, :prefix, fn prefix ->
        "commandedtest_" <> prefix <> uuid
      end)

    ExtremeTestCase.start_event_store(config)
  end
end

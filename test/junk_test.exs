defmodule Commanded.EventStore.Adapters.Extreme.JunkTest do
  use ExUnit.Case

  alias Commanded.ExtremeTestCase
  alias Commanded.EventStore.Adapters.Extreme.Mapper
  alias Commanded.EventStore.Adapters.Extreme.SubscriptionTest.BankAccountOpened

  require Spear.Records.Streams, as: Streams
  require Spear.Records.Shared, as: Shared

  test "lol" do
    ("commandedtest" <> Commanded.UUID.uuid4()) |> IO.inspect()
  end
end

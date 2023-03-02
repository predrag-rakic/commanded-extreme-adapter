defmodule Commanded.EventStore.Adapters.Extreme.Config do
  @moduledoc false

  def all_stream(config), do: "$ce-" <> stream_prefix(config)

  def stream_prefix(config) do
    prefix =
      Keyword.get(config, :stream_prefix) ||
        raise ArgumentError, "expects :stream_prefix to be configured in environment"

    case String.contains?(prefix, "-") do
      true -> raise ArgumentError, ":stream_prefix cannot contain a dash (\"-\")"
      false -> prefix
    end
  end

  def serializer(config) do
    Keyword.get(config, :serializer) ||
      raise ArgumentError, "expects :serializer to be configured in environment"
  end

  def pubsub_name(adapter_name), do: Module.concat([adapter_name, PubSub])
  def spear_conn_name(adapter_name), do: Module.concat([adapter_name, SpearConn])
  def leader_conn_name(adapter_name), do: Module.concat([adapter_name, LeaderConn])

  def supervisor_name(adapter_name),
    do: Module.concat([adapter_name, Supervisor])

  def leader_supervisor_name(adapter_name),
    do: Module.concat([adapter_name, LeaderSupervisor])

  def leader_manager_name(adapter_name),
    do: Module.concat([adapter_name, LeaderManager])
end

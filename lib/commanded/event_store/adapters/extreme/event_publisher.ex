defmodule Commanded.EventStore.Adapters.Extreme.EventPublisher do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false

    defstruct [:spear_conn_name, :pubsub_name, :subscription_ref, :stream_name, :serializer]
  end

  alias Commanded.EventStore.Adapters.Extreme.EventPublisher.State
  alias Commanded.EventStore.Adapters.Extreme.Mapper
  alias Commanded.EventStore.RecordedEvent

  def start_link({spear_conn_name, pubsub_name, stream_name, serializer}, opts \\ []) do
    state = %State{
      spear_conn_name: spear_conn_name,
      pubsub_name: pubsub_name,
      stream_name: stream_name,
      serializer: serializer
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl GenServer
  def init(%State{} = state) do
    :ok = GenServer.cast(self(), :subscribe)

    {:ok, state}
  end

  @impl GenServer
  def handle_cast(:subscribe, state) do
    %State{spear_conn_name: spear_conn_name, stream_name: stream_name} = state

    {:ok, subscription_ref} = Spear.subscribe(spear_conn_name, self(), stream_name, raw?: true)

    {:noreply, %{state | subscription_ref: subscription_ref}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{subscription_ref: ref} = state) do
    reconnect_delay = 1_000

    Logger.warn("Subscription to EventStore is down. Will retry in #{reconnect_delay} ms.")

    :timer.sleep(reconnect_delay)

    :ok = GenServer.cast(self(), :subscribe)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({_, msg}, state) do
    %State{serializer: serializer, pubsub_name: pubsub_name} = state

    msg
    |> Mapper.to_recorded_event_spear(serializer)
    |> publish(pubsub_name)

    {:noreply, state}
  end

  defp publish(%RecordedEvent{} = recorded_event, pubsub_name) do
    :ok = publish_to_all(recorded_event, pubsub_name)
    :ok = publish_to_stream(recorded_event, pubsub_name)
  end

  defp publish_to_all(%RecordedEvent{} = recorded_event, pubsub_name) do
    Registry.dispatch(pubsub_name, "$all", fn entries ->
      for {pid, _} <- entries, do: send(pid, {:events, [recorded_event]})
    end)
  end

  defp publish_to_stream(%RecordedEvent{} = recorded_event, pubsub_name) do
    %RecordedEvent{stream_id: stream_id} = recorded_event

    Registry.dispatch(pubsub_name, stream_id, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:events, [recorded_event]})
    end)
  end
end

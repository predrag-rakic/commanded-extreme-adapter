defmodule Commanded.EventStore.Adapters.Extreme.Subscription do
  @moduledoc false

  use GenServer

  require Logger

  alias Commanded.EventStore.Adapters.Extreme.LeaderConnectionManager
  alias Commanded.EventStore.Adapters.Extreme.Mapper
  alias Commanded.EventStore.Adapters.Extreme.Config
  alias Commanded.EventStore.RecordedEvent

  defmodule State do
    @moduledoc false

    defstruct [
      :adapter_name,
      :spear_conn_name,
      :last_seen_correlation_id,
      :last_seen_event_id,
      :last_seen_event_number,
      :last_ack_time,
      :leader_conn_manager_name,
      :name,
      :retry_interval,
      :serializer,
      :stream,
      :start_from,
      :concurrency_limit,
      :subscriber,
      :subscriber_ref,
      :subscription_ref,
      subscribed?: false
    ]
  end

  alias Commanded.EventStore.Adapters.Extreme.Subscription.State

  @doc """
  Start a process to create and connect a persistent connection to the Event Store
  """
  def start_link(adapter_name, stream, subscription_name, subscriber, serializer, opts) do
    partition_by = Keyword.get(opts, :partition_by, nil)

    if partition_by != nil do
      raise "partition_by not supported by eventstoredb"
    end

    state = %State{
      adapter_name: adapter_name,
      spear_conn_name: Config.leader_conn_name(adapter_name),
      leader_conn_manager_name: Config.leader_conn_manager_name(adapter_name),
      stream: stream,
      name: subscription_name,
      serializer: serializer,
      subscriber: subscriber,
      start_from: Keyword.get(opts, :start_from),
      concurrency_limit: Keyword.get(opts, :concurrency_limit, 1),
      retry_interval: subscription_retry_interval(),
      last_ack_time: System.monotonic_time()
    }

    Logger.debug(fn -> describe(state) <> " start_link" end)

    # Prevent duplicate subscriptions by stream/name
    name =
      {:global,
       {adapter_name, __MODULE__, stream, subscription_name, Keyword.get(opts, :index, 1)}}

    GenServer.start_link(__MODULE__, state, name: name)
  end

  @doc """
  Acknowledge receipt and successful processing of the given event.
  """
  def ack(subscription, event_number) do
    GenServer.call(subscription, {:ack, event_number})
  end

  @impl GenServer
  def init(%State{} = state) do
    Process.flag(:trap_exit, true)

    %State{subscriber: subscriber} = state

    state = %State{state | subscriber_ref: Process.monitor(subscriber)}

    send(self(), :subscribe)

    {:ok, state}
  end

  @impl GenServer
  def handle_call(
        {:ack, event_number},
        _from,
        %State{last_seen_event_number: event_number} = state
      ) do
    %State{
      spear_conn_name: spear_conn_name,
      subscription_ref: subscription_ref,
      last_seen_event_id: event_id
    } = state

    Logger.debug(fn ->
      describe(state) <> " ack event: #{inspect(event_number)}, #{inspect(event_id)}"
    end)

    :ok = Spear.ack(spear_conn_name, subscription_ref, [event_id])

    state = %State{
      state
      | last_seen_event_id: nil,
        last_seen_event_number: nil,
        last_ack_time: System.monotonic_time()
    }

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:subscribe, state) do
    Logger.debug(fn ->
      describe(state) <>
        " to stream: #{inspect(state.stream)}, start from: #{inspect(state.start_from)}"
    end)

    {:noreply, subscribe(state)}
  end

  @impl GenServer
  def handle_info({_, response}, %State{} = state) do
    %State{
      subscriber: subscriber,
      spear_conn_name: spear_conn_name,
      subscription_ref: subscription_ref,
      serializer: serializer
    } = state

    response_map =
      response
      |> Mapper.persistent_read_response_to_map()

    event_type = Map.get(response_map.event.metadata, "type")

    Logger.debug(fn -> describe(state) <> " received response: #{inspect(response_map)}" end)

    event_id =
      case Map.get(response_map, :link, nil) do
        nil -> response_map.event.id
        link -> link.id
      end
      |> Spear.Uuid.from_proto()

    Logger.debug(fn ->
      describe(state) <>
        " event_id: #{event_id}, response.event.id: #{inspect(response_map.event.id)}"
    end)

    state =
      if event_type != nil and "$" != String.first(event_type) do
        %RecordedEvent{event_number: event_number} =
          recorded_event = Mapper.to_recorded_event_spear(response, serializer)

        send(subscriber, {:events, [recorded_event]})

        %State{
          state
          | last_seen_event_id: event_id,
            last_seen_event_number: event_number
        }
      else
        Logger.debug(fn ->
          describe(state) <> " ignoring event of type: #{inspect(event_type)}"
        end)

        :ok = Spear.ack(spear_conn_name, subscription_ref, [event_id])

        state
      end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    Logger.debug(fn -> describe(state) <> " down due to: #{inspect(reason)}" end)

    %State{subscriber_ref: subscriber_ref, subscription_ref: subscription_ref} = state

    case {ref, reason} do
      {^subscriber_ref, _} ->
        {:stop, {:shutdown, :subscriber_shutdown}, state}

      {^subscription_ref, :unsubscribe} ->
        {:noreply, state}

      {^subscription_ref, _} ->
        {:stop, {:shutdown, :receiver_shutdown}, state}
    end
  end

  @impl GenServer
  def handle_info({:EXIT, _from, reason}, %State{} = state) do
    Logger.debug(fn -> describe(state) <> " exit due to: #{inspect(reason)}" end)
  end

  @impl GenServer
  def terminate(_, %State{} = state) do
    ms_since_last_ack =
      (System.monotonic_time() - state.last_ack_time)
      |> System.convert_time_unit(:native, :millisecond)

    if ms_since_last_ack >= 0 && ms_since_last_ack < 1000 do
      # If there is not enough time between an ack and a cancel the ack can be
      # dropped, see the docs for Spear.connect_to_persistent_subscription/4 for
      # more info.
      #
      # https://hexdocs.pm/spear/Spear.html#connect_to_persistent_subscription/4
      Logger.debug(fn -> describe(state) <> " sleeping for #{1000 - ms_since_last_ack}" end)

      Process.sleep(1000 - ms_since_last_ack)
    end

    Logger.debug(fn -> describe(state) <> " cancelling" end)

    case state.subscription_ref do
      nil ->
        nil

      ref ->
        Spear.cancel_subscription(state.spear_conn_name, ref)
    end

    Logger.debug(fn -> describe(state) <> " cancelled" end)
  end

  defp subscribe(%State{} = state) do
    with :ok <- create_persistent_subscription(state),
         {:ok, subscription_ref} <- connect_to_persistent_subscription(state) do
      :ok = notify_subscribed(state)

      %State{
        state
        | subscription_ref: subscription_ref,
          subscribed?: true
      }
    else
      {:error, %Spear.Grpc.Response{message: "Leader info available"}} ->
        Logger.warn(fn ->
          describe(state) <>
            " failed to subscribe due to not being connected to leader, restarting leader supervisor"
        end)

        :ok = LeaderConnectionManager.refresh_leader_connection(state.leader_conn_manager_name)

        Logger.warn(fn -> describe(state) <> " supervisor restarted" end)

        send(self(), :subscribe)

        %State{state | subscribed?: false}

      err ->
        %State{retry_interval: retry_interval} = state

        Logger.warn(fn ->
          describe(state) <>
            " failed to subscribe due to: #{inspect(err)}. Will retry in #{retry_interval}ms"
        end)

        Process.send_after(self(), :subscribe, retry_interval)

        %State{state | subscribed?: false}
    end
  end

  defp notify_subscribed(%State{subscriber: subscriber}) do
    send(subscriber, {:subscribed, self()})

    :ok
  end

  defp create_persistent_subscription(%State{} = state) do
    %State{
      spear_conn_name: spear_conn_name,
      name: name,
      stream: stream,
      start_from: start_from,
      concurrency_limit: concurrency_limit,
      leader_conn_manager_name: leader_conn_manager_name
    } = state

    from =
      case start_from do
        :origin -> :start
        :current -> :end
        event_number -> event_number
      end

    Logger.debug(fn -> describe(state) <> " create_persistent_subscription" end)

    LeaderConnectionManager.start_leader_connection(leader_conn_manager_name)

    case Spear.create_persistent_subscription(
           spear_conn_name,
           stream,
           name,
           %Spear.PersistentSubscription.Settings{
             max_subscriber_count: concurrency_limit
           },
           from: from
         ) do
      :ok ->
        Logger.debug("persistent subscription created")
        :ok

      {:error, %Spear.Grpc.Response{status: :already_exists}} ->
        Logger.debug("persistent subscription already exists")
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect_to_persistent_subscription(%State{} = state) do
    %State{spear_conn_name: spear_conn_name, name: name, stream: stream} = state

    Logger.debug(fn -> describe(state) <> " connect_to_persistent_subscription" end)

    Spear.connect_to_persistent_subscription(spear_conn_name, self(), stream, name, raw?: true)
  end

  # Get the delay between subscription attempts, in milliseconds, from app
  # config. The default value is one minute. The minimum allowed value is one
  # second.
  defp subscription_retry_interval do
    case Application.get_env(:commanded_extreme_adapter, :subscription_retry_interval) do
      interval when is_integer(interval) and interval > 0 ->
        # Ensure interval is no less than one second
        max(interval, 1_000)

      _ ->
        # Default to one minute
        60_000
    end
  end

  defp describe(%State{name: name}) do
    "Extreme event store subscription #{inspect(name)} (#{inspect(self())})"
  end
end

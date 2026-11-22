# Where does a ReplicationConnection's password appear? Never prints the secret itself:
# every RESULT line carries booleans only, so any occurrence in the job log came from Postgrex/OTP.
secret = System.fetch_env!("REPRO_SECRET")
mode = System.fetch_env!("REPRO_MODE")
level = String.to_atom(System.fetch_env!("REPRO_LEVEL"))
Logger.configure(level: level)

defmodule Repro.Conn do
  use Postgrex.ReplicationConnection

  def start_link(opts), do: Postgrex.ReplicationConnection.start_link(__MODULE__, :ok, opts)

  @impl true
  def init(:ok), do: {:ok, nil}

  @impl true
  def handle_connect(state) do
    send(:repro_main, :connected)
    {:noreply, state}
  end

  @impl true
  def handle_info(:crash, _state), do: raise("repro crash")
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_data(_data, state), do: {:noreply, state}
end

defmodule Repro.Configure do
  def conf(opts), do: Keyword.put(opts, :password, System.fetch_env!("REPRO_SECRET"))
end

defmodule Repro.Capture do
  def log(event, %{config: %{pid: pid}}), do: send(pid, {:captured, event})
end

has = fn term ->
  term |> inspect(limit: :infinity, printable_limit: :infinity) |> String.contains?(secret)
end

erl_has = fn term ->
  :io_lib.format(~c"~p", [term]) |> IO.chardata_to_string() |> String.contains?(secret)
end

Process.register(self(), :repro_main)
Process.flag(:trap_exit, true)
:ok = :logger.add_handler(:repro_capture, Repro.Capture, %{config: %{pid: self()}, level: :all})

base = [hostname: "127.0.0.1", port: 5432, username: "postgres", database: "postgres"]

opts =
  case mode do
    "literal" -> [password: secret] ++ base
    "configure" -> [configure: {Repro.Configure, :conf, []}] ++ base
  end

{:ok, pid} = Repro.Conn.start_link(opts)

receive do
  :connected -> :ok
after
  15_000 -> raise "did not connect"
end

status = :sys.get_status(pid)
{:dictionary, dict} = Process.info(pid, :dictionary)
keys = for {k, v} <- dict, has.(v), do: k

IO.puts(
  "RESULT mode=#{mode} level=#{level} sys_get_status_inspect=#{has.(status)} " <>
    "sys_get_status_erlang_p=#{erl_has.(status)} pdict_keys_holding_secret=#{inspect(keys)}"
)

send(pid, :crash)

receive do
  {:EXIT, ^pid, _} -> :ok
after
  5_000 -> raise "connection did not exit"
end

Process.sleep(1_500)

collect = fn collect, acc ->
  receive do
    {:captured, event} -> collect.(collect, [event | acc])
  after
    0 -> Enum.reverse(acc)
  end
end

events = collect.(collect, [])

label = fn
  %{msg: {:report, %{label: l}}} -> l
  %{meta: %{error_logger: %{tag: t}}} -> t
  _ -> :other
end

for {l, group} <- Enum.group_by(events, label) do
  IO.puts(
    "RESULT mode=#{mode} level=#{level} raw_event=#{inspect(l)} count=#{length(group)} " <>
      "raw_contains_secret=#{Enum.any?(group, has)}"
  )
end

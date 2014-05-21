-module(clients).
-author("Vincent").
-import(werkzeug, [logging/2, get_config_value/2, logstop/0, timeMilliSecond/0]).
-import(meinWerkzeug, [read_config/2]).
-import(io_lib, [format/2]).
-import(random, [uniform/1]).
-import(timer, [kill_after/2, sleep/1]).
-export([start/1]).

start(Host) ->
  [Clients|[Lifetime|[Servername|[Sendeintervall|[]]]]] = read_config([clients, lifetime, servername, sendeintervall], "client.cfg"),
  spawn_clients(Sendeintervall * 1000, {Servername, Host}, Lifetime * 1000, Clients).

spawn_clients(_, _, _, 0) -> ok;
spawn_clients(Sendeintervall, Server, Lifetime, Client) ->
  Pid = spawn_link(fun() -> start_client(Sendeintervall, Server, Client) end),
  kill_after(Lifetime, Pid),
  spawn_clients(Sendeintervall, Server, Lifetime, Client - 1).

start_client(Sendeintervall, Server, Client) ->
  {ok, Hostname} = inet:gethostname(),
  LogPrefix = format("~b-client@~s-~p-C-9-09: ", [Client, Hostname, self()]),
  LogName = format("Client~bclient@~s.log", [Client, Hostname]),
  start_redakteur_client(Sendeintervall, Server, LogName, LogPrefix, 5, []).

start_redakteur_client(Sendeintervall, Server, LogName, LogPrefix, 0, Messages) ->
  Server ! {query_msgid, self()},
  receive
    {msgid, Number} ->
      logging(LogName, format("~bte_Nachricht um ~svergessen zu senden ******~n", [Number, timeMilliSecond()])),
      NewSendeinterval = calc_sendeintervall(Sendeintervall, LogName),
      start_lese_client(NewSendeinterval, Server, LogName, LogPrefix, Messages)
  end;
start_redakteur_client(Sendeintervall, Server, LogName, LogPrefix, Sendungen, Messages) ->
  Server ! {query_msgid, self()},
  receive
    {msgid, Number} ->
      sleep(Sendeintervall),
      Message = format("~s~bte_Nachricht. C Out: ~s", [LogPrefix, Number, timeMilliSecond()]),
      logging(LogName, format("~s gesendet~n", [Message])),
      NewMessages = [Number|Messages],
      Server ! {new_message, {Message, Number}},
      start_redakteur_client(Sendeintervall, Server, LogName, LogPrefix, Sendungen - 1, NewMessages)
  end.

start_lese_client(Sendeinterval, Server, LogName, LogPrefix, Messages) ->
  Server ! {query_messages, self()},
  receive
    {message, Number, Message, Terminated} ->
      Member = lists:member(Number, Messages),
      if
        Member == true ->
          logging(LogName, format("~s~s******* ; C In: ~s~n", [LogPrefix, Message, timeMilliSecond()]));
        true ->
          logging(LogName, format("~s~s ; C In: ~s~n", [LogPrefix, Message, timeMilliSecond()]))
      end,
      if
        Terminated == true ->
          logging(LogName, "..getmessages..Done...~n");
        true ->
          start_lese_client(Sendeinterval, Server, LogName, LogPrefix, Messages)
      end
  end.

calc_sendeintervall(Sendeintervall, LogName) ->
  Rand = uniform(2),
  if
    Rand == 1 ->
      NewSendeintervall = Sendeintervall * 1.5;
    Sendeintervall * 0.5 < 2000 ->
      NewSendeintervall = 2000;
    Rand == 2 ->
      NewSendeintervall = Sendeintervall * 0.5
  end,
  % Der Fall "(mindestens 1 Sekunde) per Zufall vergrößert oder verkleinert"
  % wird abgefangen, weil die kleinste Zahl nur 2 Sekunden sein kann
  % und das Delta von 50% minimal nur 1 Sekunde sein kann.
  logging(LogName, format("Neues Sendeintervall: ~f Sekunden (~f).~n", [NewSendeintervall / 1000, Sendeintervall / 1000])),
  NewSendeintervall.

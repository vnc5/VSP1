-module(server).
-author("Vincent").
-import(werkzeug, [logging/2, logstop/0, get_config_value/2, reset_timer/3, emptySL/0, pushSL/2, timeMilliSecond/0, findneSL/2, lengthSL/1, minNrSL/1, popSL/1, findSL/2, list2String/1]).
-import(meinWerkzeug, [read_config/2, maxNrSL/1]).
-import(global, [register_name/2, unregister_name/1]).
-import(io_lib, [format/2]).
-import(timer, [send_after/2]).
-export([start/0]).

start() ->
  [_|[Clientlifetime|[Servername|[Dlqlimit|[]]]]] = read_config([latency, clientlifetime, servername, dlqlimit], "server.cfg"),
  Lifetime = 1800,
  register(Servername, self()),
  {ok, Hostname} = inet:gethostname(),
  LogName = format("NServer@~s.log", [Hostname]),
  logging(LogName, format("Server Startzeit: ~s mit PID ~p~n", [timeMilliSecond(), self()])),
  receive_client_request(1, LogName, Servername, Lifetime * 1000, Clientlifetime, emptySL(), emptySL(), Dlqlimit, []).

receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, Clients) ->
  receive
    {query_msgid, Pid} ->
      Pid ! {msgid, MsgId},
      logging(LogName, format("Server: Nachrichtennummer ~b an ~p gesendet~n", [MsgId, Pid])),
      receive_client_request(MsgId + 1, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, Clients);
    {query_messages, Pid} ->
      {NewClients, Timer, ClientMsgId} = update_client_timer(Pid, Clientlifetime, Clients),
      NewClientMsgId = lists:max([ClientMsgId, minNrSL(Deliveryqueue)]),
      case findneSL(Deliveryqueue, NewClientMsgId) of
        {-1, nok} ->
          Pid ! {message, 1, "nicht leere dummy-Nachricht", true};
        {NewerClientMsgId, Message} ->
          case maxNrSL(Deliveryqueue) > NewerClientMsgId of
            true ->
              Pid ! {message, NewerClientMsgId, Message, false};
            false ->
              Pid ! {message, NewerClientMsgId, Message, true}
          end,
          UpdatedClients = lists:keyreplace(Pid, 1, NewClients, {Pid, Timer, NewerClientMsgId + 1}),
          receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, UpdatedClients)
      end,
      receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, NewClients);
    {new_message, {Message, Number}} ->
      {NewDeliveryqueue, NewHoldbackqueue} = new_message(LogName, Number, Message, Deliveryqueue, Holdbackqueue, Dlqlimit),
      receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, NewDeliveryqueue, NewHoldbackqueue, Dlqlimit, Clients);
    {remove_client, Pid} ->
      logging(LogName, format("Client ~p wird vergessen! *************~n", [Pid])),
      receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, lists:keydelete(Pid, 1, Clients));
    _ ->
      % Flush message queue
      receive_client_request(MsgId, LogName, Servername, Lifetime, Clientlifetime, Deliveryqueue, Holdbackqueue, Dlqlimit, Clients)
  after
    Lifetime ->
      logging(LogName, format("Downtime: ~s vom Nachrichtenserver ~p; Anzahl Restnachrichten in der HBQ:~b~n", [timeMilliSecond(), self(), 5])),
      unregister_name(Servername),
      logstop()
  end.

update_client_timer(Pid, Clientlifetime, Clients) ->
  case lists:keyfind(Pid, 1, Clients) of
    false ->
      {ok, Timer} = send_after(Clientlifetime * 1000, {remove_client, Pid}),
      {[{Pid, Timer, 1}|Clients], Timer, 1};
    {_, Timer, MsgId} ->
      NewTimer = reset_timer(Timer, Clientlifetime, {remove_client, Pid}),
      {lists:keyreplace(Pid, 1, Clients, {Pid, NewTimer, MsgId}), NewTimer, MsgId}
  end.

new_message(LogName, Number, Message, Deliveryqueue, Holdbackqueue, Dlqlimit) ->
  NewHoldbackqueue = pushSL(Holdbackqueue, {Number, format("~s(~b); HBQ In: ~s", [Message, Number, timeMilliSecond()])}),
  case lengthSL(Holdbackqueue) > Dlqlimit / 2 of
    true ->
      NewDeliveryqueue = fill_with_errors(LogName, Deliveryqueue, NewHoldbackqueue),
      {NewerDeliveryqueue, NewerHoldbackqueue} = move_to_dlq(LogName, NewDeliveryqueue, NewHoldbackqueue),
      NewestDeliveryqueue = shrink_dlq(LogName, NewerDeliveryqueue, Dlqlimit),
      {NewestDeliveryqueue, NewerHoldbackqueue};
    false ->
      {Deliveryqueue, NewHoldbackqueue}
  end.


fill_with_errors(LogName, Deliveryqueue, Holdbackqueue) ->
  Min = minNrSL(Holdbackqueue),
  Max = maxNrSL(Deliveryqueue),
  if
    Min - Max > 1 ->
      logging(LogName, format("***Fehlernachricht fuer Nachrichtennummern ~b bis ~b um ~s~n", [Max, Min, timeMilliSecond()])),
      fill_with_errors_rec(Deliveryqueue, Min, Min - Max - 1);
    true ->
      Deliveryqueue
  end.

fill_with_errors_rec(Deliveryqueue, _, 0) ->
  Deliveryqueue;
fill_with_errors_rec(Deliveryqueue, BaseNumber, Count) ->
  fill_with_errors_rec(pushSL(Deliveryqueue, {BaseNumber - Count, "Fehlernachricht"}), BaseNumber, Count - 1).

move_to_dlq(LogName, Deliveryqueue, Holdbackqueue) ->
  {NewDeliveryqueue, NewHoldbackqueue, MovedMsgNumbers} = move_to_dlq_rec(Deliveryqueue, Holdbackqueue, []),
  logging(LogName, format("QVerwaltung1>>> Nachrichten ~s von HBQ in DLQ transferiert.~n", [list2String(MovedMsgNumbers)])),
  {NewDeliveryqueue, NewHoldbackqueue}.
move_to_dlq_rec(Deliveryqueue, Holdbackqueue, MovedMsgNumbers) ->
  Min = minNrSL(Holdbackqueue),
  Max = maxNrSL(Deliveryqueue),
  if
    Min == Max + 1 ->
      {PopNum, PopMsg} = findSL(Holdbackqueue, Min),
      NewHoldbackqueue = popSL(Holdbackqueue),
      NewMovedMsgNumbers = [Min|MovedMsgNumbers],
      move_to_dlq_rec(pushSL(Deliveryqueue, {PopNum, format("~s DLQ In:~s", [PopMsg, timeMilliSecond()])}), NewHoldbackqueue, NewMovedMsgNumbers);
    true ->
      {Deliveryqueue, Holdbackqueue, MovedMsgNumbers}
  end.

shrink_dlq(LogName, Deliveryqueue, Dlqlimit) ->
  case lengthSL(Deliveryqueue) > Dlqlimit of
    true ->
      {NewDeliveryqueue, DeletedMessages} = shrink_dlq_rec(Deliveryqueue, Dlqlimit, []),
      logging(LogName, format("QVerwaltung1>>> Nachrichten ~s von DLQ geloescht.~n", [list2String(DeletedMessages)])),
      NewDeliveryqueue;
    false ->
      Deliveryqueue
  end.
shrink_dlq_rec(Deliveryqueue, Dlqlimit, DeletedMessages) ->
  case lengthSL(Deliveryqueue) > Dlqlimit of
    true ->
      MinNum = minNrSL(Deliveryqueue),
      shrink_dlq_rec(popSL(Deliveryqueue), Dlqlimit, [MinNum|DeletedMessages]);
    false ->
      {Deliveryqueue, DeletedMessages}
  end.

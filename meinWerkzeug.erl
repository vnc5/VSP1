-module(meinWerkzeug).
-author("Vincent").
-import(werkzeug, [get_config_value/2]).
-import(file, [consult/1]).
-export([read_config/2, maxNrSL/1]).

read_config(Keys, FileName) ->
  {ok, File} = consult(FileName),
  read_config_rec(Keys, File).
read_config_rec([], _) ->
  [];
read_config_rec([Key|Keys], File) ->
  {ok, Value} = get_config_value(Key, File),
  [Value|read_config_rec(Keys, File)].


maxNrSL([]) -> 0;
maxNrSL([{Num, _}|_]) -> Num.

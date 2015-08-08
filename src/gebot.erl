-module(gebot).
-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
	ok ->
	    ok;
	{error, {already_started, App}} ->
	    ok
    end.
	
start() ->
    ensure_started(crypto),
    application:start(gebot),
    gebot_weibo:schedule_task().

stop() ->
    Res = application:stop(gebot),
    application:stop(crypto),
    Res.

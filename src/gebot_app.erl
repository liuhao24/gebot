-module(gebot_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ok = application:start(bson),
    ok = application:start(mongodb),
    {ok, XmppNode} = application:get_env(gebot, xmpp_node),
    net_adm:ping(XmppNode),
    Sup = gebot_sup:start_link(),
    gebot_mongo:start(),
    
    Sup.

stop(_State) ->
    ok.

-module(gebot_weibo).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 schedule_task/0
	]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3,
	 handle_cast/2, handle_info/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(PROC, gebot_weibo).

-record(state, {tasks = []}).

start_link() ->
    gen_server:start_link({local, ?PROC}, ?MODULE,
			  [], []).

schedule_task() ->
    gen_server:cast(?PROC, {schedule}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
    {ok, #state{}}.

terminate(_Reason, #state{}) ->
    ok.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast({schedule}, State) ->
    %% Tasks = [{<<"11">>, <<"22">>}, {<<"xx">>, <<"yy">>}],
    Tasks = gebot_mongo:get_weibo_binding(),
    Iter = fun({Channel, Tokens}) -> 
           case gebot_weibo_task:start_link({sleeper, 1000*60}, Tokens, Channel) of
	          {ok, Pid} ->
           	      TaskList = [{Pid, {Tokens, Channel}}|State#state.tasks],
                      State#state{tasks = TaskList};
                  {error, Reason} ->
                      ok
           end
	   end,
    [Iter(X) || X <- Tasks],
    {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-module(gebot_weibo_task).
-behaviour(gen_server).

%% API
-export([start_link/4, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
	  task_pid :: pid(),
	  since_id,
	  xmpp_node,
	  xmpp_server
	}).


start_link(Schedule, Id, Tokens, Channel) ->
    gen_server:start_link(?MODULE, [{Schedule, Id, Tokens, Channel}], []).
    
stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([{Schedule, Id, Tokens, Channel}]) ->
    Self = self(),
    SinceId = gebot_mongo:get_since_id(Channel, Tokens),
    {ok, XmppNode} = application:get_env(gebot, xmpp_node),
    {ok, XmppServer} = application:get_env(gebot, xmpp_server),
    Pid = spawn_link(fun() -> 
    	  		   run_task(Schedule, Id, Tokens, Channel, XmppNode, XmppServer, Self)
			   end),
    {ok, #state{task_pid = Pid, since_id = SinceId, xmpp_server= XmppServer, xmpp_node = XmppNode}}.

run_task({sleeper, Millis}, Id, Tokens, Channel, XmppNode, XmppServer, ParentPid) ->
    %% apply_task(Exec),
    {Header, HttpOption, RequestOption} = get_http_option(),
    SinceId = gen_server:call(ParentPid, sinceid),
    io:format("tokens: ~p , channel: ~p, since: ~p node: ~p, server: ~p ~n", [Tokens, Channel, SinceId, XmppNode, XmppServer]),
    Count = case SinceId of 
		0 -> 1;
		_ -> 50
	    end,
    Url = lists:concat(["https://api.weibo.com/2/statuses/friends_timeline.json?access_token=", 
			binary_to_list(Tokens),"&since_id=", SinceId,  "&count=", Count]),
    io:format("url is: ~p ~n", [Url]),    
    try 
	case httpc:request(get, {Url, Header},HttpOption,[{sync, false} | RequestOption]) of
	    {ok, RequestId} ->
		receive 
		    {http, {RequestId, Result}} -> 
			case Result of 
			    {error, AReason} ->
				Result;
			    {{NewVersion, 200, NewReasonPhrase}, NewHeaders, NewBody} ->
				case jiffy:decode(NewBody) of
				    {Data} when is_list(Data) ->
					{_, Since} = lists:keyfind(<<"since_id">>, 1, Data),

					{_, Statuses} = lists:keyfind(<<"statuses">>, 1, Data),
					%% Acc1 = lists:foldl(fun({Msg0}, Acc0) ->
					%% 			   {_, Text} = lists:keyfind(<<"text">>, 1, Msg0),
					%% 			   {_, CreatedAt} = lists:keyfind(<<"created_at">>, 1, Msg0),
					%% 			   {_, Pics} = lists:keyfind(<<"pic_urls">>, 1, Msg0),
					%% 			   Pics0 = lists:foldl(fun({Pic}, PicAcc0) ->
					%% 						      {_, PicUrl} = lists:keyfind(<<"thumbnail_pic">>, 1, Pic), 
					%% 						      [PicUrl | PicAcc0]
					%% 					      end, [], Pics),
					%% 			   io:format("pics is: ~p ~n", [Pics0]),
					%% 			   {_, {User}} = lists:keyfind(<<"user">>, 1, Msg0),
					%% 			   {_, AuthorName} = lists:keyfind(<<"name">>, 1, User),
					%% 			   {_, AuthorLink} = lists:keyfind(<<"domain">>, 1, User),
					%% 			   [{Id, Channel, Text, AuthorName, AuthorLink, CreatedAt, Pics0} | Acc0]
					%% 		   end, [], Statuses),
					Acc1 = parse_statuses(Statuses, []),
					gebot_mongo:save_weibo_msg(Id, Channel, Acc1),
					case Since of
					    Since0 when Since > 0 ->
						Args = [{new_weibo, Channel, Id, length(Acc1)}, 
							list_to_binary(XmppServer)],
						io:format("rpc node: ~p , args is : ~p ~n", [XmppNode,Args]),
						rpc:call(XmppNode, cobber_channel, pub_event, Args),
						gen_server:call(ParentPid, {setsince, Channel, Tokens, Since});
					    _ -> ok
					end;
				    _ -> ok
				end,
				{ok, Result}
			end
		after 20000 -> 
			io:format("request: ~p timeout! ~n", [Url]),
			{error, timeout} 
		end;
	    Error ->
		io:format("request: ~p error! ~n", [Url]),
		Error
	end
    catch
	Reason -> 
	    io:format("request weibo failed: ~p ~n", [Reason])
	    %% {error, Reason}
    end,

    sleep_accounting_for_max(Millis),
    run_task({sleeper, Millis}, Id, Tokens, Channel,XmppNode, XmppServer, ParentPid);
run_task(_Schedule,_Id,  _Tokens, _Channel, _, _, _ParentPid) ->
    ok.

parse_statuses([], Acc) ->
    Acc;
parse_statuses([Status | Rest], Acc) ->
    Acc0 = [parse_status(Status) | Acc],
    parse_statuses(Rest, Acc0).

parse_status({Msg0}) ->
    {_, Text} = lists:keyfind(<<"text">>, 1, Msg0),
    {_, CreatedAt} = lists:keyfind(<<"created_at">>, 1, Msg0),
    Pics0 = case lists:keyfind(<<"pic_urls">>, 1, Msg0) of
	       {ok, Pics} ->
		   lists:foldl(fun({Pic}, PicAcc0) ->
				       {_, PicUrl} = lists:keyfind(<<"thumbnail_pic">>, 1, Pic), 
				       [PicUrl | PicAcc0]
			       end, [], Pics);
		_ -> []
	    end,
    {_, {User}} = lists:keyfind(<<"user">>, 1, Msg0),
    {_, AuthorName} = lists:keyfind(<<"name">>, 1, User),
    {_, AuthorLink} = lists:keyfind(<<"profile_url">>, 1, User),
    io:format("msg0 : ~p ~n", [Msg0]),
    Retweet = case lists:keyfind(<<"retweeted_status">>, 1, Msg0) of
		  {<<"retweeted_status">>, RetweetStatus} ->
		      parse_sub_status(RetweetStatus);
		  _ -> {}
	      end,
    {Text, AuthorName, AuthorLink, CreatedAt, Pics0, Retweet}.    

parse_sub_status({Msg0}) ->
    {_, Text} = lists:keyfind(<<"text">>, 1, Msg0),
    {_, CreatedAt} = lists:keyfind(<<"created_at">>, 1, Msg0),
    {_, {User}} = lists:keyfind(<<"user">>, 1, Msg0),
    {_, AuthorName} = lists:keyfind(<<"name">>, 1, User),
    {_, AuthorLink} = lists:keyfind(<<"profile_url">>, 1, User),
    {text, Text, author_name, AuthorName, author_link, AuthorLink, created_at, CreatedAt}.    


get_http_option() ->
    %% ---------------------------------------------------------
    %% WEB
    %% ---------------------------------------------------------

    Header =  [
	       {"User-Agent", "Mozilla/5.0 ebot/1.0-snapshot"},
	       {"Accept-Charset", "utf-8"},
	       {"Accept", "text/xml, application/json, application/xml, application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8"},
	       {"Accept-Language", "en-us,en;q=0.5"}
	      ],
    HttpOptions = [{autoredirect, true}, 
		   {timeout, 10000}
		  ],
    RequestOptions = [],	
    {Header, HttpOptions, RequestOptions}.

-define(LONG_SLEEP_TIME, 100000000).

sleep_accounting_for_max(TimeInMillis) ->
       case (TimeInMillis > ?LONG_SLEEP_TIME) of 
	       true -> timer:sleep(TimeInMillis rem ?LONG_SLEEP_TIME), long_sleep(TimeInMillis div ?LONG_SLEEP_TIME);
	       false -> timer:sleep(TimeInMillis)
       end.

long_sleep(0) -> ok;
long_sleep(Chunks) -> 
	timer:sleep(?LONG_SLEEP_TIME),
	long_sleep(Chunks - 1).

%% -----------------------------------------------------
handle_call(sinceid, _From, State) ->
    {reply, State#state.since_id, State};
handle_call({setsince, Channel, Token, Since}, _From, State) ->
    gebot_mongo:set_since_id(Channel, Token, Since),
    {reply, ok, State#state{since_id=Since}};
handle_call(status, _From, State) ->
     {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @end
%%--------------------------------------------------------------------

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------

terminate(_Reason, State) ->
    exit(State#state.task_pid, kill),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-module(gebot_mongo).

-author('liuhao').

%% API
-export([start/0,
         start_link/0,
	 init/1,
	 objectid_to_binary/1,
	 binary_to_objectid/1, 
	 get_weibo_binding/0,
	 save_weibo_msg/1,
	 get_since_id/2,
	 set_since_id/3
	]).


-define(DEFAULT_MAX_OVERFLOW, 15).
-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_START_INTERVAL, 30). % 30 seconds
-define(DEFAULT_MONGO_HOST, "127.0.0.1").
-define(DEFAULT_MONGO_PORT, 27017).

% time to wait for the supervisor to start its child before returning
% a timeout error to the request
-define(CONNECT_TIMEOUT, 5000). % milliseconds
-define(MONGOPOOL, getbot_mongopool).
-define(MAX, 50000).
-define(COLL_BINDING, bindings).
-define(COLL_TEAM, teams).
-define(COLL_CHANNEL, channels).
-define(COLL_MSG, messages).

-define(FIELD_USER, uid).
-define(FIELD_TYPE, type).
-define(FIELD_TS, ts).
-define(FIELD_TEXT, text).
-define(FIELD_CHANNEL, channel).
-define(FIELD_FILE, file).
-define(FIELD_STATUS, status).

start() ->
    SupervisorName = ?MODULE,	       
    PoolSize = get_pool_size(),
    Server = get_mongo_server(),
    Port = get_mongo_port(),
    Db = get_mongo_db(),
    User = get_mongo_user(),
    Pwd = get_mongo_pwd(),
    Maxoverflow = get_max_overflow(),
    ChildSpec = mongo_pool:child_spec(?MONGOPOOL, PoolSize, Server, Port, Db, Maxoverflow),
    io:format("user : ~p , pwd: ~p ~n", [User, Pwd]),
    case supervisor:start_child(gebot_sup, ChildSpec) of
	{ok, _Pid} ->
	    case gen_server:call(?MONGOPOOL, get_avail_workers) of
		Workers when is_list(Workers) ->
		    F = fun(Worker) ->
				mongo:auth(Worker, User, Pwd) end,
		    [F(X) || X <- Workers];
		_ ->   ok
             end,
	     ok;
	_Error ->
	    io:format("Start of supervisor ~p failed:~n~p~nRetrying...~n",
                       [SupervisorName, _Error]),
            timer:sleep(5000),
	    start()
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

save_weibo_msg(Docs) ->
    Acc1 = lists:foldl(fun({Cid, Content, AuthorName, AuthorLink}, Acc0) ->
			       Ts = get_now_utc(),
			       Doc = {
				 ?FIELD_USER, <<"weibo">>,
				 ?FIELD_TYPE, <<"weibo">>,
				 ?FIELD_CHANNEL, Cid,
				 ?FIELD_TEXT, <<"">>,
				 ?FIELD_TS, Ts,
				 ?FIELD_STATUS, 0,
				 attach, {
				   color, <<"#36a64f">>,
				   author_name, AuthorName, 
				   author_link, AuthorLink,
				   text, Content
				  }
				},
			       [Doc | Acc0]
		       end, [], Docs),
    mongo_pool:insert(?MONGOPOOL, ?COLL_MSG, Acc1),
    ok.


init([]) -> ok.
   

get_max_overflow() ->
    {ok, Overflow} = application:get_env(gebot, db_overflow),
    case Overflow of
	N when is_integer(N), N >= 1, N < 30 ->
	    N;
	_ ->
	    ?DEFAULT_MAX_OVERFLOW
    end.

get_pool_size() ->
    {ok, Poolsize} = application:get_env(gebot, db_pool_size),
    case Poolsize of
	N when is_integer(N),  N >= 1, N < 30 -> 
	    N;
	_ ->
	    ?DEFAULT_POOL_SIZE
    end.

get_mongo_server() ->
    {ok, DbHost} = application:get_env(gebot, db_host),
    case DbHost of
	N when is_list(N) -> 
	    N;
	_ ->
	    ?DEFAULT_MONGO_HOST
    end.

get_mongo_port() ->
    {ok, DbPort} = application:get_env(gebot, db_port),
    case DbPort of
	N when is_integer(N), N > 1, N < 65536 -> 
	    N;
	_ ->
	    ?DEFAULT_MONGO_PORT
    end.

get_mongo_db() ->
    {ok, DbName} = application:get_env(gebot, db_name),
    case DbName of
	N when is_atom(N) -> 
	    N;
	_ ->
	    test
    end.

get_mongo_user() ->
    {ok, DbUser} = application:get_env(gebot, db_user),
    case DbUser of
	N when is_list(N) -> 
	    list_to_binary(N);
	_ ->
	    <<"geteasier">>
    end.

get_mongo_pwd() ->
    {ok, DbPwd} = application:get_env(gebot, db_pwd),
    case DbPwd of
	N when is_list(N) -> 
	    list_to_binary(N);
	_ ->
	    <<"geteasier12">>
    end.

get_weibo_binding() ->
    Docs = mongo_pool:find(?MONGOPOOL, ?COLL_BINDING, {'app', <<"weibo">>, 'status', 0}, {'cid', 1, 'token', 1}),
    case Docs of
	false -> [];
	none -> [];
	Cursor ->
	    Rs = take(Cursor, ?MAX, desc),
	    Result = lists:foldl(fun(X, Acc) ->
					 Channel = bson:lookup(cid, X, <<"">>),
					 Token = bson:lookup(token, X, <<"">>),
					 [{Channel, Token} | Acc] end, [], Rs)
    end.

get_since_id(Channel, Token) ->
    Doc = mongo_pool:find_one(?MONGOPOOL, ?COLL_BINDING, {'cid', Channel, 'token', Token}, {'since', 1}),
    case Doc of
	{Doc1} ->
	    case bson:lookup(since, Doc1) of
		{M}  ->
		    M;
		_ ->
		    0
	    end;
	_ ->
	    0
    end.

set_since_id(Channel, Token, SinceId) ->
    mongo_pool:update(?MONGOPOOL, ?COLL_BINDING, {'cid', Channel, 'token', Token}, {'$set', {'since', SinceId}}).
    
get_now_utc() ->
    %% calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - 62167219200.
    {A, B, C} = erlang:now(),
    A*1000000+B+C*0.000001.

take(Cursor, Count, Order) ->
    Result = take_inner(Cursor, Count, []),
    mc_cursor:close(Cursor),
    case Order of
        desc -> Result;
        asc  -> lists:reverse(Result)
    end.

take_inner(Cursor, Count, Acc) when Count > 0 ->
    case mc_cursor:next(Cursor) of
        {}  -> Acc;
        {X} -> take_inner(Cursor, Count-1, [X|Acc])
    end;
take_inner(_Cursor, _Count, Acc) -> Acc.

iolist_to_list(IOList) ->
    binary_to_list(iolist_to_binary(IOList)).

objectid_to_binary({Id}) -> objectid_to_binary(Id, []).

objectid_to_binary(<<>>, Result) ->
    jlib:tolower(list_to_binary(lists:reverse(Result)));
objectid_to_binary(<<Hex:8, Bin/binary>>, Result) ->
    SL1 = erlang:integer_to_list(Hex, 16),
    SL2 = case erlang:length(SL1) of
        1 -> ["0"|SL1];
        _ -> SL1
    end,
    objectid_to_binary(Bin, [SL2|Result]).

binary_to_objectid(BS) -> binary_to_objectid(BS, []).

binary_to_objectid(<<>>, Result) ->
    {list_to_binary(lists:reverse(Result))};
binary_to_objectid(<<BS:2/binary, Bin/binary>>, Result) ->
    binary_to_objectid(Bin, [erlang:binary_to_integer(BS, 16)|Result]).



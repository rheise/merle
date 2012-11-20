%% Copyright 2009, Joe Williams <joe@joetify.com>
%% Copyright 2009, Nick Gerakines <nick@gerakines.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% @author Joseph Williams <joe@joetify.com>
%% @copyright 2008 Joseph Williams
%% @version 0.3
%% @seealso http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
%% @doc An Erlang memcached client.
%%
%% This code is available as Open Source Software under the MIT license.
%%
%% Updates at http://github.com/joewilliams/merle/

-module(merle).
-behaviour(gen_server2).

-author("Joe Williams <joe@joetify.com>").
-version("Version: 0.3").

-define(SERVER, ?MODULE).
-define(TIMEOUT, 1000).
-define(RANDOM_MAX, 65535).
-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 11211).
-define(DEFAULT_POOL_SIZE, 10).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).


-record(state, {
    free_connections = queue:new(),
    busy_connections = [],
    host,
    port,
    tcp_options = ?TCP_OPTS
}).


%% gen_server API
-export([
    stats/0, stats/1, version/0, getkey/1, delete/2, set/4, add/4, replace/2,
    replace/4, cas/5, set/2, flushall/0, flushall/1, verbosity/1, add/2,
    cas/3, getskey/1, connect/0, connect/2, delete/1, disconnect/0
]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

%% @doc retrieve memcached stats
stats() ->
	gen_server2:call(?SERVER, {stats}).

%% @doc retrieve memcached stats based on args
stats(Args) when is_atom(Args)->
	stats(atom_to_list(Args));
stats(Args) ->
	gen_server2:call(?SERVER, {stats, {Args}}).

%% @doc retrieve memcached version
version() ->
	gen_server2:call(?SERVER, {version}).

%% @doc set the verbosity level of the logging output
verbosity(Args) when is_integer(Args) ->
	verbosity(integer_to_list(Args));
verbosity(Args)->
	case gen_server2:call(?SERVER, {verbosity, {Args}}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc invalidate all existing items immediately
flushall() ->
	case gen_server2:call(?SERVER, {flushall}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc invalidate all existing items based on the expire time argument
flushall(Delay) when is_integer(Delay) ->
	flushall(integer_to_list(Delay));
flushall(Delay) ->
	case gen_server2:call(?SERVER, {flushall, {Delay}}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc retrieve value based off of key
getkey(Key) when is_atom(Key) ->
	getkey(atom_to_list(Key));
getkey(Key) ->
	case gen_server2:call(?SERVER, {getkey,{Key}}) of
	    ["END"] -> undefined;
	    [X] -> X
	end.

%% @doc retrieve value based off of key for use with cas
getskey(Key) when is_atom(Key) ->
	getskey(atom_to_list(Key));
getskey(Key) ->
	case gen_server2:call(?SERVER, {getskey,{Key}}) of
	    ["END"] -> undefined;
	    [X] -> X
	end.

%% @doc delete a key
delete(Key) ->
	delete(Key, "0").

delete(Key, Time) when is_atom(Key) ->
	delete(atom_to_list(Key), Time);
delete(Key, Time) when is_integer(Time) ->
	delete(Key, integer_to_list(Time));
delete(Key, Time) ->
	case gen_server2:call(?SERVER, {delete, {Key, Time}}) of
		["DELETED"] -> ok;
		["NOT_FOUND"] -> not_found;
		[X] -> X
	end.

%% Time is the amount of time in seconds
%% the client wishes the server to refuse
%% "add" and "replace" commands with this key.

%%
%% Storage Commands
%%

%% *Flag* is an arbitrary 16-bit unsigned integer (written out in
%% decimal) that the server stores along with the Value and sends back
%% when the item is retrieved.
%%
%% *ExpTime* is expiration time. If it's 0, the item never expires
%% (although it may be deleted from the cache to make place for other
%%  items).
%%
%% *CasUniq* is a unique 64-bit value of an existing entry.
%% Clients should use the value returned from the "gets" command
%% when issuing "cas" updates.
%%
%% *Value* is the value you want to store.

%% @doc Store a key/value pair.
set(Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    set(Key, integer_to_list(Flag), "0", Value).

set(Key, Flag, ExpTime, Value) when is_atom(Key) ->
	set(atom_to_list(Key), Flag, ExpTime, Value);
set(Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    set(Key, integer_to_list(Flag), ExpTime, Value);
set(Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    set(Key, Flag, integer_to_list(ExpTime), Value);
set(Key, Flag, ExpTime, Value) ->
	case gen_server2:call(?SERVER, {set, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Store a key/value pair if it doesn't already exist.
add(Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	add(Key, integer_to_list(Flag), "0", Value).

add(Key, Flag, ExpTime, Value) when is_atom(Key) ->
	add(atom_to_list(Key), Flag, ExpTime, Value);
add(Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    add(Key, integer_to_list(Flag), ExpTime, Value);
add(Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    add(Key, Flag, integer_to_list(ExpTime), Value);
add(Key, Flag, ExpTime, Value) ->
	case gen_server2:call(?SERVER, {add, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Replace an existing key/value pair.
replace(Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	replace(Key, integer_to_list(Flag), "0", Value).

replace(Key, Flag, ExpTime, Value) when is_atom(Key) ->
	replace(atom_to_list(Key), Flag, ExpTime, Value);
replace(Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    replace(Key, integer_to_list(Flag), ExpTime, Value);
replace(Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    replace(Key, Flag, integer_to_list(ExpTime), Value);
replace(Key, Flag, ExpTime, Value) ->
	case gen_server2:call(?SERVER, {replace, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Store a key/value pair if possible.
cas(Key, CasUniq, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	cas(Key, integer_to_list(Flag), "0", CasUniq, Value).

cas(Key, Flag, ExpTime, CasUniq, Value) when is_atom(Key) ->
	cas(atom_to_list(Key), Flag, ExpTime, CasUniq, Value);
cas(Key, Flag, ExpTime, CasUniq, Value) when is_integer(Flag) ->
    cas(Key, integer_to_list(Flag), ExpTime, CasUniq, Value);
cas(Key, Flag, ExpTime, CasUniq, Value) when is_integer(ExpTime) ->
    cas(Key, Flag, integer_to_list(ExpTime), CasUniq, Value);
cas(Key, Flag, ExpTime, CasUniq, Value) when is_integer(CasUniq) ->
    cas(Key, Flag, ExpTime, integer_to_list(CasUniq), Value);
cas(Key, Flag, ExpTime, CasUniq, Value) ->
	case gen_server2:call(?SERVER, {cas, {Key, Flag, ExpTime, CasUniq, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc connect to memcached with defaults
connect() ->
	connect(?DEFAULT_HOST, ?DEFAULT_PORT).

%% @doc connect to memcached
connect(Host, Port) ->
	start_link(Host, Port, []).

%% @doc disconnect from memcached
disconnect() ->
	gen_server2:cast(?SERVER, stop).

%% @private
start_link(Host, Port, Options) ->
    PoolSize = proplists:get_value(connections_pool_size,
                                   Options, ?DEFAULT_POOL_SIZE),
    State = #state {
      host = Host,
      port = Port
     },
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [State, PoolSize], []).

%% @private
init([State, PoolSize]) ->
    process_flag(trap_exit, true),
    NewState = lists:foldl(
                 fun (_, S=#state{free_connections=Free}) ->
                         {ok, Pid} = spawn_client(State),
                         S#state{free_connections=queue:in(Pid, Free)}
                 end,
                 State, lists:duplicate(PoolSize, 0)),
    {ok, NewState}.

handle_call({stats}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"stats">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({stats, {Args}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"stats ">>, Args]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({version}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"version">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({verbosity, {Args}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"verbosity ">>, Args]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({flushall}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"flush_all">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({flushall, {Delay}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(Socket, iolist_to_binary([<<"flush_all ">>, Delay]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({getkey, {Key}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_get_cmd(Socket, iolist_to_binary([<<"get ">>, Key]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({getskey, {Key}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_gets_cmd(Socket, iolist_to_binary([<<"gets ">>, Key]))
                             end,
                             From,
                             State),
    {reply, [Reply], NewState};

handle_call({delete, {Key, Time}}, From, State) ->
    {NewState, Socket} = get_socket(From, State),
    Reply = send_generic_cmd(
        Socket,
        iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])
    ),
    {reply, Reply, NewState};

handle_call({set, {Key, Flag, ExpTime, Value}}, From, State) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                             send_storage_cmd(
                               Socket,
                               iolist_to_binary([
                                                 <<"set ">>, Key,
                                                 <<" ">>, Flag, <<" ">>,
                                                 ExpTime, <<" ">>, Bytes
                                                ]),
                               Bin
                              )
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({add, {Key, Flag, ExpTime, Value}}, From, State) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                             send_storage_cmd(
                               Socket,
                               iolist_to_binary([
                                                 <<"add ">>, Key, <<" ">>,
                                                 Flag, <<" ">>, ExpTime, <<" ">>, Bytes
                                                ]),
                               Bin
                              )
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({replace, {Key, Flag, ExpTime, Value}}, From, State) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                             send_storage_cmd(
                               Socket,
                               iolist_to_binary([
                                                 <<"replace ">>, Key, <<" ">>,
                                                 Flag, <<" ">>, ExpTime, <<" ">>,
                                                 Bytes
                                                ]),
                               Bin
                              )
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, From, State) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                          send_storage_cmd(
                            Socket,
                            iolist_to_binary([
                                              <<"cas ">>, Key, <<" ">>,
                                              Flag, <<" ">>,
                                              ExpTime, <<" ">>, Bytes,
                                              <<" ">>, CasUniq
                                             ]),
                            Bin
                           )
                             end,
                             From, State),
    {reply, Reply, NewState}.

%% @private
handle_cast(stop, State) ->
   {stop, normal, State};

handle_cast({free, Socket}, State) ->
    {noreply, process_freed(Socket, State)};

handle_cast({close, Pid, Reason}, State) ->
    {noreply, process_close(Pid, Reason, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% handling sockets faliure
handle_info({'EXIT', Pid, Reason}, State) ->
    {noreply, process_close(Pid, Reason, State)};

handle_info(_Info, State) -> {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% @doc Closes the socket
terminate(_Reason, #state{free_connections=Free, busy_connections=Busy}) ->
    lists:foreach(
        fun (Pid) ->
                gen_tcp:close(Pid)
        end,
        queue:to_list(Free)),
    %% Need a better way to wait for connections to finish
    timer:sleep(1000),
    lists:foreach(
      fun ({_, Pid}) ->
              gen_tcp:close(Pid)
      end,
      Busy),
    ok.

spawn_client(#state{host=Host, port=Port, tcp_options=TCPOpts}) ->
    gen_tcp:connect(Host, Port, TCPOpts).


%% @private
exec(Fun, FromPid, State) ->
    {CurrentState, Socket} = get_socket(FromPid, State),
    Reply = try Fun(Socket)
            catch C:E ->
                    erlang:raise(C, E, erlang:get_stacktrace())
            after
                gen_server2:cast(?SERVER, {free, Socket})
            end,
    NewState = case Reply of
        timeout -> process_close(Socket, timeout, CurrentState);
        connection_closed -> process_close(Socket, connection_closed, CurrentState);
        _ -> CurrentState
    end,
    {NewState, Reply}.

%% @private
process_close(Pid, Reason, State) ->
    Busy = State#state.busy_connections,
    Free = queue:to_list(State#state.free_connections),
    NewState = State#state {
                 busy_connections = remove_from_busy(Pid, Busy),
                 free_connections = queue:from_list(Free -- [Pid])
                },
    gen_tcp:close(Pid),
    NewState.

%% @private
process_freed(Pid, State) ->
    Free = State#state.free_connections,
    Busy = State#state.busy_connections,
    State#state{
        free_connections = queue:in(Pid, Free),
        busy_connections = remove_from_busy(Pid, Busy)
    }.


remove_from_busy(Pid, Busy) ->
    case lists:keyfind(Pid, 2, Busy) of
        {_ParentPid, Pid} -> lists:keydelete(Pid, 2, Busy);
        _ -> Busy
    end.


%% @private
%%
get_socket(FromPid, State) ->
    Free = State#state.free_connections,
    Busy = State#state.busy_connections,
    {Result, NewFree} = case queue:is_empty(Free) of
        false ->
            {{ok, queue:get(Free)}, queue:drop(Free)};
        true ->
            {spawn_client(State), Free}
    end,
    case Result of
        {ok, Pid} ->
            NewState = State#state{
                free_connections = NewFree,
                busy_connections = [{FromPid, Pid} | Busy]
            },
            {NewState, Pid};
        {error, Reason} ->
            {State, {error, Reason}}
    end.

%% @private
%% @doc send_generic_cmd/2 function for simple informational and deletion commands
send_generic_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_simple_reply(),
	Reply.

%% @private
%% @doc send_storage_cmd/3 funtion for storage commands
send_storage_cmd(Socket, Cmd, Value) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(),
   	Reply.

%% @private
%% @doc send_get_cmd/2 function for retreival commands
send_get_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_get_reply(Socket),
	Reply.

%% @private
%% @doc send_gets_cmd/2 function for cas retreival commands
send_gets_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_gets_reply(Socket),
	Reply.

%% @private
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply() ->
	receive
	  	{tcp,_,Data} ->
        	string:tokens(binary_to_list(Data), "\r\n");
        {error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc receive function for respones containing VALUEs
recv_complex_get_reply(Socket) ->
	receive
		%% For receiving get responses where the key does not exist
		{tcp, Socket, <<"END\r\n">>} -> ["END"];
		%% For receiving get responses containing data
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes], ListBin} = Parse,
  			Bin = list_to_binary(ListBin),
  			Reply = get_data(Socket, Bin, Bytes, length(ListBin)),
  			[Reply];
  		{error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc receive function for cas responses containing VALUEs
recv_complex_gets_reply(Socket) ->
	receive
		%% For receiving get responses where the key does not exist
		{tcp, Socket, <<"END\r\n">>} -> ["END"];
		%% For receiving get responses containing data
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes,CasUniq], ListBin} = Parse,
  			Bin = list_to_binary(ListBin),
  			Reply = get_data(Socket, Bin, Bytes, length(ListBin)),
  			[CasUniq, Reply];
  		{error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc recieve loop to get all data
get_data(Socket, Bin, Bytes, Len) when Len < Bytes + 7->
    receive
        {tcp, Socket, Data} ->
            Combined = <<Bin/binary, Data/binary>>,
            get_data(Socket, Combined, Bytes, size(Combined));
     	{error, closed} ->
  			connection_closed
        after ?TIMEOUT -> timeout
    end;
get_data(_, Data, Bytes, _) ->
	<<Bin:Bytes/binary, "\r\nEND\r\n">> = Data,
    binary_to_term(Bin).

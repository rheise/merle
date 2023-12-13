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

-module(merle_connect).
-behaviour(gen_server2).

-author("Joe Williams <joe@joetify.com>").

-include("merle.h").


%% gen_server callbacks
-export([
         init/1,
         start_link/3, start_link/4, start_link/5,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2,
         code_change/3
]).



start_link(Name, Host, Port) ->
    start_link(Name, Host, Port, ?TIMEOUT, []).

start_link(Name, Host, Port, RequestTimeout) ->
    start_link(Name, Host, Port, RequestTimeout, []).

%% @private
start_link(Name, Host, Port, RequestTimeout, Options) ->
    PoolSize = proplists:get_value(connections_pool_size,
                                   Options, ?DEFAULT_POOL_SIZE),
    MaxConnections = proplists:get_value(max_connections,
                                         Options, 0),
    State = #state {
      host = Host,
      port = Port,
      max  = MaxConnections,
      name = Name,
      request_timeout = RequestTimeout
     },
    gen_server2:start_link({local, Name}, ?MODULE, [State, PoolSize], []).

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
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"stats">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({stats, {Args}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"stats ">>, Args]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({version}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"version">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({verbosity, {Args}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"verbosity ">>, Args]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({flushall}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"flush_all">>]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({flushall, {Delay}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"flush_all ">>, Delay]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({getkey, {Key}, IsTerm}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_get_cmd(State, Socket, iolist_to_binary([<<"get ">>, Key]), IsTerm)
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({getskey, {Key}, IsTerm}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_gets_cmd(State, Socket, iolist_to_binary([<<"gets ">>, Key]), IsTerm)
                             end,
                             From,
                             State),
    {reply, [Reply], NewState};

handle_call({delete, {Key, Time}}, From, State) ->
    {NewState, Reply} = exec(fun (Socket) ->
                             send_generic_cmd(State, Socket, iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time]))
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({set, {Key, Flag, ExpTime, Value, IsTerm}}, From, State) ->
    {NewState, Reply} = do_set(Key, Flag, ExpTime, Value, IsTerm, From, State),
    {reply, Reply, NewState};

handle_call({add, {Key, Flag, ExpTime, Value, IsTerm}}, From, State) ->
    Bin = encode(IsTerm, Value),
    Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                             send_storage_cmd(State, Socket,
                               iolist_to_binary([
                                                 <<"add ">>, Key, <<" ">>,
                                                 Flag, <<" ">>, ExpTime, <<" ">>, Bytes
                                                ]),
                               Bin
                              )
                             end,
                             From, State),
    {reply, Reply, NewState};

handle_call({replace, {Key, Flag, ExpTime, Value, IsTerm}}, From, State) ->
    Bin = encode(IsTerm, Value),
    Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                             send_storage_cmd(State, Socket,
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
handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value, IsTerm}}, From, State) ->
    Bin = encode(IsTerm, Value),
    Bytes = integer_to_list(size(Bin)),
    {NewState, Reply} = exec(fun (Socket) ->
                          send_storage_cmd(State, Socket,
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
    {reply, Reply, NewState};
handle_call(get_state, _From, State) ->
    {reply, {ok, State}, State}.


%% @private
handle_cast(stop, State) ->
   {stop, normal, State};

handle_cast({free, Socket}, State) ->
    {noreply, process_freed(Socket, State)};

handle_cast({close, Pid, _Reason}, State) ->
    {noreply, process_close(Pid, State)};

handle_cast({set, {Key, Flag, ExpTime, Value, From}}, State) ->
    {NewState, _Reply} = do_set(Key, Flag, ExpTime, Value, true, From, State),
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% handling sockets faliure
handle_info({'EXIT', Pid, _Reason}, State) ->
    {noreply, process_close(Pid, State)};

handle_info(_Info, State) -> {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.


do_set(Key, Flag, ExpTime, Value, IsTerm, From, State) ->
    Bin = encode(IsTerm, Value),
    Bytes = integer_to_list(size(Bin)),
    exec(fun (Socket) ->
                 send_storage_cmd(State, Socket,
                                  iolist_to_binary([
                                                 <<"set ">>, Key,
                                                 <<" ">>, Flag, <<" ">>,
                                                 ExpTime, <<" ">>, Bytes
                                                ]),
                                  Bin
                                 )
         end,
         From, State).


%% @private
%% @doc Closes the socket
terminate(_Reason, #state{free_connections=Free, busy_connections=Busy}) ->
    try
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
        ok
    catch _:_ -> ok
    end.

spawn_client(#state{host=Host, port=Port, tcp_options=TCPOpts}) ->
    case gen_tcp:connect(Host, Port, TCPOpts) of
        {error, Reason} ->
%%            try error_logger:error_msg("CONNECTION Failed ~p~n", [{Host, Port, TCPOpts, Reason}]) catch _Exception:_Reason -> _A=1 end,
            {error, Reason};
        Res -> Res
    end.


%% @private
exec(Fun, FromPid, State) ->
    Name = State#state.name,
    case get_socket(FromPid, State) of
        {_ErrorState, {error, Reason}} ->
            erlang:error(Reason);
        {CurrentState, Socket} ->
            Reply = try Fun(Socket)
                    catch C:E ->
                            %% An error should close the connection
                            gen_server2:cast(Name, {close, Socket, error}),
                            erlang:raise(C, E, erlang:get_stacktrace())
                    end,
            case Reply of
                timeout -> gen_server2:cast(Name, {close, Socket, timeout});
                connection_closed -> gen_server2:cast(Name, {close, Socket, connection_closed});
                _ -> gen_server2:cast(Name, {free, Socket})
            end,
            {CurrentState, Reply}
    end.

%% @private
process_close(Pid, State) ->
    Busy = State#state.busy_connections,
    Free = State#state.free_connections,
    NewState = State#state {
                 busy_connections = lists:keydelete(Pid, 2, Busy),
                 free_connections = queue:filter(fun(P) ->
                                                      P =/= Pid
                                                 end, Free)
                },
    catch gen_tcp:close(Pid),
    NewState.

%% @private
process_freed(Pid, State) ->
    Free = State#state.free_connections,
    Busy = State#state.busy_connections,
    State#state{
        free_connections = queue:in(Pid, Free),
        busy_connections = lists:keydelete(Pid, 2, Busy)
    }.



%% @private
%%
get_socket(FromPid, State) ->
    Free = State#state.free_connections,
    Busy = State#state.busy_connections,
    NumConnections = queue_count(Free) + length(Busy),
    MaxConnections = case State#state.max of
                         0 -> NumConnections + 1;
                         V -> V
                     end,
    {Result, NewFree} = case queue:is_empty(Free) of
        false ->
            {{ok, queue:get(Free)}, queue:drop(Free)};
        true ->
            if NumConnections < MaxConnections ->
                    {spawn_client(State), Free};
               true ->
                    {{error, max_connections}, Free}
            end
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
queue_count(Q) ->
    case queue:is_empty(Q) of
        true -> 0;
        false -> queue:len(Q)
    end.

%% @private
%% @doc send_generic_cmd/2 function for simple informational and deletion commands
send_generic_cmd(State, Socket, Cmd) ->
    ok = gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    Reply = recv_simple_reply(State),
    Reply.

%% @private
%% @doc send_storage_cmd/3 funtion for storage commands
send_storage_cmd(State, Socket, Cmd, Value) ->
    ok = gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    ok = gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(State),
    Reply.

%% @private
%% @doc send_get_cmd/2 function for retreival commands
send_get_cmd(State, Socket, Cmd, IsTerm) ->
    ok = gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    case recv_complex_get_reply(State, Socket) of
        [Reply] -> [decode(IsTerm, Reply)];
        Other -> Other
    end.

%% @private
%% @doc send_gets_cmd/2 function for cas retreival commands
send_gets_cmd(State, Socket, Cmd, IsTerm) ->
    ok = gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    case recv_complex_gets_reply(State, Socket) of
        [CasUniq, Reply] -> [CasUniq, decode(IsTerm, Reply)];
        Other -> Other
    end.

%% @private
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply(State) ->
    Timeout = State#state.request_timeout,
    receive
        {tcp,_,Data} ->
            string:tokens(binary_to_list(Data), "\r\n");
        {error, closed} ->
            connection_closed
    after Timeout -> timeout
    end.

%% @private
%% @doc receive function for respones containing VALUEs
recv_complex_get_reply(State, Socket) ->
    Timeout = State#state.request_timeout,
    receive
        %% For receiving get responses where the key does not exist
        {tcp, Socket, <<"END\r\n">>} -> ["END"];
        %% For receiving get responses containing data
        {tcp, Socket, Data} ->
            %% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
            Parse = io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
            {ok,[_,_,_,Bytes], ListBin} = Parse,
            Bin = list_to_binary(ListBin),
            case get_data(State, Socket, Bin, Bytes, length(ListBin)) of
                timeout ->           timeout;
                connection_closed -> connection_closed;
                Reply ->             [Reply]
            end;
        {error, closed} ->
            connection_closed
    after Timeout -> timeout
    end.

%% @private
%% @doc receive function for cas responses containing VALUEs
recv_complex_gets_reply(State, Socket) ->
    Timeout = State#state.request_timeout,
    receive
        %% For receiving get responses where the key does not exist
        {tcp, Socket, <<"END\r\n">>} -> ["END"];
        %% For receiving get responses containing data
        {tcp, Socket, Data} ->
            %% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
            Parse = io_lib:fread("~s ~s ~u ~u ~u\r\n", binary_to_list(Data)),
            {ok,[_,_,_,Bytes,CasUniq], ListBin} = Parse,
            Bin = list_to_binary(ListBin),
            case get_data(State, Socket, Bin, Bytes, length(ListBin)) of
                timeout -> timeout;
                connection_closed -> connection_closed;
                Reply -> [CasUniq, Reply]
            end;
        {error, closed} ->
            connection_closed
    after Timeout -> timeout
    end.

%% @private
%% @doc recieve loop to get all data
get_data(State, Socket, Bin, Bytes, Len) when Len < Bytes + 7->
    Timeout = State#state.request_timeout,
    receive
        {tcp, Socket, Data} ->
            Combined = <<Bin/binary, Data/binary>>,
            get_data(State, Socket, Combined, Bytes, size(Combined));
        {error, closed} ->
            connection_closed
    after Timeout -> timeout
    end;
get_data(_, _, Data, Bytes, _) ->
    <<Bin:Bytes/binary, "\r\nEND\r\n">> = Data,
    Bin.

%% @private
%% @doc decode the value to term or list
decode(_, "END") ->
    "END";
decode(true, Value) ->
    binary_to_term(Value);
decode(_, Value) ->
    binary_to_list(Value).

%% @private
%% @doc encode the term or list value to binary
encode(_, Value) when is_binary(Value) ->
    Value;
encode(true, Value) ->
    term_to_binary(Value);
encode(_, Value) ->
    list_to_binary(Value).

%%
%% FIXME: Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.

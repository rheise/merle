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

-module(merle_client).

-author("Joe Williams <joe@joetify.com>").


-include("merle.h").

%% client API
-export([
    stats/1, stats/2, version/1, getkey/2, delete/3, set/5, add/5, replace/3,
    replace/5, cas/6, set/3, flushall/1, flushall/2, verbosity/2, add/3,
    cas/4, getskey/2, connect/1, connect/3, connect/4, connect/5, delete/2,
    disconnect/1, get_state/1, get_state/2
]).

%% @doc retrieve memcached stats
-spec stats(atom()) -> term().
stats(Name) ->
    gen_server2:call(?SERVER, {stats}).

%% @doc retrieve memcached stats based on args
-spec stats(atom(), atom()) -> term().
stats(Name, Args) when is_atom(Args)->
    stats(Name, atom_to_list(Args));
stats(Name, Args) ->
    gen_server2:call(Name, {stats, {Args}}).

%% @doc retrieve memcached version
version(Name) ->
    gen_server2:call(Name, {version}).

%% @doc set the verbosity level of the logging output
verbosity(Name, Args) when is_integer(Args) ->
    verbosity(Name, integer_to_list(Args));
verbosity(Name, Args)->
    case gen_server2:call(Name, {verbosity, {Args}}) of
        ["OK"] -> ok;
        [X] -> X
    end.

%% @doc invalidate all existing items immediately
flushall(Name) ->
    case gen_server2:call(Name, {flushall}) of
        ["OK"] -> ok;
        [X] -> X
    end.

%% @doc invalidate all existing items based on the expire time argument
flushall(Name, Delay) when is_integer(Delay) ->
    flushall(Name, integer_to_list(Delay));
flushall(Name, Delay) ->
    case gen_server2:call(Name, {flushall, {Delay}}) of
        ["OK"] -> ok;
        [X] -> X
    end.

%% @doc retrieve value based off of key
getkey(Name, Key) when is_atom(Key) ->
    getkey(Name, atom_to_list(Key));
getkey(Name, Key) ->
    case gen_server2:call(Name, {getkey,{Key},true}) of
        ["END"] -> undefined;
        [X] -> X
    end.

%% @doc get that plays nicely with Python
%%   doesn't assume values are terms
s_getkey(Name, Key) when is_atom(Key) ->
    s_getkey(Name, atom_to_list(Key));
s_getkey(Name, Key) ->
    case gen_server2:call(Name, {getkey,{Key},false}) of
        ["END"] -> undefined;
        [X] -> X
    end.

%% @doc retrieve value based off of key for use with cas
getskey(Name, Key) when is_atom(Key) ->
    getskey(Name, atom_to_list(Key));
getskey(Key) ->
    case gen_server2:call(Name, {getskey,{Key},true}) of
        ["END"] -> undefined;
        [X] -> X
    end.

%% @doc get with cas that plays nicely with Python
%%   doesn't assume values are terms
s_getskey(Name, Key) when is_atom(Key) ->
    s_getskey(Name, atom_to_list(Key));
s_getskey(Name, Key) ->
    case gen_server2:call(Name, {getskey,{Key},false}) of
        ["END"] -> undefined;
        [X] -> X
    end.

%% @doc delete a key
delete(Name, Key) ->
    delete(Name, Key, "0").

delete(Name, Key, Time) when is_atom(Key) ->
    delete(Name, atom_to_list(Key), Time);
delete(Name, Key, Time) when is_integer(Time) ->
    delete(Name, Key, integer_to_list(Time));
delete(Name, Key, Time) ->
    case gen_server2:call(Name, {delete, {Key, Time}}) of
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
set(Name, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    set(Name, Key, integer_to_list(Flag), "0", Value).

set(Name, Key, Flag, ExpTime, Value) when is_atom(Key) ->
    set(atom_to_list(Key), Flag, ExpTime, Value);
set(Name, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    set(Key, integer_to_list(Flag), ExpTime, Value);
set(Name, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    set(Key, Flag, integer_to_list(ExpTime), Value);
set(Name, Key, Flag, ExpTime, Value) ->
    gen_server2:cast(Name, {set, {Key, Flag, ExpTime, Value, self()}}).


%% @doc set that plays nicely with other languages (ex. Python).
%%   doesn't assume values are terms
%%   doesn't use random flag
s_set(Name, Key, Value) ->
    s_set(Name, Key, "0", "0", Value).

s_set(Name, Key, ExpTime, Value) when is_atom(Key) ->
    s_set(Name, atom_to_list(Key), "0", ExpTime, Value);
s_set(Name, Key, ExpTime, Value) when is_integer(ExpTime) ->
    s_set(Name, Key, "0", integer_to_list(ExpTime), Value);
s_set(Name, Key, ExpTime, Value) ->
    s_set(Name, Key, "0", ExpTime, Value).

s_set(Name, Key, Flag, ExpTime, Value) ->
    set_call(Name, {set, {Key, Flag, ExpTime, Value, false}}).

%% @doc Store a key/value pair if it doesn't already exist.
add(Name, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    add(Name, Key, integer_to_list(Flag), "0", Value).

add(Name, Key, Flag, ExpTime, Value) when is_atom(Key) ->
    add(Name, atom_to_list(Key), Flag, ExpTime, Value);
add(Name, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    add(Name, Key, integer_to_list(Flag), ExpTime, Value);
add(Name, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    add(Name, Key, Flag, integer_to_list(ExpTime), Value);
add(Name, Key, Flag, ExpTime, Value) ->
    set_call(Name, {add, {Key, Flag, ExpTime, Value, true}}).


%% @doc add that plays nicely with Python
%%   doesn't assume values are terms
s_add(Name, Key, Value) ->
    s_add(Name, Key, "0", "0", Value).

s_add(Name, Key, ExpTime, Value) when is_atom(Key) ->
    s_add(Name, atom_to_list(Key), "0", ExpTime, Value);
s_add(Name, Key, ExpTime, Value) when is_integer(ExpTime) ->
    s_add(Name, Key, "0", integer_to_list(ExpTime), Value);
s_add(Name, Key, ExpTime, Value) ->
    s_add(Name, Key, "0", ExpTime, Value).

s_add(Name, Key, Flag, ExpTime, Value) ->
    set_call(Name, {add, {Key, Flag, ExpTime, Value, false}}).


%% @doc Replace an existing key/value pair.
replace(Name, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    replace(Name, Key, integer_to_list(Flag), "0", Value).

replace(Name, Key, Flag, ExpTime, Value) when is_atom(Key) ->
    replace(Name, atom_to_list(Key), Flag, ExpTime, Value);
replace(Name, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    replace(Name, Key, integer_to_list(Flag), ExpTime, Value);
replace(Name, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    replace(Name, Key, Flag, integer_to_list(ExpTime), Value);
replace(Name, Key, Flag, ExpTime, Value) ->
    set_call(Name, {replace, {Key, Flag, ExpTime, Value, true}}).


%% @doc replace that places nice with Python
%%   doesn't assume values are terms
s_replace(Name, Key, Value) ->
    s_replace(Name, Key, "0", "0", Value).

s_replace(Name, Key, ExpTime, Value) when is_atom(Key) ->
    s_replace(Name, atom_to_list(Key), "0", ExpTime, Value);
s_replace(Name, Key, ExpTime, Value) when is_integer(ExpTime) ->
    s_replace(Name, Key, "0", integer_to_list(ExpTime), Value);
s_replace(Name, Key, ExpTime, Value) ->
    s_replace(Name, Key, "0", ExpTime, Value).

s_replace(Name, Key, Flag, ExpTime, Value) ->
    set_call(Name, {replace, {Key, Flag, ExpTime, Value, false}}).


%% @doc Store a key/value pair if possible.
cas(Name, Key, CasUniq, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    cas(Name, Key, integer_to_list(Flag), "0", CasUniq, Value).

cas(Name, Key, Flag, ExpTime, CasUniq, Value) when is_atom(Key) ->
    cas(Name, atom_to_list(Key), Flag, ExpTime, CasUniq, Value);
cas(Name, Key, Flag, ExpTime, CasUniq, Value) when is_integer(Flag) ->
    cas(Name, Key, integer_to_list(Flag), ExpTime, CasUniq, Value);
cas(Name, Key, Flag, ExpTime, CasUniq, Value) when is_integer(ExpTime) ->
    cas(Name, Key, Flag, integer_to_list(ExpTime), CasUniq, Value);
cas(Name, Key, Flag, ExpTime, CasUniq, Value) when is_integer(CasUniq) ->
    cas(Name, Key, Flag, ExpTime, integer_to_list(CasUniq), Value);
cas(Name, Key, Flag, ExpTime, CasUniq, Value) ->
    set_call(Name, {cas, {Key, Flag, ExpTime, CasUniq, Value, true}}).

%% @doc cas that plays nice with Python
%%   doesn't assume values are terms
s_cas(Name, Key, CasUniq, Value) ->
    s_cas(Name, Key, "0", "0", CasUniq, Value).

s_cas(Name, Key, ExpTime, CasUniq, Value) when is_atom(Key) ->
    s_cas(Name, atom_to_list(Key), "0", ExpTime, CasUniq, Value);
s_cas(Name, Key, ExpTime, CasUniq, Value) when is_integer(ExpTime) ->
    s_cas(Name, Key, "0", integer_to_list(ExpTime), CasUniq, Value);
s_cas(Name, Key, ExpTime, CasUniq, Value) when is_integer(CasUniq) ->
    s_cas(Name, Key, "0", ExpTime, integer_to_list(CasUniq), Value);
s_cas(Name, Key, ExpTime, CasUniq, Value) ->
    s_cas(Name, Key, "0", ExpTime, CasUniq, Value).

s_cas(Name, Key, Flag, ExpTime, CasUniq, Value) ->
    set_call(Name, {cas, {Key, Flag, ExpTime, CasUniq, Value, false}}).

set_call(Name, Msg) ->
    case gen_server2:call(Name, Msg) of
        ["STORED"] -> ok;
        ["NOT_STORED"] -> not_stored;
        [X] -> X;
        Error -> {error, Error}
    end.


%% @doc connect to memcached with defaults
connect(Name) ->
    connect(Name, ?DEFAULT_HOST, ?DEFAULT_PORT).

%% @doc connect to memcached
connect(Name, Host, Port) ->
    connect(Name, Host, Port, ?TIMEOUT, []).

%% @doc connect to memcached
connect(Name, Host, Port, Opts) when is_list(Opts)->
    connect(Name, Host, Port, ?TIMEOUT, Opts);
connect(Name, Host, Port, RequestTimeout) ->
    connect(Name, Host, Port, RequestTimeout, []).

%% @doc connect with opts to memcached
connect(Name, Host, Port, RequestTimeout, Opts) ->
    merle_connect:start_link(Name, Host, Port, RequestTimeout, Opts).

%% @doc disconnect from memcached
disconnect(Name) ->
    gen_server2:cast(Name, stop).

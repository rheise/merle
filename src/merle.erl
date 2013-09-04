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

-include("merle.h");

%% API
-export([
    stats/0, stats/1, version/0, getkey/1, delete/2, set/4, add/4, replace/2,
    replace/4, cas/5, set/2, flushall/0, flushall/1, verbosity/1, add/2,
    cas/3, getskey/1, connect/0, connect/2, connect/3, connect/4, delete/1, disconnect/0
]).

% API -- non-term apis
-export([
       s_getkey/1,
       s_getskey/1,
       s_set/2, s_set/3,
       s_add/2, s_add/3,
       s_replace/2, s_replace/3,
       s_cas/3, s_cas/4
]).


%% @doc retrieve memcached stats
stats() ->
    merle_client:stats(?SERVER, {stats}).

%% @doc retrieve memcached stats based on args
stats(Args) ->
    merle_client:stats(?SERVER, Args).

%% @doc retrieve memcached version
version() ->
    merle_client:version(?SERVER).

%% @doc set the verbosity level of the logging output
verbosity(Args)->
    merle_client:verbosity(?SERVER, Args).

%% @doc invalidate all existing items immediately
flushall() ->
    merle_client:flushall(?SERVER).

%% @doc invalidate all existing items based on the expire time argument
flushall(Delay) ->
    merle_client:flushall(?SERVER, Delay).

%% @doc retrieve value based off of key
getkey(Key) ->
    merle_client:getkey(?SERVER, Key).

%% @doc get that plays nicely with Python
%%   doesn't assume values are terms
s_getkey(Key) ->
    merle_client:s_getkey(?SERVER, Key).

%% @doc retrieve value based off of key for use with cas
getskey(Key) ->
    merle_client:getskey(?SERVER, Key).

%% @doc get with cas that plays nicely with Python
%%   doesn't assume values are terms
s_getskey(Key) ->
    merle_client:s_getskey(?SERVER, Key).

%% @doc delete a key
delete(Key) ->
    merle_client:delete(?SERVER, Key).

delete(Key, Time) ->
    merle_client:delete(?SERVER, Key, Time).

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
    merle_client:set(?SERVER, Key, Value).
set(Key, ExpTime, Value) ->
    merle_client:set(?SERVER, Key, ExpTime, Value).
set(Key, Flag, ExpTime, Value) ->
    merle_client:set(?SERVER, Key, Flag, ExpTime, Value).


%% @doc set that plays nicely with other languages (ex. Python).
%%   doesn't assume values are terms
%%   doesn't use random flag
s_set(Key, Value) ->
    merle_client:s_set(?SERVER, Key, Value).
s_set(Key, ExpTime, Value) ->
    merle_client:s_set(?SERVER, Key, ExpTime, Value).
s_set(Key, Flag, ExpTime, Value) ->
    merle_client:s_set(?SERVER, Key, Flag, ExpTime, Value).

%% @doc Store a key/value pair if it doesn't already exist.
add(Key, Value) ->
    merle_client:add(?SERVER, Key, Value).
add(Key, ExpTime, Value) ->
    merle_client:add(?SERVER, Key, ExpTime, Value).
add(Key, Flag, ExpTime, Value) ->
    merle_client:add(?SERVER, Key, Flag, ExpTime, Value).


%% @doc add that plays nicely with Python
%%   doesn't assume values are terms
s_add(Key, Value) ->
    merle_client:s_add(?SERVER, Key, Value).
s_add(Key, ExpTime, Value) ->
    merle_client:s_add(?SERVER, Key, ExpTime, Value).
s_add(Key, Flag, ExpTime, Value) ->
    merle_client:s_add(?SERVER, Key, Flag, ExpTime, Value).


%% @doc Replace an existing key/value pair.
replace(Key, Value) ->
    merle_client:replace(?SERVER, Key, Value).
replace(Key, Flag, ExpTime, Value) ->
    merle_client:replace(?SERVER, Key, Flag, ExpTime, Value).

%% @doc replace that places nice with Python
%%   doesn't assume values are terms
s_replace(Key, Value) ->
    merle_client:s_replace(?SERVER, Key, Value).
s_replace(Key, ExpTime, Value) ->
    merle_client:s_replace(?SERVER, Key, ExpTime, Value).
s_replace(Key, Flag, ExpTime, Value) ->
    merle_client:s_replace(?SERVER, Key, Flag, ExpTime, Value).

%% @doc Store a key/value pair if possible.
cas(Key, CasUniq, Value) ->
    merle_client:cas(?SERVER, Key, CasUniq, Value).
cas(Key, Flag, ExpTime, CasUniq, Value) ->
    merle_client:cas(?SERVER, Key, Flag, ExpTime, CasUniq, Value).

%% @doc cas that plays nice with Python
%%   doesn't assume values are terms
s_cas(Key, CasUniq, Value) ->
    s_cas(Key, "0", "0", CasUniq, Value).
s_cas(Key, Flag, ExpTime, CasUniq, Value) ->
    merle_client:s_case(?SERVER, Key, Flag, ExpTime, CasUniq, Value).


%% @doc connect to memcached with defaults
connect() ->
    connect(?DEFAULT_HOST, ?DEFAULT_PORT).

%% @doc connect to memcached
connect(Host, Port) ->
    merle_client:connect(?SERVER, Host, Port).

%% @doc connect to memcached
connect(Host, Port, Opts) ->
    merle_client:connect(?SERVER, Host, Port, Opts).

%% @doc connect with opts to memcached
connect(Host, Port, RequestTimeout, Opts) ->
    merle_client:connect(?SERVER, Host, Port, RequestTimeout, Opts).

%% @doc disconnect from memcached
disconnect() ->
    merle_client:disconnect(?SERVER).

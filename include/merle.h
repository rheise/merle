-record(state, {
    free_connections = queue:new(),
    busy_connections = [],
    host,
    port,
    request_timeout,
    max = 0,
    name = ?MODULE,
    tcp_options = ?TCP_OPTS
}).

-define(SERVER, merle_connect).
-define(TIMEOUT, 1000).
-define(RANDOM_MAX, 65535).
-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 11211).
-define(DEFAULT_POOL_SIZE, 10).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).

.. _arch_overview_redis:

Redis
=======

Envoy can act as a Redis proxy, partitioning commands among instances in a cluster.
In this mode, the goals of Envoy are to maintain availability and partition tolerance
over consistency. This is the key point when comparing Envoy to `Redis Cluster
<https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>`_. Envoy is designed as a best-effort cache,
meaning that it will not try to reconcile inconsistent data or keep a globally consistent
view of cluster membership. It also supports routing commands from different workloads to
different upstream clusters based on their access patterns, eviction, or isolation
requirements.

The Redis project offers a thorough reference on partitioning as it relates to Redis. See
"`Partitioning: how to split data among multiple Redis instances
<https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/>`_".

**Features of Envoy Redis**:

* `Redis protocol <https://redis.io/docs/latest/develop/reference/protocol-spec/>`_ codec.
* Hash-based partitioning.
* Redis transaction support.
* Ketama distribution.
* Detailed command statistics.
* Active and passive healthchecking.
* Hash tagging.
* Prefix routing.
* Separate downstream client and upstream server authentication.
* Request mirroring for all requests or write requests only.
* Control :ref:`read requests routing<envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.read_policy>`. This only works with Redis Cluster.

**Planned future enhancements**:

* Additional timing stats.
* Circuit breaking.
* Request collapsing for fragmented commands.
* Replication.
* Built-in retry.
* Tracing.

.. _arch_overview_redis_configuration:

Configuration
-------------

For filter configuration details, see the Redis proxy filter
:ref:`configuration reference <config_network_filters_redis_proxy>`.

Unless :ref:`static_shard_routing <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.static_shard_routing>`
is configured, the corresponding cluster definition should use
:ref:`ring hash load balancing <envoy_v3_api_field_config.cluster.v3.Cluster.lb_policy>`.

If :ref:`active health checking <arch_overview_health_checking>` is desired, the
cluster should be configured with a :ref:`custom health check
<envoy_v3_api_field_config.core.v3.HealthCheck.custom_health_check>` which configured as a
:ref:`Redis health checker <config_health_checkers_redis>`.

If passive healthchecking is desired, also configure
:ref:`outlier detection <arch_overview_outlier_detection>`.

For the purposes of passive healthchecking, connect timeouts, command timeouts, and connection
close map to 5xx. All other responses from Redis are counted as a success.

.. _arch_overview_redis_static_shard_routing:

Fixed shard routing
-------------------

Independent Redis instances that are already sharded by the application can use
:ref:`static_shard_routing <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.static_shard_routing>`
instead of a ring-hash load balancer. In this mode Envoy routes each key to
``XXH32(hashtag(key)) % shard_count`` using seed 0 over the exact key or hash-tag bytes,
and uses endpoint metadata to bind every upstream to an explicit, stable shard ordinal.

This mode is intended for migrations where existing clients already use the same ``XXH32``
placement. Envoy never derives an ordinal from discovery order and never compacts the shard map
when an endpoint becomes unavailable. A missing, unhealthy, degraded, or excluded endpoint makes
only that ordinal unavailable, rather than moving its keys to another Redis instance.

Each endpoint in the single supported priority must have a unique
``filter_metadata.envoy.filters.network.redis_proxy.shard_index`` value in
``[0, shard_count)``. Endpoints may be distributed across localities. For example, the Redis
cluster load assignment can bind two endpoints to ordinals 0 and 1:

.. code-block:: yaml

  load_assignment:
    cluster_name: redis
    endpoints:
    - lb_endpoints:
      - metadata:
          filter_metadata:
            envoy.filters.network.redis_proxy:
              shard_index: 0
        endpoint:
          address:
            socket_address:
              address: redis-0
              port_value: 6379
      - metadata:
          filter_metadata:
            envoy.filters.network.redis_proxy:
              shard_index: 1
        endpoint:
          address:
            socket_address:
              address: redis-1
              port_value: 6379

The corresponding Redis proxy connection-pool settings enable hash-tag extraction and select the
same two-shard map:

.. code-block:: yaml

  settings:
    enable_hashtagging: true
    static_shard_routing:
      shard_count: 2

All Envoy instances behind the same load-balanced address must use the same ``shard_count``,
``enable_hashtagging`` value, prefix-route behavior, and endpoint-to-``shard_index`` mapping.
Rolling between incompatible configurations is unsafe because two Envoys could route the same key
to different Redis instances. Divergent health views are safe for placement: an Envoy with an
unhealthy ordinal fails that request instead of rerouting it.
Changing ``shard_count`` or reassigning a ``shard_index`` is a data-placement migration,
not a safe rolling configuration change.

Fixed shard routing cannot be combined with a Redis Cluster upstream or with
:ref:`enable_redirection <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.enable_redirection>`.
Following a ``MOVED`` or ``ASK`` response to an arbitrary host would escape the fixed ordinal map,
so an invalid configuration fails closed rather than falling back to normal load-balancer routing.

Cluster-scope commands such as ``SCRIPT LOAD`` enumerate these fixed ordinals. An unavailable
ordinal fails the command instead of silently skipping a Redis instance.

.. _arch_overview_redis_cluster_support:

Redis Cluster Support
---------------------

Envoy offers support for `Redis Cluster <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>`_.

When using Envoy as a sidecar proxy for a Redis Cluster, the service can use a non-cluster Redis client
implemented in any language to connect to the proxy as if it's a single node Redis instance.
The Envoy proxy will keep track of the cluster topology and send commands to the correct Redis node in the
cluster according to the `spec <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>`_. Advance features such as reading
from replicas can also be added to the Envoy proxy instead of updating redis clients in each language.

Envoy proxy tracks the topology of the cluster by sending periodic
`cluster slots <https://redis.io/commands/cluster-slots>`_ commands to a random node in the cluster, and maintains the
following information:

* List of known nodes.
* The primaries for each shard.
* Nodes entering or leaving the cluster.

Envoy proxy supports identification of the nodes via both IP address and hostnames in the ``cluster slots`` command response. In case of failure to resolve a primary hostname, Envoy will retry resolution of all nodes periodically until success. Failure to resolve a replica simply skips that replica. On the other hand, if the :ref:`enable_redirection <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.enable_redirection>` option is set and a MOVED or ASK response containing a hostname is received Envoy will not automatically do a DNS lookup and instead bubble the error to the client verbatim. To have Envoy do the DNS lookup and follow the redirection, you need to configure the DNS cache option :ref:`dns_cache_config <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.dns_cache_config>` under the connection pool settings. For a configuration example on how to enable DNS lookups for redirections, see the filter :ref:`configuration reference <config_network_filters_redis_proxy>`.

For topology configuration details, see the Redis Cluster
:ref:`v3 API reference <envoy_v3_api_msg_extensions.clusters.redis.v3.RedisClusterConfig>`.

Every Redis cluster has its own extra statistics tree rooted at *cluster.<name>.redis_cluster.* with the following statistics:

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  max_upstream_unknown_connections_reached, Counter, Total number of times that an upstream connection to an unknown host is not created after redirection having reached the connection pool's max_upstream_unknown_connections limit
  upstream_cx_drained, Counter, Total number of upstream connections drained of active requests before being closed
  exclusive_cx_active, Gauge, Current number of active exclusive upstream connections used by blocking commands
  exclusive_cx_idle, Gauge, Current number of idle reusable exclusive upstream connections
  exclusive_cx_created, Counter, Total number of exclusive upstream connections created
  exclusive_cx_reused, Counter, Total number of idle exclusive upstream connections reused
  exclusive_cx_retired, Counter, Total number of exclusive upstream connections retired after close or an unsafe state
  exclusive_cx_drained, Counter, Total number of exclusive upstream connections drained after host churn
  exclusive_cx_limit_reached, Counter, Total number of blocking requests rejected because the per-host exclusive connection cap was reached
  upstream_commands.upstream_rq_time, Histogram, Histogram of upstream request times for all types of requests

.. _arch_overview_redis_cluster_command_stats:

Per-cluster command statistics can be enabled via the setting :ref:`enable_command_stats <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.enable_command_stats>`.:

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  upstream_commands.[command].success, Counter, Total number of successful requests for a specific Redis command
  upstream_commands.[command].failure, Counter, Total number of failed or cancelled requests for a specific Redis command
  upstream_commands.[command].total, Counter, Total number of requests for a specific Redis command (sum of success and failure)
  upstream_commands.[command].latency, Histogram, Latency of requests for a specific Redis command

Transactions
------------

Transactions (MULTI) are supported. Their use is no different from regular Redis: you start a transaction with MULTI,
and you execute it with EXEC. Within the transaction, from the list of commands supported by Envoy (see below), only single-key
commands (e.g. GET, SET), multi-key commands (e.g. DEL, MSET) and transaction commands (e.g. WATCH, UNWATCH, DISCARD, EXEC) are supported.


When working in Redis Cluster mode, Envoy will relay all the commands in the transaction to the node handling the first
key-based command in the transaction. If this command is multi-key, it will send it to the server corresponding to the first key
in the command. It is the user's responsibility to ensure that all keys in the transaction are mapped to the same hashslot, as
commands will not be redirected.

Supported commands
------------------

At the protocol level, pipelines are supported.
Use pipelining wherever possible for the best performance.

At the command level, Envoy only supports commands that can be reliably hashed to a server. AUTH, PING, ECHO and INFO
are the only exceptions. AUTH is processed locally by Envoy if a downstream password has been configured,
and no other commands will be processed until authentication is successful when a password has been
configured. If an external authentication provider is set, Envoy will instead send the authentication arguments
to an external service and act according to the authentication response. If a downstream password is set together
with external authentication, the validation will be done still externally and the downstream password used for
upstream authentication. Envoy will transparently issue AUTH commands upon connecting to upstream servers,
if upstream authentication passwords are configured for the cluster. Envoy responds to PING immediately with PONG.
Arguments to PING are not allowed. Envoy responds to ECHO immediately with the command argument.
All other supported commands must contain a key. Supported commands are functionally identical to the
original Redis command except possibly in failure scenarios.

RESP Protocol
^^^^^^^^^^^^^
Envoy redis proxy supports only RESP2 protocol for now. Clients should connect to Envoy using RESP2 protocol.
hello command with only hello 2 argument is supported, hello 3 will result in error response from Envoy.

INFO command
^^^^^^^^^^^^
INFO command is handled by envoy differently it aggregates metrics across all shards and returns consolidated cluster-wide statistics.
An optional section parameter can be provided to filter the output (e.g., INFO memory).
INFO.SHARD is an Envoy-specific command introduced for debugging purposes that queries a specific shard by index
and returns that shard's complete INFO response (e.g., INFO.SHARD 0 memory).
Shard numbering starts from 0. With Redis Cluster, shards are ordered from lowest to highest slot
assignment. With fixed static shard routing, the number is the configured endpoint
``shard_index``.
when using INFO.SHARD command, if the provided shard index is invalid, Envoy will return an error.
when using INFO.SHARD command, via redis-cli, make sure to use --raw flag to get the proper output format.

Shard-scoped scripts
^^^^^^^^^^^^^^^^^^^^
EVAL.SHARD and EVALSHA.SHARD are Envoy-specific commands that execute a script on one explicit
shard ordinal:

.. code-block:: none

  EVAL.SHARD <shard_id> <script> <numkeys> [key ...] [arg ...]
  EVALSHA.SHARD <shard_id> <sha1> <numkeys> [key ...] [arg ...]

Envoy strips ``<shard_id>`` and forwards the corresponding Redis ``EVAL`` or ``EVALSHA`` command
to that shard. The first declared key is used for prefix-route selection; a keyless script needs a
catch-all route. The caller is responsible for ensuring that every key in the script belongs to
the selected shard.

Blocking list pops
^^^^^^^^^^^^^^^^^^
Envoy supports one-key ``BLPOP`` and ``BRPOP`` requests. A blocking pop owns an exclusive upstream
connection until Redis responds, so later requests received on the same downstream connection are
not dispatched until that response has been returned. This preserves Redis response ordering
without allowing an unrelated pipelined request to overtake a blocked pop. The blocking pop also
waits for earlier requests on that downstream connection to complete before it uses its separate
upstream connection. Envoy applies downstream read backpressure while this dispatch barrier is
active.

The Redis timeout argument is preserved. A timeout of ``0`` waits indefinitely; a finite timeout
uses Redis's timeout plus a small proxy grace period for Envoy's operation timer. Multiple keys
are rejected because independent static shards can place them on different Redis instances, and
blocking pops are not supported inside ``MULTI``/``EXEC`` transactions.

If Redis returns a ``MOVED`` or ``ASK`` response for a blocking pop, Envoy passes that response
downstream instead of retrying it through the shared pipelined pool, which would violate the
exclusive-connection guarantee.

Exclusive connections are separate from Envoy's shared pipelined Redis clients. Configure
:ref:`blocking_command_settings <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.blocking_command_settings>`
to cap active and idle exclusive connections per Redis host and per Envoy worker thread. The
effective process-wide cap scales with the number of worker threads, and a VIP fronting multiple
Envoy instances scales the aggregate cap by the number of instances.

For details on each command's usage see the official
`Redis command reference <https://redis.io/commands>`_.

.. csv-table::
  :header: Command, Group
  :widths: 1, 1

  AUTH, Authentication
  ECHO, Connection
  PING, Connection
  QUIT, Connection
  DEL, Generic
  DISCARD, Transaction
  DUMP, Generic
  EXEC, Transaction
  EXISTS, Generic
  EXPIRE, Generic
  EXPIREAT, Generic
  KEYS, String
  PERSIST, Generic
  PEXPIRE, Generic
  PEXPIREAT, Generic
  PTTL, Generic
  RESTORE, Generic
  SELECT, Generic
  TOUCH, Generic
  TTL, Generic
  TYPE, Generic
  UNLINK, Generic
  COPY, Generic
  RENAME, Generic
  RENAMENX, Generic
  SORT, Generic
  SORT_RO, Generic
  SCRIPT, Generic
  FLUSHALL, Generic
  FLUSHDB, Generic
  SLOWLOG, Generic
  CONFIG, Generic
  CLUSTER INFO, Generic
  CLUSTER SLOTS, Generic
  CLUSTER KEYSLOT, Generic
  CLUSTER NODES, Generic
  RANDOMKEY, Generic
  OBJECT, Generic
  GEOADD, Geo
  GEODIST, Geo
  GEOHASH, Geo
  GEOPOS, Geo
  GEORADIUS_RO, Geo
  GEORADIUSBYMEMBER_RO, Geo
  GEOSEARCH, Geo
  GEOSEARCHSTORE, Geospatial
  GEORADIUS, Geospatial
  GEORADIUSBYMEMBER, Geospatial
  HDEL, Hash
  HEXISTS, Hash
  HEXPIRE, Hash
  HEXPIREAT, Hash
  HEXPIRETIME, Hash
  HGET, Hash
  HGETALL, Hash
  HINCRBY, Hash
  HINCRBYFLOAT, Hash
  HKEYS, Hash
  HLEN, Hash
  HMGET, Hash
  HMSET, Hash
  HPERSIST, Hash
  HPEXPIRE, Hash
  HPEXPIREAT, Hash
  HPEXPIRETIME, Hash
  HPTTL, Hash
  HRANDFIELD, Hash
  HSCAN, Hash
  HSET, Hash
  HSETNX, Hash
  HSTRLEN, Hash
  HTTL, Hash
  HVALS, Hash
  PFADD, HyperLogLog
  PFCOUNT, HyperLogLog
  PFMERGE, HyperLogLog
  BLPOP, List
  BRPOP, List
  LINDEX, List
  LINSERT, List
  LLEN, List
  LPOP, List
  LPUSH, List
  LPUSHX, List
  LRANGE, List
  LREM, List
  LSET, List
  LTRIM, List
  LPOS, List
  RPOPLPUSH, List
  MULTI, Transaction
  RPOP, List
  RPUSH, List
  RPUSHX, List
  PUBLISH, Pubsub
  EVAL, Scripting
  EVAL.SHARD, Scripting
  EVALSHA, Scripting
  EVALSHA.SHARD, Scripting
  SADD, Set
  SCARD, Set
  SISMEMBER, Set
  SMEMBERS, Set
  SPOP, Set
  SRANDMEMBER, Set
  SREM, Set
  SCAN, Generic
  SSCAN, Set
  SDIFF, Set
  SDIFFSTORE, Set
  SINTER, Set
  SINTERSTORE, Set
  SMISMEMBER, Set
  SMOVE, Set
  SUNION, Set
  SUNIONSTORE, Set
  WATCH, String
  UNWATCH, String
  ZADD, Sorted Set
  ZCARD, Sorted Set
  ZCOUNT, Sorted Set
  ZINCRBY, Sorted Set
  ZLEXCOUNT, Sorted Set
  ZRANGE, Sorted Set
  ZRANGEBYLEX, Sorted Set
  ZRANGEBYSCORE, Sorted Set
  ZRANK, Sorted Set
  ZREM, Sorted Set
  ZREMRANGEBYLEX, Sorted Set
  ZREMRANGEBYRANK, Sorted Set
  ZREMRANGEBYSCORE, Sorted Set
  ZREVRANGE, Sorted Set
  ZREVRANGEBYLEX, Sorted Set
  ZREVRANGEBYSCORE, Sorted Set
  ZREVRANK, Sorted Set
  ZPOPMIN, Sorted Set
  ZPOPMAX, Sorted Set
  ZSCAN, Sorted Set
  ZSCORE, Sorted Set
  ZDIFF, Sorted Set
  ZDIFFSTORE, Sorted Set
  ZINTER, Sorted Set
  ZINTERSTORE, Sorted Set
  ZMSCORE, Sorted Set
  ZRANDMEMBER, Sorted Set
  ZRANGESTORE, Sorted Set
  ZUNION, Sorted Set
  ZUNIONSTORE, Sorted Set
  APPEND, String
  BITCOUNT, String
  BITFIELD, String
  BITFIELD_RO, String
  BITPOS, String
  DECR, String
  DECRBY, String
  GET, String
  GETBIT, String
  GETDEL, String
  GETEX, String
  GETRANGE, String
  GETSET, String
  INCR, String
  INCRBY, String
  INCRBYFLOAT, String
  INFO, Server
  INFO.SHARD, Server
  ROLE, Server
  MGET, String
  MSET, String
  PSETEX, String
  SET, String
  SETBIT, String
  SETEX, String
  SETNX, String
  SETRANGE, String
  STRLEN, String
  MSETNX, String
  SUBSTR, String
  XACK, Stream
  XADD, Stream
  XAUTOCLAIM, Stream
  XCLAIM, Stream
  XDEL, Stream
  XLEN, Stream
  XPENDING, Stream
  XRANGE, Stream
  XREVRANGE, Stream
  XTRIM, Stream
  BF.ADD, Bloom
  BF.CARD, Bloom
  BF.EXISTS, Bloom
  BF.INFO, Bloom
  BF.INSERT, Bloom
  BF.LOADCHUNK, Bloom
  BF.MADD, Bloom
  BF.MEXISTS, Bloom
  BF.RESERVE, Bloom
  BF.SCANDUMP, Bloom
  BITOP, Bitmap

Failure modes
-------------

If Redis throws an error, we pass that error along as the response to the command. Envoy treats a
response from Redis with the error datatype as a normal response and passes it through to the
caller.

Envoy can also generate its own errors in response to the client.

.. csv-table::
  :header: Error, Meaning
  :widths: 1, 1

  no upstream host, "The ring hash load balancer did not have a healthy host available at the
  ring position chosen for the key."
  upstream failure, "The backend did not respond within the timeout period or closed
  the connection."
  invalid request, "Command was rejected by the first stage of the command splitter due to
  datatype or length."
  ERR unknown command, "The command was not recognized by Envoy and therefore cannot be serviced
  because it cannot be hashed to a backend server."
  finished with n errors, "Fragmented commands which sum the response (e.g. DEL) will return the
  total number of errors received if any were received."
  upstream protocol error, "A fragmented command received an unexpected datatype or a backend
  responded with a response that not conform to the Redis protocol."
  wrong number of arguments for command, "Certain commands check in Envoy that the number of
  arguments is correct."
  "NOAUTH Authentication required.", "The command was rejected because a downstream authentication
  password or external authentication have been set and the client has not successfully authenticated."
  ERR invalid password, "The authentication command failed due to an invalid password."
  ERR <external-message>, "The authentication command failed on the external auth provider."
  "ERR Client sent AUTH, but no password is set", "An authentication command was received, but no
  downstream authentication password or external authentication provider have been configured."
  ERR invalid cursor, "The iteration command failed due to an invalid or unrecognized cursor."


In the case of MGET, each individual key that cannot be fetched will generate an error response.
For example, if we fetch five keys and two of the keys' backends time out, we would get an error
response for each in place of the value.

.. code-block:: none

  $ redis-cli MGET a b c d e
  1) "alpha"
  2) "bravo"
  3) (error) upstream failure
  4) (error) upstream failure
  5) "echo"

Protocol
--------

Although `RESP <https://redis.io/docs/latest/develop/reference/protocol-spec/>`_ is recommended for production use,
`inline commands <https://redis.io/docs/latest/develop/reference/protocol-spec/#inline-commands>`_ are also supported.

Added fixed shard routing, shard-scoped ``EVAL.SHARD`` and ``EVALSHA.SHARD`` commands, and
one-key ``BLPOP`` and ``BRPOP`` support to the
:ref:`redis_proxy <config_network_filters_redis_proxy>` network filter. Fixed shard routing
keeps application-managed Redis shard placement stable across multiple Envoy instances, while
blocking pops use bounded exclusive upstream connections.

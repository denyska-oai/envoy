#include "source/extensions/filters/network/redis_proxy/conn_pool_impl.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "envoy/config/core/v3/base.pb.h"
#include "envoy/config/core/v3/health_check.pb.h"
#include "envoy/config/endpoint/v3/endpoint_components.pb.h"
#include "envoy/extensions/common/aws/v3/credential_provider.pb.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.validate.h"

#include "source/common/common/assert.h"
#include "source/common/common/hash.h"
#include "source/common/common/logger.h"
#include "source/common/http/message_impl.h"
#include "source/common/http/utility.h"
#include "source/common/stats/utility.h"
#include "source/extensions/filters/network/common/redis/utility.h"
#include "source/extensions/filters/network/redis_proxy/config.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {
namespace ConnPool {
namespace {
// null_pool_callbacks is used for requests that must be filtered and not redirected such as
// "asking".
Common::Redis::Client::DoNothingPoolCallbacks null_client_callbacks;

const Common::Redis::RespValue& getRequest(const RespVariant& request) {
  if (request.index() == 0) {
    return absl::get<const Common::Redis::RespValue>(request);
  } else {
    return *(absl::get<Common::Redis::RespValueConstSharedPtr>(request));
  }
}

static uint16_t default_port = 6379;
constexpr char kStaticShardMetadataNamespace[] = "envoy.filters.network.redis_proxy";
constexpr char kStaticShardIndexMetadataKey[] = "shard_index";

std::chrono::milliseconds blockingOpTimeout(std::chrono::milliseconds blocking_timeout,
                                            std::chrono::milliseconds grace) {
  if (blocking_timeout.count() >
      std::numeric_limits<std::chrono::milliseconds::rep>::max() - grace.count()) {
    return std::chrono::milliseconds::max();
  }
  return blocking_timeout + grace;
}

} // namespace

InstanceImpl::InstanceImpl(
    const std::string& cluster_name, Upstream::ClusterManager& cm,
    Common::Redis::Client::ClientFactory& client_factory, ThreadLocal::SlotAllocator& tls,
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&
        config,
    Api::Api& api, Stats::ScopeSharedPtr&& stats_scope,
    const Common::Redis::RedisCommandStatsSharedPtr& redis_command_stats,
    Extensions::Common::Redis::ClusterRefreshManagerSharedPtr refresh_manager,
    const Extensions::Common::DynamicForwardProxy::DnsCacheSharedPtr& dns_cache,
    std::optional<envoy::extensions::filters::network::redis_proxy::v3::AwsIam> aws_iam_config,
    std::optional<Common::Redis::AwsIamAuthenticator::AwsIamAuthenticatorSharedPtr>
        aws_iam_authenticator,
    const std::string& local_zone)
    : cluster_name_(cluster_name), cm_(cm), client_factory_(client_factory),
      tls_(tls.allocateSlot()), config_(new Common::Redis::Client::ConfigImpl(config)),
      static_shard_count_(config.has_static_shard_routing()
                              ? std::optional<uint16_t>(static_cast<uint16_t>(
                                    config.static_shard_routing().shard_count()))
                              : std::nullopt),
      api_(api), stats_scope_(std::move(stats_scope)), redis_command_stats_(redis_command_stats),
      redis_cluster_stats_{
          REDIS_CLUSTER_STATS(POOL_COUNTER(*stats_scope_), POOL_GAUGE(*stats_scope_))},
      refresh_manager_(std::move(refresh_manager)), dns_cache_(dns_cache),
      aws_iam_authenticator_(aws_iam_authenticator), aws_iam_config_(aws_iam_config),
      max_active_exclusive_client_leases_per_host_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(
          config.blocking_command_settings(), max_active_connections_per_host, 32)),
      max_idle_exclusive_client_leases_per_host_(PROTOBUF_GET_WRAPPED_OR_DEFAULT(
          config.blocking_command_settings(), max_idle_connections_per_host, 1)),
      local_zone_(local_zone) {}

void InstanceImpl::init() {
  // Note: `this` and `cluster_name` have a a lifetime of the filter.
  // That may be shorter than the tls callback if the listener is torn down shortly after it is
  // created. We use a weak pointer to make sure this object outlives the tls callbacks.
  std::weak_ptr<InstanceImpl> this_weak_ptr = this->shared_from_this();
  tls_->set(
      [this_weak_ptr](Event::Dispatcher& dispatcher) -> ThreadLocal::ThreadLocalObjectSharedPtr {
        if (auto this_shared_ptr = this_weak_ptr.lock()) {
          return std::make_shared<ThreadLocalPool>(
              this_shared_ptr, dispatcher, this_shared_ptr->cluster_name_, this_shared_ptr->api_,
              this_shared_ptr->dns_cache_, this_shared_ptr->aws_iam_config_,
              this_shared_ptr->aws_iam_authenticator_);
        }
        return nullptr;
      });
}

uint16_t InstanceImpl::shardSize() { return tls_->getTyped<ThreadLocalPool>().shardSize(); }

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequest(const std::string& key, RespVariant&& request, PoolCallbacks& callbacks,
                          Common::Redis::Client::Transaction& transaction) {
  return tls_->getTyped<ThreadLocalPool>().makeRequest(key, std::move(request), callbacks,
                                                       transaction);
}

Common::Redis::Client::PoolRequest*
InstanceImpl::makeBlockingRequest(const std::string& key, RespVariant&& request,
                                  PoolCallbacks& callbacks,
                                  std::chrono::milliseconds blocking_timeout) {
  return tls_->getTyped<ThreadLocalPool>().makeBlockingRequest(key, std::move(request), callbacks,
                                                               blocking_timeout);
}

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequestToHost(const std::string& host_address,
                                const Common::Redis::RespValue& request,
                                Common::Redis::Client::ClientCallbacks& callbacks) {
  return tls_->getTyped<ThreadLocalPool>().makeRequestToHost(host_address, request, callbacks);
}

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequestToShard(uint16_t shard_index, RespVariant&& request,
                                 PoolCallbacks& callbacks,
                                 Common::Redis::Client::Transaction& transaction) {
  return tls_->getTyped<ThreadLocalPool>().makeRequestToShard(shard_index, std::move(request),
                                                              callbacks, transaction);
}

InstanceImpl::ThreadLocalPool::ThreadLocalPool(
    std::shared_ptr<InstanceImpl> parent, Event::Dispatcher& dispatcher, std::string cluster_name,
    Api::Api& api, const Extensions::Common::DynamicForwardProxy::DnsCacheSharedPtr& dns_cache,
    std::optional<envoy::extensions::filters::network::redis_proxy::v3::AwsIam> aws_iam_config,
    std::optional<Common::Redis::AwsIamAuthenticator::AwsIamAuthenticatorSharedPtr>
        aws_iam_authenticator)
    : parent_(parent), dispatcher_(dispatcher), cluster_name_(std::move(cluster_name)), api_(api),
      dns_cache_(dns_cache),
      drain_timer_(dispatcher.createTimer([this]() -> void { drainClients(); })),
      static_shard_count_(parent->static_shard_count_), client_factory_(parent->client_factory_),
      config_(parent->config_), stats_scope_(parent->stats_scope_),
      redis_command_stats_(parent->redis_command_stats_),
      redis_cluster_stats_(parent->redis_cluster_stats_),
      refresh_manager_(parent->refresh_manager_), aws_iam_authenticator_(aws_iam_authenticator),
      aws_iam_config_(aws_iam_config), client_zone_(parent->localZone()),
      max_active_exclusive_client_leases_per_host_(
          parent->max_active_exclusive_client_leases_per_host_),
      max_idle_exclusive_client_leases_per_host_(
          parent->max_idle_exclusive_client_leases_per_host_) {

  cluster_update_handle_ = parent->cm_.addThreadLocalClusterUpdateCallbacks(*this);
  Upstream::ThreadLocalCluster* cluster = parent->cm_.getThreadLocalCluster(cluster_name_);
  if (cluster != nullptr) {
    Upstream::ThreadLocalClusterCommand command = [&cluster]() -> Upstream::ThreadLocalCluster& {
      return *cluster;
    };
    onClusterAddOrUpdateNonVirtual(cluster->info()->name(), command);
  }
}

InstanceImpl::ThreadLocalPool::~ThreadLocalPool() {
  while (!pending_requests_.empty()) {
    pending_requests_.pop_front();
  }
  closeAllIdleExclusiveClientLeases();
  while (!client_map_.empty()) {
    client_map_.begin()->second->redis_client_->close();
  }
  while (!clients_to_drain_.empty()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }
}

void InstanceImpl::ThreadLocalPool::onClusterAddOrUpdateNonVirtual(
    absl::string_view cluster_name, Upstream::ThreadLocalClusterCommand& get_cluster) {
  if (cluster_name != cluster_name_) {
    return;
  }
  // Ensure the filter is not deleted in the main thread during this method.
  auto shared_parent = parent_.lock();
  if (!shared_parent) {
    return;
  }

  if (cluster_ != nullptr) {
    // Treat an update as a removal followed by an add.
    ThreadLocalPool::onClusterRemoval(cluster_name_);
  }

  ASSERT(cluster_ == nullptr);
  auto& cluster = get_cluster();
  cluster_ = &cluster;
  // Update username and password when cluster updates. authPassword is ignored by the client when
  // AWS IAM Authentication is enabled.
  auth_username_ = ProtocolOptionsConfigImpl::authUsername(cluster_->info(), api_);
  auth_password_ = ProtocolOptionsConfigImpl::authPassword(cluster_->info(), api_);
  ASSERT(host_set_member_update_cb_handle_ == nullptr);
  host_set_member_update_cb_handle_ = cluster_->prioritySet().addMemberUpdateCb(
      [this](const std::vector<Upstream::HostSharedPtr>& hosts_added,
             const std::vector<Upstream::HostSharedPtr>& hosts_removed) {
        onHostsAdded(hosts_added);
        onHostsRemoved(hosts_removed);
        rebuildStaticShardHosts();
      });

  ASSERT(host_address_map_.empty());
  for (const auto& i : cluster_->prioritySet().hostSetsPerPriority()) {
    onHostsAdded(i->hosts());
  }

  // Figure out if the cluster associated with this ConnPool is a Redis cluster
  // with its own hash slot sharding scheme and ability to dynamically discover
  // its members. This is done once to minimize overhead in the data path, makeRequest() in
  // particular.
  Upstream::ClusterInfoConstSharedPtr info = cluster_->info();
  OptRef<const envoy::config::cluster::v3::Cluster::CustomClusterType> cluster_type =
      info->clusterType();
  is_redis_cluster_ = cluster_type.has_value() && cluster_type->name() == "envoy.clusters.redis";
  rebuildStaticShardHosts();
}

void InstanceImpl::ThreadLocalPool::onClusterRemoval(absl::string_view cluster_name) {
  if (cluster_name != cluster_name_) {
    return;
  }

  // Treat cluster removal as a removal of all hosts. Close all connections and fail all pending
  // requests.
  host_set_member_update_cb_handle_ = nullptr;
  while (!client_map_.empty()) {
    client_map_.begin()->second->redis_client_->close();
  }
  while (!clients_to_drain_.empty()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }
  drainAllExclusiveClientLeases();
  closeAllIdleExclusiveClientLeases();
  // Active leases retain their own draining_ bit and will retire when they finish. Do not retain
  // old Host shared pointers across cluster updates; a replacement cluster may reuse the same
  // endpoint objects and must be able to create fresh leases.
  draining_exclusive_hosts_.clear();

  cluster_ = nullptr;
  host_address_map_.clear();
  cx_rate_limiter_map_.clear();
  static_shard_hosts_.reset();
}

void InstanceImpl::ThreadLocalPool::onHostsAdded(
    const std::vector<Upstream::HostSharedPtr>& hosts_added) {
  for (const auto& host : hosts_added) {
    // A host object that becomes selectable again may receive new exclusive leases. Existing
    // draining leases retain their own mark and still retire when they finish.
    draining_exclusive_hosts_.erase(host);
    std::string host_address = host->address()->asString();
    // Insert new host into address map, possibly overwriting a previous host's entry.
    host_address_map_[host_address] = host;
    for (const auto& created_host : created_via_redirect_hosts_) {
      if (created_host->address()->asString() == host_address) {
        // Remove our "temporary" host created in makeRequestToHost().
        onHostsRemoved({created_host});
        created_via_redirect_hosts_.remove(created_host);
        break;
      }
    }
  }
}

void InstanceImpl::ThreadLocalPool::onHostsRemoved(
    const std::vector<Upstream::HostSharedPtr>& hosts_removed) {
  for (const auto& host : hosts_removed) {
    drainExclusiveClientLeases(host);
    auto token_bucket = cx_rate_limiter_map_.find(host);
    if (token_bucket != cx_rate_limiter_map_.end()) {
      cx_rate_limiter_map_.erase(token_bucket);
    }
    auto it = client_map_.find(host);
    if (it != client_map_.end()) {
      if (it->second->redis_client_->active()) {
        // Put the ThreadLocalActiveClient to the side to drain.
        clients_to_drain_.push_back(std::move(it->second));
        client_map_.erase(it);
        if (!drain_timer_->enabled()) {
          drain_timer_->enableTimer(std::chrono::seconds(1));
        }
      } else {
        // There are no pending requests so close the connection.
        it->second->redis_client_->close();
      }
    }
    // There is the possibility that multiple hosts with the same address
    // are registered in host_address_map_ given that hosts may be created
    // upon redirection or supplied as part of the cluster's definition.
    auto it2 = host_address_map_.find(host->address()->asString());
    if ((it2 != host_address_map_.end()) && (it2->second == host)) {
      host_address_map_.erase(it2);
    }
  }
}

void InstanceImpl::ThreadLocalPool::drainClients() {
  while (!clients_to_drain_.empty() && !(*clients_to_drain_.begin())->redis_client_->active()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }
  if (!clients_to_drain_.empty()) {
    drain_timer_->enableTimer(std::chrono::seconds(1));
  }
}

InstanceImpl::ThreadLocalActiveClientPtr&
InstanceImpl::ThreadLocalPool::threadLocalActiveClient(Upstream::HostConstSharedPtr host) {
  TokenBucketPtr& rate_limiter = cx_rate_limiter_map_[host];
  if (config_->connectionRateLimitEnabled() && !rate_limiter) {
    rate_limiter = std::make_unique<TokenBucketImpl>(config_->connectionRateLimitPerSec(),
                                                     dispatcher_.timeSource(),
                                                     config_->connectionRateLimitPerSec());
  }
  ThreadLocalActiveClientPtr& client = client_map_[host];
  if (!client) {
    if (config_->connectionRateLimitEnabled() && rate_limiter->consume(1, false) == 0) {
      redis_cluster_stats_.connection_rate_limited_.inc();
    } else {
      ASSERT(cluster_ != nullptr);
      const auto credentials =
          ProtocolOptionsConfigImpl::authCredentials(cluster_->info(), api_, host);
      client = std::make_unique<ThreadLocalActiveClient>(*this);
      client->host_ = host;
      client->redis_client_ = client_factory_.create(
          host, dispatcher_, config_, redis_command_stats_, *(stats_scope_), credentials.username,
          credentials.password, false, aws_iam_config_, aws_iam_authenticator_);

      client->redis_client_->addConnectionCallbacks(*client);
    }
  }
  return client;
}

InstanceImpl::ExclusiveClientLeasePtr
InstanceImpl::ThreadLocalPool::acquireExclusiveClientLease(Upstream::HostConstSharedPtr host) {
  if (draining_exclusive_hosts_.find(host) != draining_exclusive_hosts_.end()) {
    return nullptr;
  }
  auto active = active_exclusive_client_leases_.find(host);
  if (active != active_exclusive_client_leases_.end() &&
      active->second.size() >= max_active_exclusive_client_leases_per_host_) {
    redis_cluster_stats_.exclusive_cx_limit_reached_.inc();
    return nullptr;
  }

  auto idle = idle_exclusive_client_leases_.find(host);
  if (idle != idle_exclusive_client_leases_.end()) {
    while (!idle->second.empty()) {
      ExclusiveClientLeasePtr lease = std::move(idle->second.front());
      idle->second.pop_front();
      ASSERT(lease->idle_);
      lease->redis_client_->removeConnectionCallbacks(*lease);
      lease->idle_ = false;
      redis_cluster_stats_.exclusive_cx_idle_.dec();
      if (lease->redis_client_->isOpen() && !lease->redis_client_->active()) {
        if (idle->second.empty()) {
          idle_exclusive_client_leases_.erase(idle);
        }
        if (!activateExclusiveClientLease(*lease)) {
          retireExclusiveClientLease(std::move(lease));
          return nullptr;
        }
        redis_cluster_stats_.exclusive_cx_reused_.inc();
        return lease;
      }
      retireExclusiveClientLease(std::move(lease));
    }
    idle_exclusive_client_leases_.erase(idle);
  }

  TokenBucketPtr& rate_limiter = cx_rate_limiter_map_[host];
  if (config_->connectionRateLimitEnabled() && !rate_limiter) {
    rate_limiter = std::make_unique<TokenBucketImpl>(config_->connectionRateLimitPerSec(),
                                                     dispatcher_.timeSource(),
                                                     config_->connectionRateLimitPerSec());
  }
  if (config_->connectionRateLimitEnabled() && rate_limiter->consume(1, false) == 0) {
    redis_cluster_stats_.connection_rate_limited_.inc();
    return nullptr;
  }

  ASSERT(cluster_ != nullptr);
  const auto credentials = ProtocolOptionsConfigImpl::authCredentials(cluster_->info(), api_, host);
  auto lease = std::make_unique<ExclusiveClientLease>(*this);
  lease->host_ = host;
  lease->redis_client_ = client_factory_.create(
      host, dispatcher_, config_, redis_command_stats_, *(stats_scope_), credentials.username,
      credentials.password, false, aws_iam_config_, aws_iam_authenticator_);
  if (!lease->redis_client_) {
    return nullptr;
  }
  redis_cluster_stats_.exclusive_cx_created_.inc();
  if (!activateExclusiveClientLease(*lease)) {
    retireExclusiveClientLease(std::move(lease));
    return nullptr;
  }
  return lease;
}

bool InstanceImpl::ThreadLocalPool::activateExclusiveClientLease(ExclusiveClientLease& lease) {
  if (draining_exclusive_hosts_.find(lease.host_) != draining_exclusive_hosts_.end()) {
    return false;
  }
  auto& active = active_exclusive_client_leases_[lease.host_];
  if (active.size() >= max_active_exclusive_client_leases_per_host_) {
    redis_cluster_stats_.exclusive_cx_limit_reached_.inc();
    if (active.empty()) {
      active_exclusive_client_leases_.erase(lease.host_);
    }
    return false;
  }
  if (!active.insert(&lease).second) {
    return false;
  }
  redis_cluster_stats_.exclusive_cx_active_.inc();
  return true;
}

void InstanceImpl::ThreadLocalPool::deactivateExclusiveClientLease(ExclusiveClientLease& lease) {
  auto active = active_exclusive_client_leases_.find(lease.host_);
  if (active == active_exclusive_client_leases_.end()) {
    return;
  }
  if (active->second.erase(&lease) == 0) {
    return;
  }
  redis_cluster_stats_.exclusive_cx_active_.dec();
  if (active->second.empty()) {
    active_exclusive_client_leases_.erase(active);
    // A removed host only needs to stay in the rejection set while an old in-flight lease still
    // owns it. Erase it once the final lease retires so repeated EDS churn does not retain Host
    // shared pointers indefinitely.
    draining_exclusive_hosts_.erase(lease.host_);
  }
}

void InstanceImpl::ThreadLocalPool::releaseExclusiveClientLease(ExclusiveClientLeasePtr&& lease) {
  if (!lease) {
    return;
  }
  deactivateExclusiveClientLease(*lease);
  if (lease->draining_ ||
      draining_exclusive_hosts_.find(lease->host_) != draining_exclusive_hosts_.end() ||
      !lease->redis_client_->isOpen() || lease->redis_client_->active() ||
      max_idle_exclusive_client_leases_per_host_ == 0) {
    retireExclusiveClientLease(std::move(lease));
    return;
  }
  auto idle = idle_exclusive_client_leases_.find(lease->host_);
  if (idle != idle_exclusive_client_leases_.end() &&
      idle->second.size() >= max_idle_exclusive_client_leases_per_host_) {
    retireExclusiveClientLease(std::move(lease));
    return;
  }
  lease->idle_ = true;
  lease->redis_client_->addConnectionCallbacks(*lease);
  idle_exclusive_client_leases_[lease->host_].push_back(std::move(lease));
  redis_cluster_stats_.exclusive_cx_idle_.inc();
}

void InstanceImpl::ThreadLocalPool::retireExclusiveClientLease(ExclusiveClientLeasePtr&& lease) {
  if (!lease) {
    return;
  }
  if (lease->idle_) {
    lease->redis_client_->removeConnectionCallbacks(*lease);
    lease->idle_ = false;
    redis_cluster_stats_.exclusive_cx_idle_.dec();
  }
  deactivateExclusiveClientLease(*lease);
  redis_cluster_stats_.exclusive_cx_retired_.inc();
  if (lease->draining_) {
    redis_cluster_stats_.exclusive_cx_drained_.inc();
  }
  // Move the lease object itself before closing. Callers commonly pass a PendingRequest member,
  // and merely moving redis_client_ would leave a non-null shell lease behind.
  ExclusiveClientLeasePtr retired = std::move(lease);
  retired->redis_client_->close();
  dispatcher_.deferredDelete(std::move(retired->redis_client_));
}

void InstanceImpl::ThreadLocalPool::onIdleExclusiveClientLeaseClosed(ExclusiveClientLease& lease) {
  if (!lease.idle_) {
    return;
  }

  auto idle = idle_exclusive_client_leases_.find(lease.host_);
  if (idle == idle_exclusive_client_leases_.end()) {
    return;
  }
  auto lease_it = std::find_if(
      idle->second.begin(), idle->second.end(),
      [&lease](const ExclusiveClientLeasePtr& candidate) { return candidate.get() == &lease; });
  if (lease_it == idle->second.end()) {
    return;
  }

  ExclusiveClientLeasePtr closed_lease = std::move(*lease_it);
  idle->second.erase(lease_it);
  if (idle->second.empty()) {
    idle_exclusive_client_leases_.erase(idle);
  }
  closed_lease->idle_ = false;
  redis_cluster_stats_.exclusive_cx_idle_.dec();
  redis_cluster_stats_.exclusive_cx_retired_.inc();
  if (closed_lease->draining_) {
    redis_cluster_stats_.exclusive_cx_drained_.inc();
  }

  // ClientImpl has already observed the close event. Defer both objects so this callback stays
  // alive until the connection finishes iterating its callback list.
  dispatcher_.deferredDelete(std::move(closed_lease->redis_client_));
  dispatcher_.deferredDelete(std::move(closed_lease));
}

void InstanceImpl::ThreadLocalPool::drainExclusiveClientLeases(Upstream::HostConstSharedPtr host) {
  draining_exclusive_hosts_.insert(host);
  closeIdleExclusiveClientLeases(host);
  auto active = active_exclusive_client_leases_.find(host);
  if (active == active_exclusive_client_leases_.end()) {
    draining_exclusive_hosts_.erase(host);
    return;
  }
  for (ExclusiveClientLease* lease : active->second) {
    lease->draining_ = true;
  }
}

void InstanceImpl::ThreadLocalPool::drainAllExclusiveClientLeases() {
  for (const auto& idle : idle_exclusive_client_leases_) {
    draining_exclusive_hosts_.insert(idle.first);
  }
  for (const auto& active : active_exclusive_client_leases_) {
    draining_exclusive_hosts_.insert(active.first);
    for (ExclusiveClientLease* lease : active.second) {
      lease->draining_ = true;
    }
  }
}

void InstanceImpl::ThreadLocalPool::closeIdleExclusiveClientLeases(
    Upstream::HostConstSharedPtr host) {
  auto idle = idle_exclusive_client_leases_.find(host);
  if (idle == idle_exclusive_client_leases_.end()) {
    return;
  }
  while (!idle->second.empty()) {
    ExclusiveClientLeasePtr lease = std::move(idle->second.front());
    idle->second.pop_front();
    ASSERT(lease->idle_);
    lease->redis_client_->removeConnectionCallbacks(*lease);
    lease->idle_ = false;
    redis_cluster_stats_.exclusive_cx_idle_.dec();
    lease->draining_ = draining_exclusive_hosts_.find(host) != draining_exclusive_hosts_.end();
    retireExclusiveClientLease(std::move(lease));
  }
  idle_exclusive_client_leases_.erase(idle);
}

void InstanceImpl::ThreadLocalPool::closeAllIdleExclusiveClientLeases() {
  while (!idle_exclusive_client_leases_.empty()) {
    closeIdleExclusiveClientLeases(idle_exclusive_client_leases_.begin()->first);
  }
}

uint16_t InstanceImpl::ThreadLocalPool::shardSize() {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return 0;
  }

  if (staticShardRoutingEnabled()) {
    return static_shard_hosts_.has_value() ? static_cast<uint16_t>(static_shard_hosts_->size()) : 0;
  }

  Common::Redis::RespValue request;
  absl::flat_hash_set<Upstream::HostConstSharedPtr> unique_hosts;
  unique_hosts.reserve(Envoy::Extensions::Clusters::Redis::MaxSlot);
  for (uint16_t size = 0; size < Envoy::Extensions::Clusters::Redis::MaxSlot; size++) {
    Clusters::Redis::RedisSpecifyShardContextImpl lb_context(
        size, request, Common::Redis::Client::ReadPolicy::Primary, client_zone_);
    Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
        cluster_->loadBalancer().chooseHost(&lb_context));
    if (!host) {
      return size;
    }
    unique_hosts.insert(std::move(host));
  }
  return static_cast<uint16_t>(unique_hosts.size());
}

bool InstanceImpl::ThreadLocalPool::staticShardRoutingEnabled() const {
  return static_shard_count_.has_value() && *static_shard_count_ > 0;
}

std::optional<uint16_t>
InstanceImpl::ThreadLocalPool::staticShardIndex(const Upstream::Host& host) const {
  const auto metadata = host.metadata();
  if (!metadata) {
    ENVOY_LOG(warn, "static Redis shard host is missing endpoint metadata");
    return std::nullopt;
  }

  const auto namespace_it = metadata->filter_metadata().find(kStaticShardMetadataNamespace);
  if (namespace_it == metadata->filter_metadata().end()) {
    ENVOY_LOG(warn, "static Redis shard host is missing '{}' metadata",
              kStaticShardMetadataNamespace);
    return std::nullopt;
  }

  const auto index_it = namespace_it->second.fields().find(kStaticShardIndexMetadataKey);
  if (index_it == namespace_it->second.fields().end() ||
      index_it->second.kind_case() != Protobuf::Value::kNumberValue) {
    ENVOY_LOG(warn, "static Redis shard host is missing numeric '{}' metadata",
              kStaticShardIndexMetadataKey);
    return std::nullopt;
  }

  const double index = index_it->second.number_value();
  if (index < 0 || std::floor(index) != index || index >= *static_shard_count_) {
    ENVOY_LOG(warn, "static Redis shard host has out-of-range '{}' metadata: {}",
              kStaticShardIndexMetadataKey, index);
    return std::nullopt;
  }
  return static_cast<uint16_t>(index);
}

void InstanceImpl::ThreadLocalPool::rebuildStaticShardHosts() {
  static_shard_hosts_.reset();
  if (!staticShardRoutingEnabled() || cluster_ == nullptr) {
    return;
  }
  if (is_redis_cluster_) {
    ENVOY_LOG(warn, "static Redis shard routing cannot be combined with a Redis Cluster upstream");
    return;
  }

  const auto& host_sets = cluster_->prioritySet().hostSetsPerPriority();
  if (host_sets.size() != 1) {
    ENVOY_LOG(warn, "static Redis shard routing requires exactly one priority, found {} priorities",
              host_sets.size());
    return;
  }

  const auto& host_set = *host_sets.front();

  // The vector size is the routing contract. Never shrink or compact it when an endpoint is
  // unavailable: a missing slot must fail closed instead of moving traffic to another Redis.
  std::vector<Upstream::HostConstSharedPtr> hosts(*static_shard_count_);
  std::vector<bool> seen_indices(*static_shard_count_, false);
  absl::flat_hash_set<Upstream::HostConstSharedPtr> healthy_hosts(host_set.healthyHosts().begin(),
                                                                  host_set.healthyHosts().end());
  absl::flat_hash_set<Upstream::HostConstSharedPtr> degraded_hosts(host_set.degradedHosts().begin(),
                                                                   host_set.degradedHosts().end());
  absl::flat_hash_set<Upstream::HostConstSharedPtr> excluded_hosts(host_set.excludedHosts().begin(),
                                                                   host_set.excludedHosts().end());

  for (const auto& host : host_set.hosts()) {
    const auto shard_index = staticShardIndex(*host);
    if (!shard_index.has_value()) {
      // An unindexed endpoint makes the topology ambiguous. Fail all static routing rather than
      // silently ignoring a configured Redis instance.
      return;
    }

    if (seen_indices[*shard_index]) {
      ENVOY_LOG(warn, "multiple static Redis shard hosts claim index {}", *shard_index);
      return;
    }
    seen_indices[*shard_index] = true;

    if (healthy_hosts.contains(host) && !degraded_hosts.contains(host) &&
        !excluded_hosts.contains(host)) {
      hosts[*shard_index] = host;
    }
  }

  static_shard_hosts_ = std::move(hosts);
}

Upstream::HostConstSharedPtr
InstanceImpl::ThreadLocalPool::staticShardHost(uint16_t shard_index) const {
  if (!staticShardRoutingEnabled() || !static_shard_hosts_.has_value() ||
      shard_index >= static_shard_hosts_->size()) {
    return nullptr;
  }
  return (*static_shard_hosts_)[shard_index];
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequest(const std::string& key, RespVariant&& request,
                                           PoolCallbacks& callbacks,
                                           Common::Redis::Client::Transaction& transaction) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  if (staticShardRoutingEnabled()) {
    if (!static_shard_hosts_.has_value()) {
      return nullptr;
    }
    const absl::string_view hash_key =
        Clusters::Redis::RedisLoadBalancerContextImpl::hashtag(key, config_->enableHashtagging());
    const uint16_t shard_index =
        static_cast<uint16_t>(HashUtil::xxHash32(hash_key) % static_shard_hosts_->size());
    Upstream::HostConstSharedPtr host = staticShardHost(shard_index);
    if (!host) {
      ENVOY_LOG(debug, "static Redis shard host not found for key '{}' at index {}", key,
                shard_index);
      return nullptr;
    }
    return makeRequestToHost(host, std::move(request), callbacks, transaction);
  }

  Clusters::Redis::RedisLoadBalancerContextImpl lb_context(
      key, config_->enableHashtagging(), is_redis_cluster_, getRequest(request),
      transaction.active_ ? Common::Redis::Client::ReadPolicy::Primary : config_->readPolicy(),
      client_zone_);
  Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
      cluster_->loadBalancer().chooseHost(&lb_context));
  if (!host) {
    ENVOY_LOG(debug, "host not found: '{}'", key);
    return nullptr;
  }

  return makeRequestToHost(host, std::move(request), callbacks, transaction);
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeBlockingRequest(const std::string& key, RespVariant&& request,
                                                   PoolCallbacks& callbacks,
                                                   std::chrono::milliseconds blocking_timeout) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  Upstream::HostConstSharedPtr host;
  if (staticShardRoutingEnabled()) {
    if (!static_shard_hosts_.has_value()) {
      return nullptr;
    }
    const absl::string_view hash_key =
        Clusters::Redis::RedisLoadBalancerContextImpl::hashtag(key, config_->enableHashtagging());
    const uint16_t shard_index =
        static_cast<uint16_t>(HashUtil::xxHash32(hash_key) % static_shard_hosts_->size());
    host = staticShardHost(shard_index);
    if (!host) {
      ENVOY_LOG(debug, "static Redis shard host not found for exclusive key '{}' at index {}", key,
                shard_index);
      return nullptr;
    }
  } else {
    // Blocking list pops mutate Redis state, so always select a primary even when the normal
    // pool is configured with a replica read policy.
    Clusters::Redis::RedisLoadBalancerContextImpl lb_context(
        key, config_->enableHashtagging(), is_redis_cluster_, getRequest(request),
        Common::Redis::Client::ReadPolicy::Primary, client_zone_);
    host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
        cluster_->loadBalancer().chooseHost(&lb_context));
    if (!host) {
      ENVOY_LOG(debug, "host not found for exclusive request: '{}'", key);
      return nullptr;
    }
  }

  ExclusiveClientLeasePtr exclusive_lease = acquireExclusiveClientLease(host);
  if (!exclusive_lease) {
    return nullptr;
  }

  pending_requests_.emplace_back(*this, std::move(request), callbacks, host);
  PendingRequest& pending_request = pending_requests_.back();
  pending_request.exclusive_client_lease_ = std::move(exclusive_lease);
  const Common::Redis::Client::Client::RequestOptions request_options =
      blocking_timeout == std::chrono::milliseconds::zero()
          ? Common::Redis::Client::Client::RequestOptions::disableOpTimeout()
          : Common::Redis::Client::Client::RequestOptions::withOpTimeout(
                blockingOpTimeout(blocking_timeout, config_->opTimeout()));
  pending_request.request_handler_ =
      pending_request.exclusive_client_lease_->redis_client_->makeRequest(
          getRequest(pending_request.incoming_request_), pending_request, request_options);
  if (pending_request.request_handler_) {
    return &pending_request;
  }

  pending_request.releaseExclusiveClientLease(false);
  onRequestCompleted();
  return nullptr;
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequestToShard(uint16_t shard_index, RespVariant&& request,
                                                  PoolCallbacks& callbacks,
                                                  Common::Redis::Client::Transaction& transaction) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  if (staticShardRoutingEnabled()) {
    Upstream::HostConstSharedPtr host = staticShardHost(shard_index);
    if (!host) {
      ENVOY_LOG(debug, "static Redis shard host not found at index {}", shard_index);
      return nullptr;
    }
    return makeRequestToHost(host, std::move(request), callbacks, transaction);
  }

  Clusters::Redis::RedisSpecifyShardContextImpl lb_context(
      shard_index, getRequest(request),
      transaction.active_ ? Common::Redis::Client::ReadPolicy::Primary : config_->readPolicy(),
      client_zone_);

  Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
      cluster_->loadBalancer().chooseHost(&lb_context));
  if (!host) {
    ENVOY_LOG(debug, "host not found: '{}'", shard_index);
    return nullptr;
  }
  return makeRequestToHost(host, std::move(request), callbacks, transaction);
}

Common::Redis::Client::PoolRequest* InstanceImpl::ThreadLocalPool::makeRequestToHost(
    const std::string& host_address, const Common::Redis::RespValue& request,
    Common::Redis::Client::ClientCallbacks& callbacks) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  auto colon_pos = host_address.rfind(':');
  if ((colon_pos == std::string::npos) || (colon_pos == (host_address.size() - 1))) {
    return nullptr;
  }

  const std::string ip_address = host_address.substr(0, colon_pos);
  const bool ipv6 = (ip_address.find(':') != std::string::npos);
  std::string host_address_map_key;
  Network::Address::InstanceConstSharedPtr address_ptr;

  if (!ipv6) {
    host_address_map_key = host_address;
  } else {
    const auto ip_port = absl::string_view(host_address).substr(colon_pos + 1);
    uint32_t ip_port_number;
    if (!absl::SimpleAtoi(ip_port, &ip_port_number) || (ip_port_number > 65535)) {
      return nullptr;
    }
    TRY_NEEDS_AUDIT {
      address_ptr = std::make_shared<Network::Address::Ipv6Instance>(ip_address, ip_port_number);
    }
    END_TRY catch (const EnvoyException&) { return nullptr; }
    host_address_map_key = address_ptr->asString();
  }

  auto it = host_address_map_.find(host_address_map_key);
  if (it == host_address_map_.end()) {
    // This host is not known to the cluster manager. Create a new host and insert it into the map.
    if (created_via_redirect_hosts_.size() == config_->maxUpstreamUnknownConnections()) {
      // Too many upstream connections to unknown hosts have been created.
      redis_cluster_stats_.max_upstream_unknown_connections_reached_.inc();
      return nullptr;
    }
    if (!ipv6) {
      // Only create an IPv4 address instance if we need a new Upstream::HostImpl.
      const auto ip_port = absl::string_view(host_address).substr(colon_pos + 1);
      uint32_t ip_port_number;
      if (!absl::SimpleAtoi(ip_port, &ip_port_number) || (ip_port_number > 65535)) {
        return nullptr;
      }
      TRY_NEEDS_AUDIT {
        address_ptr = std::make_shared<Network::Address::Ipv4Instance>(ip_address, ip_port_number);
      }
      END_TRY catch (const EnvoyException&) { return nullptr; }
    }
    Upstream::HostSharedPtr new_host{THROW_OR_RETURN_VALUE(
        Upstream::HostImpl::create(
            cluster_->info(), "", address_ptr, nullptr, nullptr, 1,
            std::make_shared<const envoy::config::core::v3::Locality>(),
            envoy::config::endpoint::v3::Endpoint::HealthCheckConfig::default_instance(), 0,
            envoy::config::core::v3::UNKNOWN),
        std::unique_ptr<Upstream::HostImpl>)};
    host_address_map_[host_address_map_key] = new_host;
    created_via_redirect_hosts_.push_back(new_host);
    it = host_address_map_.find(host_address_map_key);
  }

  ThreadLocalActiveClientPtr& client = threadLocalActiveClient(it->second);
  if (!client) {
    ENVOY_LOG(debug, "redis connection is rate limited, erasing empty client");
    client_map_.erase(it->second);
    return nullptr;
  }

  return client->redis_client_->makeRequest(request, callbacks);
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequestToHost(Upstream::HostConstSharedPtr& host,
                                                 RespVariant&& request, PoolCallbacks& callbacks,
                                                 Common::Redis::Client::Transaction& transaction) {
  uint32_t client_idx = transaction.current_client_idx_;
  // If there is an active transaction, establish a new connection if necessary.
  if (transaction.active_ && !transaction.connection_established_) {
    ASSERT(cluster_ != nullptr);
    const auto auth_credentials =
        ProtocolOptionsConfigImpl::authCredentials(cluster_->info(), api_, host);

    transaction.clients_[client_idx] =
        client_factory_.create(host, dispatcher_, config_, redis_command_stats_, *(stats_scope_),
                               auth_credentials.username, auth_credentials.password, true,
                               aws_iam_config_, aws_iam_authenticator_);
    if (transaction.connection_cb_) {
      transaction.clients_[client_idx]->addConnectionCallbacks(*transaction.connection_cb_);
    }
  }

  pending_requests_.emplace_back(*this, std::move(request), callbacks, host);
  PendingRequest& pending_request = pending_requests_.back();

  if (!transaction.active_) {
    ThreadLocalActiveClientPtr& client = this->threadLocalActiveClient(host);
    if (!client) {
      ENVOY_LOG(debug, "redis connection is rate limited, erasing empty client");
      pending_request.request_handler_ = nullptr;
      onRequestCompleted();
      client_map_.erase(host);
      return nullptr;
    }
    pending_request.request_handler_ = client->redis_client_->makeRequest(
        getRequest(pending_request.incoming_request_), pending_request);
  } else {
    pending_request.request_handler_ = transaction.clients_[client_idx]->makeRequest(
        getRequest(pending_request.incoming_request_), pending_request);
  }

  if (pending_request.request_handler_) {
    return &pending_request;
  } else {
    onRequestCompleted();
    return nullptr;
  }
}

void InstanceImpl::ThreadLocalPool::onRequestCompleted() {
  ASSERT(!pending_requests_.empty());

  // The response we got might not be in order, so flush out what we can. (A new response may
  // unlock several out of order responses).
  while (!pending_requests_.empty() && !pending_requests_.front().request_handler_) {
    pending_requests_.pop_front();
  }
}

void InstanceImpl::ThreadLocalActiveClient::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    auto client_to_delete = parent_.client_map_.find(host_);
    if (client_to_delete != parent_.client_map_.end()) {
      parent_.dispatcher_.deferredDelete(std::move(redis_client_));
      parent_.client_map_.erase(client_to_delete);
    } else {
      for (auto it = parent_.clients_to_drain_.begin(); it != parent_.clients_to_drain_.end();
           it++) {
        if ((*it).get() == this) {
          if (!redis_client_->active()) {
            parent_.redis_cluster_stats_.upstream_cx_drained_.inc();
          }
          parent_.dispatcher_.deferredDelete(std::move(redis_client_));
          parent_.clients_to_drain_.erase(it);
          break;
        }
      }
    }
  }
}

void InstanceImpl::ExclusiveClientLease::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    parent_.onIdleExclusiveClientLeaseClosed(*this);
  }
}

InstanceImpl::PendingRequest::PendingRequest(InstanceImpl::ThreadLocalPool& parent,
                                             RespVariant&& incoming_request,
                                             PoolCallbacks& pool_callbacks,
                                             Upstream::HostConstSharedPtr& host)
    : parent_(parent), incoming_request_(std::move(incoming_request)),
      pool_callbacks_(pool_callbacks), host_(host) {}

InstanceImpl::PendingRequest::~PendingRequest() {
  cache_load_handle_.reset();

  if (request_handler_) {
    request_handler_->cancel();
    request_handler_ = nullptr;
    releaseExclusiveClientLease(false);
    // If we have to cancel the request on the client, then we'll treat this as failure for pool
    // callback
    pool_callbacks_.onFailure();
  } else {
    releaseExclusiveClientLease(false);
  }
}

void InstanceImpl::PendingRequest::onResponse(Common::Redis::RespValuePtr&& response) {
  request_handler_ = nullptr;
  releaseExclusiveClientLease(true);
  pool_callbacks_.onResponse(std::move(response));
  parent_.onRequestCompleted();
}

void InstanceImpl::PendingRequest::onFailure() {
  request_handler_ = nullptr;
  releaseExclusiveClientLease(false);
  pool_callbacks_.onFailure();
  parent_.refresh_manager_->onFailure(parent_.cluster_name_);
  parent_.onRequestCompleted();
}

void InstanceImpl::PendingRequest::onRedirection(Common::Redis::RespValuePtr&& value,
                                                 const std::string& host_address,
                                                 bool ask_redirection) {
  // Retrying an exclusive request through makeRequestToHost() would put it back onto the shared
  // pipelined client. Until redirections can preserve exclusive ownership, pass the redirect back
  // downstream instead of silently violating the isolation guarantee.
  if (hasExclusiveClientLease()) {
    request_handler_ = nullptr;
    releaseExclusiveClientLease(false);
    pool_callbacks_.onResponse(std::move(value));
    parent_.onRequestCompleted();
    return;
  }

  if (!parent_.dns_cache_) {
    doRedirection(std::move(value), host_address, ask_redirection);
    return;
  }

  resp_value_ = std::move(value);
  ask_redirection_ = ask_redirection;
  auto result = parent_.dns_cache_->loadDnsCacheEntry(host_address, default_port, false, *this);
  cache_load_handle_ = std::move(result.handle_);

  switch (result.status_) {
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::InCache: {
    ASSERT(cache_load_handle_ == nullptr);
    if (!result.host_info_.has_value() || !result.host_info_.value()->address()) {
      ENVOY_LOG(debug, "DNS entry for '{}' was in cache but did not contain an address",
                host_address);
      auto host = host_;
      onResponse(std::move(resp_value_));
      host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    } else {
      doRedirection(std::move(resp_value_),
                    formatAddress(*result.host_info_.value()->address()->ip()), ask_redirection_);
    }
    return;
  }
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::Loading:
    ASSERT(cache_load_handle_ != nullptr);
    return;
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::Overflow:
    ASSERT(cache_load_handle_ == nullptr);
    ENVOY_LOG(debug, "DNS lookup for '{}' was not performed due to an overflow in the cache",
              host_address);
    auto host = host_;
    onResponse(std::move(resp_value_));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    return;
  }
  PANIC_DUE_TO_CORRUPT_ENUM;
}

std::string InstanceImpl::PendingRequest::formatAddress(const Envoy::Network::Address::Ip& ip) {
  return fmt::format("{}:{}", ip.addressAsString(), ip.port());
}
void InstanceImpl::PendingRequest::onLoadDnsCacheComplete(
    const Extensions::Common::DynamicForwardProxy::DnsHostInfoSharedPtr& host_info) {
  cache_load_handle_.reset();

  if (!host_info || !host_info->address()) {
    ENVOY_LOG(debug, "DNS lookup failed");
    auto host = host_;
    onResponse(std::move(resp_value_));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
  } else {
    doRedirection(std::move(resp_value_), formatAddress(*host_info->address()->ip()),
                  ask_redirection_);
  }
}

void InstanceImpl::PendingRequest::doRedirection(Common::Redis::RespValuePtr&& value,
                                                 const std::string& host_address,
                                                 bool ask_redirection) {
  // This request might go away, so keep a copy of host.
  auto host = host_;

  // Prepend request with an asking command if redirected via an ASK error. The returned handle is
  // not important since there is no point in being able to cancel the request. The use of
  // null_pool_callbacks ensures the transparent filtering of the Redis server's response to the
  // "asking" command; this is fine since the server either responds with an OK or an error message
  // if cluster support is not enabled (in which case we should not get an ASK redirection error).
  if (ask_redirection &&
      !parent_.makeRequestToHost(host_address, Common::Redis::Utility::AskingRequest::instance(),
                                 null_client_callbacks)) {
    onResponse(std::move(value));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
  } else {
    request_handler_ =
        parent_.makeRequestToHost(host_address, getRequest(incoming_request_), *this);
    if (!request_handler_) {
      onResponse(std::move(value));
      host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    } else {
      parent_.refresh_manager_->onRedirection(parent_.cluster_name_);
      host->cluster().trafficStats()->upstream_internal_redirect_succeeded_total_.inc();
    }
  }
}

void InstanceImpl::PendingRequest::cancel() {
  request_handler_->cancel();
  request_handler_ = nullptr;
  releaseExclusiveClientLease(false);
  parent_.onRequestCompleted();
}

void InstanceImpl::PendingRequest::releaseExclusiveClientLease(bool reusable) {
  if (!exclusive_client_lease_) {
    return;
  }

  if (reusable) {
    parent_.releaseExclusiveClientLease(std::move(exclusive_client_lease_));
  } else {
    parent_.retireExclusiveClientLease(std::move(exclusive_client_lease_));
  }
}

} // namespace ConnPool
} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy

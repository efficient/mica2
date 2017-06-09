#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_CLIENT_H_
#define MICA_DATAGRAM_DATAGRAM_CLIENT_H_

#include "mica/common.h"
// #include <unordered_set>
#include <unistd.h>
#include "mica/util/config.h"
#include "mica/processor/request_accessor.h"
#include "mica/datagram/datagram_server.h"

// Configuration file entries for DatagramClient:
//
//  * directory_refresh_interval (integer): The time interval in seconds to
//    refresh the server information on the directory.  This should be smaller
//    than TTL used by DirectoryClient. Default = 1
//  * directory_refresh_lcore (integer): The ID of the lcore to use refreshing
//    the directory.  Default = (the last lcore ID in the system)
//  * flush_status_report (bool): Flush the regular status output.  Default =
//    true

namespace mica {
namespace datagram {
struct BasicDatagramClientConfig {
  // The network type.
  typedef ::mica::network::DPDK<> Network;

  // User argument type for requsts.
  typedef struct {
  } Argument;

  // The maximum number of LCore to support.
  static constexpr size_t kMaxLCoreCount = 64;

  // The maximum size of a request batch.
  static constexpr uint16_t kMaxRequestBatchSize = 1;

  // The request batching timeout (accumulation time) in microseconds.
  // static constexpr uint16_t kRequestBatchTimeout = 1;

  // The maximum number of outstanding requests.
  static constexpr uint32_t kMaxOutstandingRequestCount = 1024;

  // The request timeout in microseconds.
  static constexpr uint16_t kRequestTimeout = 1000;

  // The maximum number of outstanding requests per partition.
  // static constexpr uint32_t kMaxOutstandingRequestCountPerPartition = 64;

  // The RX burst size.
  static constexpr uint16_t kRXBurst = 32;

  // The TX burst size.
  static constexpr uint16_t kTXBurst = 32;

  // The TX burst size to flush.  Must be no larger than kTXBurst.
  static constexpr uint16_t kTXMinBurst = 32;

  // The TX burst timeout (accumulation time) in microseconds.
  static constexpr uint16_t kTXBurstTimeout = 10;

  // The probing timeout in microseconds.
  static constexpr uint32_t kProbeTimeout = 30 * 1000 * 1000;

  // The probing interval in microseconds.
  static constexpr uint32_t kProbeInterval = 1 * 1000 * 1000;

  // The maximum number of partitions per server.
  static constexpr uint16_t kMaxPartitionsPerServer = 256;

  // The maximum number of endpoints per server.
  static constexpr uint16_t kMaxEndpointsPerServer = 32;

  // Skip RX.  Use this for full-speed TX with (distorted) key distribution to
  // stress test the server.
  static constexpr bool kSkipRX = false;

  // Ignore partitions within each server to test the server's request
  // redirection capability.
  static constexpr bool kIgnoreServerPartition = false;

  // Be verbose.
  static constexpr bool kVerbose = false;
};

template <class Client>
class ResponseHandlerInterface {
 public:
  typedef ::mica::table::Result Result;
  typedef typename Client::Argument Argument;

  void handle(typename Client::RequestDescriptor rd, Result result,
              const char* value, size_t value_length, const Argument& arg) {
    (void)rd;
    (void)result;
    (void)value;
    (void)value_length;
    (void)arg;
  }
};

template <class StaticConfig = BasicDatagramClientConfig>
class DatagramClient {
 public:
  typedef typename StaticConfig::Network Network;
  typedef ::mica::directory::DirectoryClient DirectoryClient;

  typedef typename StaticConfig::Argument Argument;

  typedef ::mica::processor::Operation Operation;
  typedef ::mica::table::Result Result;

  typedef uint16_t RequestDescriptor;

  DatagramClient(const ::mica::util::Config& config, Network* network,
                 DirectoryClient* dir_client);
  ~DatagramClient();

  void discover_servers(size_t min_servers = 1);

  void probe_reachability();

  bool can_request(uint64_t key_hash) const;

  template <class ResponseHandler>
  void handle_response(ResponseHandler& rh);

  // TODO
  // void flush();

  RequestDescriptor noop_read(uint64_t key_hash, const char* key,
                              size_t key_length,
                              const Argument& arg = Argument());

  RequestDescriptor noop_write(uint64_t key_hash, const char* key,
                               size_t key_length, const char* value,
                               size_t value_length,
                               const Argument& arg = Argument());

  RequestDescriptor del(uint64_t key_hash, const char* key, size_t key_length,
                        const Argument& arg = Argument());

  RequestDescriptor get(uint64_t key_hash, const char* key, size_t key_length,
                        const Argument& arg = Argument());

  RequestDescriptor increment(uint64_t key_hash, const char* key,
                              size_t key_length, uint64_t increment,
                              const Argument& arg = Argument());

  RequestDescriptor set(uint64_t key_hash, const char* key, size_t key_length,
                        const char* value, size_t value_length, bool overwrite,
                        const Argument& arg = Argument());

  RequestDescriptor test(uint64_t key_hash, const char* key, size_t key_length,
                         const Argument& arg = Argument());

 private:
  ::mica::util::Config config_;
  Network* network_;
  DirectoryClient* dir_client_;

  typedef typename Network::EndpointId EndpointId;
  typedef typename Network::PacketBuffer PacketBuffer;

  static constexpr RequestDescriptor kNullRD =
      static_cast<RequestDescriptor>(-1);

  // Request descriptor item.
  struct RequestDescriptorItem {
    // The next and prev request descriptor.  prev is not used when the
    // descriptor is not in use (the unused list is singly linked).
    RequestDescriptor next;
    RequestDescriptor prev;

    // The epoch used for constructing an opaque number in the batch.
    // Used for filtering out false matches by duplicate/late responses,
    // canceled requests, etc.
    uint16_t epoch;

    // The time to trigger a timeout.
    uint64_t expire_time;

    // The user argument.
    Argument arg;
  };

  struct ServerPartitionInfo {
    volatile uint16_t owner_lcore_id;
  };

  // A server's endpoint information.
  // This differs from local EndpointInfo as it contains more information (e.g.,
  // reachability).
  struct ServerEndpointInfo {
    ether_addr mac_addr;
    uint32_t ipv4_addr;
    uint16_t udp_port;

    // The following is used for diagnosis only.
    EndpointId eid;
    uint16_t owner_lcore_id;

    // Local endpoint IDs that can talk to a remote endpoint.
    // For now, this remains static once probing is done.
    // std::unordered_set<EndpointId> valid_local_eids;
    // std::unordered_set<EndpointId> invalid_local_eids;
  };

  // A server information.
  struct Server {
    // Partitioning scheme.
    uint8_t concurrent_read;
    uint8_t concurrent_write;
    uint16_t partition_count;
    uint16_t endpoint_count;

    // Remote endpoint on the server.  Indexed by the endpoint index.
    std::array<ServerEndpointInfo, StaticConfig::kMaxEndpointsPerServer>
        endpoints;

    // Partitions on the server.  Indexed by the partitionId.
    std::array<ServerPartitionInfo, StaticConfig::kMaxPartitionsPerServer>
        partitions;

    // For diagnosis only.
    std::string server_name;
  };

  // A pending request batch.
  struct PendingRequestBatch {
    RequestBatchBuilder<PacketBuffer> b;

    // Remote endpoint index to use.
    uint16_t remote_eid_index;
    // Remote endpoint will be chosen round-robin for concurrent access.
    uint8_t round_robin;
  };

  // A request state for a server.
  struct RequestState {
    // Local endpoint index to use for a specific remote endpoint.  Indexed by
    // the remote endpoint index.
    std::array<uint16_t, StaticConfig::kMaxEndpointsPerServer> local_eid_index;

    // Pending request state for concurrent access.
    PendingRequestBatch c;

    // Pending request state for exclusive access. Indexed by the partition ID.
    std::array<PendingRequestBatch, StaticConfig::kMaxPartitionsPerServer> e;
  };

  // RX/TX state for an endpoint.
  struct RXTXState {
    EndpointId eid;

    // Pending TX request packets for a local endpoint.
    struct PendingTX {
      std::array<PacketBuffer*, StaticConfig::kTXBurst> bufs;
      uint16_t count;
      uint64_t oldest_time;
    } pending_tx;
  };

  // The root per-thread states.
  struct ThreadState {
    // The entire request descriptor information.
    std::array<RequestDescriptorItem, StaticConfig::kMaxOutstandingRequestCount>
        rd_items;

    // The first unused request descriptor.
    RequestDescriptor rd_unused_first;

    // Request descriptor list for active requests.  Used for invoking the
    // response handler and detecting timeouts.
    RequestDescriptor rd_active_first;
    RequestDescriptor rd_active_last;
    uint16_t outstanding_request_count;

    // Local endpoint states for this thread.
    std::array<RXTXState, Network::kMaxEndpointCount> rx_tx_states;
    uint16_t endpoint_count;

    // Indexed by the server index.  This is a vector because there may be many
    // servers in the cluster.
    std::vector<RequestState> request_states;

    uint64_t last_status_report;
    uint32_t report_status_check;
    uint32_t report_status_check_max;

    volatile bool need_to_update_remote_eid;
  } __attribute__((aligned(128)));

  struct WorkerStats {
    uint64_t alive;
    uint64_t num_operations_done;
    uint64_t num_operations_succeeded;
    uint64_t num_operations_rejected;
    uint64_t num_operations_timeout;

    uint64_t last_num_operations_done;
    uint64_t last_num_operations_succeeded;
    uint64_t last_num_operations_rejected;
    uint64_t last_num_operations_timeout;
  } __attribute__((aligned(128)));

  struct EndpointStats {
    uint64_t last_rx_bursts;
    uint64_t last_rx_packets;
    uint64_t last_tx_bursts;
    uint64_t last_tx_packets;
    uint64_t last_tx_dropped;
  };

  // Indexed by the server index.  This is a vector because there may be many
  // servers in the cluster.
  std::vector<Server> servers_;
  ::mica::util::Stopwatch stopwatch_;

  uint32_t directory_refresh_interval_;
  uint16_t directory_refresh_lcore_;

  bool flush_status_report_;

  std::thread directory_thread_;
  volatile bool stopping_;

  // Padding to separate static and dynamic fields.
  char padding0[128];

  std::array<ThreadState, StaticConfig::kMaxLCoreCount> thread_states_;

  // Padding to separate two dynamic fields.
  char padding1[128];

  std::vector<WorkerStats> worker_stats_;

  // Padding to separate two dynamic fields.
  char padding2[128];

  std::vector<EndpointStats> endpoint_stats_;

  // Discovery.
  void update_servers_config();
  static void directory_proc_wrapper(void* arg);
  void directory_proc();

  void probe_send(EndpointId eid, ServerEndpointInfo& sei);
  bool probe_receive(EndpointId eid, ServerEndpointInfo& sei);

  void update_remote_eid();

  // Partitioning.
  static uint32_t calc_server_index(uint64_t key_hash, size_t server_count);
  static uint16_t calc_partition_id(uint64_t key_hash, size_t partition_count);

  // Request descriptor management.
  void init_rds(ThreadState& state);
  RequestDescriptor allocate_rd(ThreadState& thread_state, uint64_t now,
                                const Argument& arg);
  void release_rd(ThreadState& state, RequestDescriptor rd);

  // Generic request batch construction.
  RequestDescriptor append_request(Operation operation, uint64_t key_hash,
                                   const char* key, size_t key_length,
                                   const char* value, size_t value_length,
                                   const Argument& arg);

  // Pending request batch management.
  void make_new_pending_request_batch(PendingRequestBatch& prb);
  void release_pending_request_batch(PendingRequestBatch& prb);
  void flush_pending_request_batch(ThreadState& thread_state,
                                   uint32_t server_index,
                                   PendingRequestBatch& prb, uint64_t now);

  // TX packet handling.
  void check_pending_tx_full(RXTXState& rx_tx_state);
  void check_pending_tx_min(RXTXState& tx_state);
  void check_pending_tx_timeout(RXTXState& rx_tx_state, uint64_t now);
  void release_pending_tx(RXTXState& rx_tx_state);

  // Diagnosis.
  void reset_status();
  void report_status(double time_diff);
} __attribute__((aligned(128)));
}
}

#include "mica/datagram/datagram_client_impl.h"

#endif

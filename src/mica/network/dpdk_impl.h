#pragma once
#ifndef MICA_NETWORK_DPDK_IMPL_H_
#define MICA_NETWORK_DPDK_IMPL_H_

namespace mica {
namespace network {
template <class StaticConfig>
DPDK<StaticConfig>::DPDK(const ::mica::util::Config& config)
    : config_(config), rte_argc_(0), endpoint_count_(0), started_(false) {
  for (uint16_t numa_id = 0; numa_id < StaticConfig::kMaxNUMACount; numa_id++)
    mempools_[numa_id] = nullptr;

  assert(::mica::util::lcore.numa_count() <= StaticConfig::kMaxNUMACount);

  uint64_t core_mask = 0;
  auto lcores_conf = config_.get("lcores");
  for (size_t i = 0; i < lcores_conf.size(); i++) {
    auto lcore_id = lcores_conf.get(i).get_uint64();
    core_mask |= uint64_t(1) << lcore_id;
  }
  init_eal(core_mask);

  {
    uint16_t num_ports = rte_eth_dev_count();
    if (StaticConfig::kVerbose)
      printf("total %" PRIu16 " ports available\n", num_ports);

    ports_.resize(num_ports);
    for (uint16_t port_id = 0; port_id < num_ports; port_id++)
      ports_[port_id].valid = false;
  }

  // Parse configurations.
  auto ports_conf = config_.get("ports");
  for (size_t i = 0; i < ports_conf.size(); i++) {
    auto port_conf = ports_conf.get(i);
    uint16_t port_id = ::mica::util::safe_cast<uint16_t>(
        port_conf.get("port_id").get_uint64());
    assert(port_id < ports_.size());

    uint32_t ipv4_addr = NetworkAddress::parse_ipv4_addr(
        port_conf.get("ipv4_addr").get_str().c_str());

    ether_addr mac_addr;
    if (port_conf.get("mac_addr").exists())
      mac_addr = NetworkAddress::parse_mac_addr(
          port_conf.get("mac_addr").get_str().c_str());
    else
      rte_eth_macaddr_get(static_cast<uint8_t>(port_id), &mac_addr);

    uint16_t numa_id = get_port_numa_id(port_id);
    assert(numa_id < StaticConfig::kMaxNUMACount);

    ports_[port_id].valid = true;
    ports_[port_id].mac_addr = mac_addr;
    ports_[port_id].ipv4_addr = ipv4_addr;
    ports_[port_id].numa_id = numa_id;
    ports_[port_id].next_available_queue_id = 0;
  }

  auto endpoints_conf = config_.get("endpoints");
  if (endpoints_conf.exists()) {
    assert(endpoints_conf.size() <= StaticConfig::kMaxEndpointCount);
    for (size_t i = 0; i < endpoints_conf.size(); i++) {
      uint16_t lcore_id = ::mica::util::safe_cast<uint16_t>(
          endpoints_conf.get(i).get(0).get_uint64());

      uint16_t port_id = ::mica::util::safe_cast<uint16_t>(
          endpoints_conf.get(i).get(1).get_uint64());
      assert(ports_[port_id].valid);

      add_endpoint(lcore_id, port_id);
    }
  } else {
    uint16_t next_lcore_id[StaticConfig::kMaxNUMACount] = {
        0,
    };
    for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
      if (!ports_[port_id].valid) continue;

      uint16_t endpoint_count = 0;

      struct rte_eth_link link;
      while (true) {
        if (StaticConfig::kVerbose)
          printf("querying port %" PRIu16 "...\n", port_id);

        rte_eth_link_get(static_cast<uint8_t>(port_id), &link);

        if (!link.link_status) {
          printf("warning: port %" PRIu16 ": link down; retrying...\n",
                 port_id);
          sleep(1);
          continue;
        }
        if (link.link_speed / 1000 < StaticConfig::kMinLinkSpeed) {
          printf("warning: port %" PRIu16 ": low speed (current: %" PRIu32
                 " Gbps, minimum: %" PRIu32 " Gbps); retrying...\n",
                 port_id, link.link_speed / 1000, StaticConfig::kMinLinkSpeed);
          sleep(1);
          continue;
        }
        break;
      }

      switch (link.link_speed) {
        case ETH_SPEED_NUM_10M:
        case ETH_SPEED_NUM_100M:
        case ETH_SPEED_NUM_1G:
        case ETH_SPEED_NUM_10G:
          endpoint_count = 2;
          break;
        case ETH_SPEED_NUM_20G:
          endpoint_count = 4;
          break;
        case ETH_SPEED_NUM_40G:
          endpoint_count = 8;
          break;
        default:
          if (StaticConfig::kVerbose)
            printf("unknown link speed for port %" PRIu16 ": %" PRIu32 "\n",
                   port_id, link.link_speed);
          endpoint_count = static_cast<uint16_t>(link.link_speed / 5000);
          break;
      }

      size_t numa_id = ports_[port_id].numa_id;

      for (uint16_t j = 0; j < endpoint_count; j++) {
        while (true) {
          next_lcore_id[numa_id] = static_cast<uint16_t>(
              next_lcore_id[numa_id] % ::mica::util::lcore.lcore_count());
          if (::mica::util::lcore.numa_id(next_lcore_id[numa_id]) != numa_id)
            next_lcore_id[numa_id]++;
          else
            break;
        }

        add_endpoint(next_lcore_id[numa_id]++, port_id);
      }
    }
  }

  init_mempool();
}

template <class StaticConfig>
DPDK<StaticConfig>::~DPDK() {
  if (started_) stop();

  for (int i = 0; i < rte_argc_; i++) free(rte_argv_[i]);
}

template <class StaticConfig>
uint16_t DPDK<StaticConfig>::get_port_numa_id(uint16_t port_id) {
  int ret = rte_eth_dev_socket_id(static_cast<uint8_t>(port_id));
  assert(ret >= 0);
  return static_cast<uint16_t>(ret);
}

template <class StaticConfig>
void DPDK<StaticConfig>::add_endpoint(uint16_t lcore_id, uint16_t port_id) {
  uint16_t eid = endpoint_count_++;
  assert(eid < StaticConfig::kMaxEndpointCount);

  if (StaticConfig::kVerbose)
    printf("creating an endpoint %" PRIu16 " for lcore %" PRIu16
           " and port %" PRIu16 "\n",
           eid, lcore_id, port_id);

  auto& ei = endpoint_info_[eid];
  ei.owner_lcore_id = lcore_id;
  ei.rx_bursts = 0;
  ei.rx_packets = 0;
  ei.tx_bursts = 0;
  ei.tx_packets = 0;
  ei.tx_dropped = 0;

  ei.mac_addr = ports_[port_id].mac_addr;
  ei.ipv4_addr = ports_[port_id].ipv4_addr;
  ei.numa_id = ports_[port_id].numa_id;

  ei.port_id = port_id;
  assert(ports_[port_id].next_available_queue_id != static_cast<uint16_t>(-1));
  ei.queue_id = ports_[port_id].next_available_queue_id++;
  // XXX: We cannot use udp_port == 0 because fdir will never find a match?
  // ei.udp_port = static_cast<uint16_t>(1 + ei.queue_id);
  ei.udp_port = ei.queue_id;
}

template <class StaticConfig>
void DPDK<StaticConfig>::init_eal(uint64_t core_mask) {
  rte_argc_ = 0;

  rte_argv_[rte_argc_++] = strdup("");

  char s_core_mask[1024];
  snprintf(s_core_mask, sizeof(core_mask), "%" PRIx64 "", core_mask);
  rte_argv_[rte_argc_++] = strdup("-c");
  rte_argv_[rte_argc_++] = strdup(s_core_mask);

  assert(static_cast<uint64_t>(rte_argc_) <=
         sizeof(rte_argv_) / sizeof(rte_argv_[0]));

  auto args = config_.get("dpdk_args");
  if (args.exists()) {
    assert(args.is_array());
    assert(static_cast<size_t>(rte_argc_) + args.size() <=
           sizeof(rte_argv_) / sizeof(rte_argv_[0]));
    for (size_t i = 0; i < args.size(); i++)
      rte_argv_[rte_argc_++] = strdup(args.get(i).get_str().c_str());
  }

  int ret = rte_eal_init(rte_argc_, rte_argv_);

  if (ret < 0) {
    fprintf(stderr, "error: failed to initialize DPDK EAL (err=%s)\n",
            rte_strerror(ret));
    assert(false);
    return;
  }
}

template <class StaticConfig>
void DPDK<StaticConfig>::init_mempool() {
  // Initialize mempool.  Note that mempool will not be freed because DPDK
  // does not support it.

  size_t mbuf_entry_size =
      2048 + sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM;
  size_t desc_per_queue = StaticConfig::kRXDescCount +
                          StaticConfig::kTXDescCount +
                          StaticConfig::kSpareMBufCount;

  for (uint16_t numa_id = 0; numa_id < StaticConfig::kMaxNUMACount; numa_id++) {
    std::vector<uint16_t> queues_per_lcore(::mica::util::lcore.lcore_count(),
                                           0);

    for (uint16_t eid = 0; eid < endpoint_count_; eid++) {
      if (endpoint_info_[eid].numa_id == numa_id)
        queues_per_lcore[endpoint_info_[eid].owner_lcore_id]++;
    }
    // Calculate the size of mempool for these queues.
    size_t queue_count = std::accumulate(queues_per_lcore.begin(),
                                         queues_per_lcore.end(), size_t(0));
    size_t mbuf_size = desc_per_queue * queue_count;

    // No queues on this NUMA node.
    if (mbuf_size == 0) continue;

    size_t total_size =
        mbuf_size * rte_mempool_calc_obj_size(
                        static_cast<uint32_t>(mbuf_entry_size), 0, nullptr);

    // Ensure the cache size is large enough.
    size_t cache_size = RTE_MEMPOOL_CACHE_MAX_SIZE;
    // desc_per_queue *
    // *std::max_element(queues_per_lcore.begin(), queues_per_lcore.end());

    // If this is not big enough, RX/TX performance may not be consistent,
    // e.g., between CREW and CRCW experiments the maximum cache size can be
    // adjusted in DPDK's .config file: CONFIG_RTE_MEMPOOL_CACHE_MAX_SIZE
    if (RTE_MEMPOOL_CACHE_MAX_SIZE < cache_size) {
      fprintf(stderr,
              "CONFIG_RTE_MEMPOOL_CACHE_MAX_SIZE is too small (%d; must be at "
              "least %zu)\n",
              RTE_MEMPOOL_CACHE_MAX_SIZE, cache_size);
      assert(false);
      return;
    }

    if (StaticConfig::kVerbose)
      printf("initializing mempool on node %" PRIu16
             " for %zu queues (size: %.2lf MiB, cache size: %zu)\n",
             numa_id, queue_count, static_cast<double>(total_size) / 1048576.,
             cache_size);

    // Create a new mempool.
    char pool_name[64];
    snprintf(pool_name, sizeof(pool_name), "pktmbuf_pool%" PRIu16 "", numa_id);

    rte_mempool* mempool = rte_mempool_create(
        pool_name, static_cast<unsigned int>(mbuf_size),
        static_cast<unsigned int>(mbuf_entry_size),
        static_cast<unsigned int>(cache_size), sizeof(rte_pktmbuf_pool_private),
        rte_pktmbuf_pool_init, nullptr, rte_pktmbuf_init, nullptr, numa_id, 0);

    if (mempool == nullptr) {
      fprintf(stderr, "error: failed to allocate mbuf for numa node %" PRIu16
                      " (error: %s)\n",
              numa_id, rte_strerror(rte_errno));
      assert(false);
      return;
    }

    mempools_[numa_id] = mempool;
  }
}

template <class StaticConfig>
std::vector<typename DPDK<StaticConfig>::EndpointId>
DPDK<StaticConfig>::get_endpoints() const {
  std::vector<EndpointId> eids;
  eids.reserve(endpoint_count_);
  for (uint16_t eid = 0; eid < endpoint_count_; eid++) eids.push_back(eid);
  return eids;
}

template <class StaticConfig>
const typename DPDK<StaticConfig>::EndpointInfo&
DPDK<StaticConfig>::get_endpoint_info(EndpointId eid) const {
  assert(eid < endpoint_count_);
  return endpoint_info_[eid];
}

template <class StaticConfig>
void DPDK<StaticConfig>::start() {
  assert(!started_);

  rte_eth_conf eth_conf;
  rte_eth_rxconf eth_rx_conf;
  rte_eth_txconf eth_tx_conf;

  ::mica::util::memset(&eth_conf, 0, sizeof(eth_conf));
  ::mica::util::memset(&eth_rx_conf, 0, sizeof(eth_rx_conf));
  ::mica::util::memset(&eth_tx_conf, 0, sizeof(eth_tx_conf));

  // Force 10 Gbps.
  // TODO: We may want to allow higher link speeds.
  //eth_conf.link_speeds = ETH_LINK_SPEED_10G;
  eth_conf.rxmode.mq_mode = ETH_MQ_RX_NONE;
  eth_conf.rxmode.max_rx_pkt_len = ETHER_MAX_LEN;
  eth_conf.rxmode.hw_vlan_filter = 1;
  eth_conf.rxmode.hw_vlan_strip = 1;
  eth_conf.txmode.mq_mode = ETH_MQ_TX_NONE;
  eth_conf.fdir_conf.mode = RTE_FDIR_MODE_PERFECT;
  eth_conf.fdir_conf.pballoc = RTE_FDIR_PBALLOC_64K;
  eth_conf.fdir_conf.status = RTE_FDIR_NO_REPORT_STATUS;
  eth_conf.fdir_conf.mask.dst_port_mask = 0xffff;
  eth_conf.fdir_conf.drop_queue = 0;

  eth_rx_conf.rx_thresh.pthresh = 8;
  eth_rx_conf.rx_thresh.hthresh = 0;
  eth_rx_conf.rx_thresh.wthresh = 0;
  eth_rx_conf.rx_free_thresh = 0;
  eth_rx_conf.rx_drop_en = 0;

  eth_tx_conf.tx_thresh.pthresh = 32;
  eth_tx_conf.tx_thresh.hthresh = 0;
  eth_tx_conf.tx_thresh.wthresh = 0;
  eth_tx_conf.tx_free_thresh = 0;
  eth_tx_conf.tx_rs_thresh = 0;
  eth_tx_conf.txq_flags = (ETH_TXQ_FLAGS_NOMULTSEGS | ETH_TXQ_FLAGS_NOREFCOUNT |
                           ETH_TXQ_FLAGS_NOMULTMEMP | ETH_TXQ_FLAGS_NOOFFLOADS);

  int ret;

  for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
    if (!ports_[port_id].valid) continue;
    if (ports_[port_id].next_available_queue_id == 0) continue;

    uint16_t queue_count = 0;
    for (uint16_t eid = 0; eid < endpoint_count_; eid++)
      if (endpoint_info_[eid].port_id == port_id) queue_count++;
    if (queue_count == 0) continue;

    if (StaticConfig::kVerbose)
      printf("configuring port %" PRIu16 "...\n", port_id);

    ret = rte_eth_dev_configure(static_cast<uint8_t>(port_id), queue_count,
                                queue_count, &eth_conf);
    if (ret < 0) {
      fprintf(stderr, "error: failed to configure port %hhu (err=%s)\n",
              port_id, rte_strerror(ret));
      assert(false);
      return;
    }

    for (uint16_t eid = 0; eid < endpoint_count_; eid++) {
      if (endpoint_info_[eid].port_id == port_id) {
        uint16_t queue_id = endpoint_info_[eid].queue_id;
        uint16_t numa_id = endpoint_info_[eid].numa_id;

        ret = rte_eth_rx_queue_setup(static_cast<uint8_t>(port_id), queue_id,
                                     StaticConfig::kRXDescCount, numa_id,
                                     &eth_rx_conf, mempools_[numa_id]);
        if (ret < 0) {
          fprintf(stderr, "error: failed to configure port %" PRIu16
                          " rx_queue %" PRIu16 " (err=%s)\n",
                  port_id, queue_id, rte_strerror(ret));
          assert(false);
          return;
        }

        ret = rte_eth_tx_queue_setup(static_cast<uint8_t>(port_id), queue_id,
                                     StaticConfig::kTXDescCount, numa_id,
                                     &eth_tx_conf);
        if (ret < 0) {
          fprintf(stderr, "error: failed to configure port %" PRIu16
                          " tx_queue %" PRIu16 " (err=%s)\n",
                  port_id, queue_id, rte_strerror(ret));
          assert(false);
          return;
        }
      }
    }

    ret = rte_eth_dev_mac_addr_add(static_cast<uint8_t>(port_id),
                                   &ports_[port_id].mac_addr, 0);
    if (ret < 0) {
      fprintf(stderr, "error: failed to register MAC address for port %" PRIu16
                      " (err=%s)\n",
              port_id, rte_strerror(ret));
      assert(false);
      return;
    }

    if (StaticConfig::kVerbose)
      printf("starting port %" PRIu16 "...\n", port_id);

    ret = rte_eth_dev_start(static_cast<uint8_t>(port_id));
    if (ret < 0) {
      fprintf(stderr, "error: failed to start port %" PRIu16 " (err=%s)\n",
              port_id, rte_strerror(ret));
      assert(false);
      return;
    }
  }

  for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
    if (!ports_[port_id].valid) continue;
    if (ports_[port_id].next_available_queue_id == 0) continue;

    // This takes some time, but it is useful to ensure the port is fully ready.
    // Otherwise, the following flow director setup may disappear.

    struct rte_eth_link link;
    while (true) {
      if (StaticConfig::kVerbose)
        printf("querying port %" PRIu16 "...\n", port_id);

      rte_eth_link_get(static_cast<uint8_t>(port_id), &link);

      if (!link.link_status) {
        printf("warning: port %" PRIu16 ": link down; retrying...\n", port_id);
        sleep(1);
        continue;
      }
      if (link.link_speed / 1000 < StaticConfig::kMinLinkSpeed) {
        printf("warning: port %" PRIu16 ": low speed (current: %" PRIu32
               " Gbps, minimum: %" PRIu32 " Gbps); retrying...\n",
               port_id, link.link_speed / 1000, StaticConfig::kMinLinkSpeed);
        sleep(1);
        continue;
      }
      break;
    }

    printf("port %" PRIu16 ": %" PRIu32 " Gbps (%s)\n", port_id,
           link.link_speed / 1000,
           (link.link_duplex == ETH_LINK_FULL_DUPLEX) ? ("full-duplex")
                                                      : ("half-duplex"));
  }

  for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
    if (!ports_[port_id].valid) continue;
    if (ports_[port_id].next_available_queue_id == 0) continue;

    for (uint16_t eid = 0; eid < endpoint_count_; eid++) {
      if (endpoint_info_[eid].port_id != port_id) continue;
      auto queue_id = endpoint_info_[eid].queue_id;
      auto udp_port = endpoint_info_[eid].udp_port;

      rte_eth_fdir_filter filter;
      memset(&filter, 0, sizeof(filter));
      filter.soft_id = eid;
      filter.input.flow_type = RTE_ETH_FLOW_NONFRAG_IPV4_UDP;
      filter.input.flow.udp4_flow.dst_port = rte_cpu_to_be_16(udp_port);
      filter.action.rx_queue = queue_id;
      filter.action.behavior = RTE_ETH_FDIR_ACCEPT;
      filter.action.report_status = RTE_ETH_FDIR_NO_REPORT_STATUS;

      ret = rte_eth_dev_filter_ctrl(static_cast<uint8_t>(port_id),
                                    RTE_ETH_FILTER_FDIR, RTE_ETH_FILTER_ADD,
                                    &filter);
      if (ret < 0) {
        fprintf(stderr,
                "error: failed to add perfect filter entry on port %" PRIu16
                " (err=%s)\n",
                port_id, rte_strerror(ret));
        assert(false);
        return;
      }
    }
  }

  started_ = true;
}

template <class StaticConfig>
void DPDK<StaticConfig>::stop() {
  assert(started_);

  for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
    if (!ports_[port_id].valid) continue;
    if (ports_[port_id].next_available_queue_id == 0) continue;

    if (StaticConfig::kVerbose)
      printf("stopping port %" PRIu16 "...\n", port_id);
    rte_eth_dev_stop(static_cast<uint8_t>(port_id));
  }

  for (uint16_t port_id = 0; port_id < ports_.size(); port_id++) {
    if (!ports_[port_id].valid) continue;

    if (StaticConfig::kVerbose)
      printf("closing port %" PRIu16 "...\n", port_id);
    rte_eth_dev_close(static_cast<uint8_t>(port_id));
  }

  started_ = false;
}

template <class StaticConfig>
typename DPDK<StaticConfig>::PacketBuffer* DPDK<StaticConfig>::allocate() {
  auto buf = reinterpret_cast<PacketBuffer*>(
      rte_pktmbuf_alloc(mempools_[ ::mica::util::lcore.numa_id()]));
  assert(buf);
  return buf;
}

template <class StaticConfig>
typename DPDK<StaticConfig>::PacketBuffer* DPDK<StaticConfig>::clone(
    PacketBuffer* buf) {
  assert(buf);
  return reinterpret_cast<PacketBuffer*>(
      rte_pktmbuf_clone(reinterpret_cast<rte_mbuf*>(buf),
                        mempools_[ ::mica::util::lcore.numa_id()]));
}

template <class StaticConfig>
void DPDK<StaticConfig>::release(PacketBuffer* buf) {
  rte_pktmbuf_free(buf);
}

template <class StaticConfig>
uint16_t DPDK<StaticConfig>::receive(EndpointId eid, PacketBuffer** bufs,
                                     uint16_t buf_count) {
  assert(eid < endpoint_count_);
  auto& ei = endpoint_info_[eid];

  assert(::mica::util::lcore.lcore_id() == ei.owner_lcore_id);

  auto port_id = ei.port_id;
  auto queue_id = ei.queue_id;

  uint16_t rx_packets =
      rte_eth_rx_burst(static_cast<uint8_t>(port_id), queue_id,
                       reinterpret_cast<rte_mbuf**>(bufs), buf_count);

  ei.rx_bursts++;
  ei.rx_packets += rx_packets;
  return rx_packets;
}

template <class StaticConfig>
uint16_t DPDK<StaticConfig>::send(EndpointId eid, PacketBuffer** bufs,
                                  uint16_t buf_count) {
  assert(eid < endpoint_count_);
  auto& ei = endpoint_info_[eid];

  assert(::mica::util::lcore.lcore_id() == ei.owner_lcore_id);

  auto port_id = ei.port_id;
  auto queue_id = ei.queue_id;

  uint16_t tx_packets =
      rte_eth_tx_burst(static_cast<uint8_t>(port_id), queue_id,
                       reinterpret_cast<rte_mbuf**>(bufs), buf_count);

  ei.tx_bursts++;
  ei.tx_dropped +=
      static_cast<uint64_t>(buf_count) - static_cast<uint64_t>(tx_packets);
  ei.tx_packets += tx_packets;

  // TODO: Allow the user to resend packets?
  for (uint16_t i = tx_packets; i < buf_count; i++) release(bufs[i]);

  return tx_packets;
}
}
}

#endif

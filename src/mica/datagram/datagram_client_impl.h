#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_CLIENT_IMPL_H_
#define MICA_DATAGRAM_DATAGRAM_CLIENT_IMPL_H_

namespace mica {
namespace datagram {
template <class StaticConfig>
DatagramClient<StaticConfig>::DatagramClient(const ::mica::util::Config& config,
                                             Network* network,
                                             DirectoryClient* dir_client)
    : config_(config), network_(network), dir_client_(dir_client) {
  assert(StaticConfig::kMaxLCoreCount >= ::mica::util::lcore.lcore_count());

  stopwatch_.init_start();

  directory_refresh_interval_ = ::mica::util::safe_cast<uint32_t>(
      config.get("directory_refresh_interval").get_uint64(1));
  directory_refresh_lcore_ = ::mica::util::safe_cast<uint16_t>(
      config.get("directory_refresh_lcore")
          .get_uint64(::mica::util::lcore.lcore_count() - 1));

  flush_status_report_ = config.get("flush_status_report").get_bool(true);

  stopwatch_.init_end();

  stopping_ = false;

  reset_status();

  for (size_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
       lcore_id++) {
    auto& thread_state = thread_states_[lcore_id];
    init_rds(thread_state);

    thread_state.last_status_report = stopwatch_.now();
    thread_state.report_status_check = 0;
    thread_state.report_status_check_max = 0xffff;

    thread_state.need_to_update_remote_eid = false;
  }
}

template <class StaticConfig>
DatagramClient<StaticConfig>::~DatagramClient() {
  if (stopping_ == false) {
    stopping_ = true;
    directory_thread_.join();
  }

  for (size_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
       lcore_id++) {
    auto& thread_state = thread_states_[lcore_id];

    for (size_t server_index = 0; server_index < servers_.size();
         server_index++) {
      auto& rs = thread_state.request_states[server_index];
      auto& s = servers_[server_index];

      release_pending_request_batch(rs.c);

      for (uint16_t partition_id = 0; partition_id < s.partition_count;
           partition_id++)
        release_pending_request_batch(rs.e[partition_id]);
    }

    for (uint16_t endpoint_index = 0;
         endpoint_index < thread_state.endpoint_count; endpoint_index++) {
      auto& rx_tx_state = thread_state.rx_tx_states[endpoint_index];
      release_pending_tx(rx_tx_state);
    }
  }
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::discover_servers(size_t min_servers) {
  std::vector<std::string> server_list;

  while (true) {
    server_list = dir_client_->get_server_list();
    if (server_list.size() >= min_servers) break;

    printf("warning: too few servers (%zu < %zu); retrying...\n",
           server_list.size(), min_servers);
    sleep(1);
  }

  for (size_t server_index = 0; server_index < server_list.size();
       server_index++) {
    auto& server_name = server_list[server_index];
    auto si = dir_client_->get_server_info(server_name);

    printf("parsing server information for %s: %s\n", server_name.c_str(),
           si.c_str());

    auto si_conf =
        ::mica::util::Config::load(si, std::string() + "(" + server_name + ")");

    servers_.push_back(Server());
    auto& s = servers_.back();

    s.server_name = server_name;

    s.concurrent_read = si_conf.get("concurrent_read").get_bool() ? 1 : 0;
    s.concurrent_write = si_conf.get("concurrent_write").get_bool() ? 1 : 0;

    s.partition_count = static_cast<uint16_t>(si_conf.get("partitions").size());

    auto partitions_conf = si_conf.get("partitions");
    for (size_t i = 0; i < partitions_conf.size(); i++) {
      auto& p = s.partitions[i];

      uint16_t partition_id = ::mica::util::safe_cast<uint16_t>(
          partitions_conf.get(i).get(0).get_uint64());
      // We only support a dense list of partition IDs now.
      assert(i == partition_id);
      (void)partition_id;

      p.owner_lcore_id = ::mica::util::safe_cast<uint16_t>(
          partitions_conf.get(i).get(1).get_uint64());
    }

    s.endpoint_count = static_cast<uint16_t>(si_conf.get("endpoints").size());

    auto endpoints_conf = si_conf.get("endpoints");
    for (size_t i = 0; i < endpoints_conf.size(); i++) {
      auto& e = s.endpoints[i];

      e.eid = ::mica::util::safe_cast<uint16_t>(
          endpoints_conf.get(i).get(0).get_uint64());
      e.owner_lcore_id = ::mica::util::safe_cast<uint16_t>(
          endpoints_conf.get(i).get(1).get_uint64());
      e.mac_addr = ::mica::network::NetworkAddress::parse_mac_addr(
          endpoints_conf.get(i).get(2).get_str().c_str());
      e.ipv4_addr = ::mica::network::NetworkAddress::parse_ipv4_addr(
          endpoints_conf.get(i).get(3).get_str().c_str());
      e.udp_port = ::mica::util::safe_cast<uint16_t>(
          endpoints_conf.get(i).get(4).get_uint64());
    }

    printf("%zu partitions and %zu endpoints on server %s\n",
           partitions_conf.size(), endpoints_conf.size(), server_name.c_str());
  }

  // if (StaticConfig::kVerbose)
  printf("%zu servers discovered\n", server_list.size());

  directory_thread_ = std::thread(directory_proc_wrapper, this);
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::update_servers_config() {
  auto server_list = dir_client_->get_server_list();

  // TODO: Support significant configuration changes.
  // TODO: Use watch instead of get to receive configuration changes.
  assert(server_list.size() == servers_.size());

  bool changed = false;

  for (size_t server_index = 0; server_index < server_list.size();
       server_index++) {
    auto& server_name = server_list[server_index];
    auto si = dir_client_->get_server_info(server_name);

    // printf("parsing server information for %s: %s\n", server_name.c_str(),
    //        si.c_str());

    auto si_conf =
        ::mica::util::Config::load(si, std::string() + "(" + server_name + ")");

    auto& s = servers_[server_index];
    assert(s.server_name == server_name);

    // TODO: Support concurrent access mode changes.
    // TODO: Support partition count changes.

    auto partitions_conf = si_conf.get("partitions");
    for (size_t i = 0; i < partitions_conf.size(); i++) {
      auto& p = s.partitions[i];

      uint16_t partition_id = ::mica::util::safe_cast<uint16_t>(
          partitions_conf.get(i).get(0).get_uint64());
      // We only support a dense list of partition IDs now.
      assert(i == partition_id);
      (void)partition_id;

      auto owner_lcore_id = ::mica::util::safe_cast<uint16_t>(
          partitions_conf.get(i).get(1).get_uint64());
      if (p.owner_lcore_id != owner_lcore_id) {
        p.owner_lcore_id = owner_lcore_id;
        changed = true;
      }
    }
  }

  if (!changed) return;

  printf("server configuration changes detected\n");

  for (size_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
       lcore_id++) {
    auto& thread_state = thread_states_[lcore_id];

    thread_state.need_to_update_remote_eid = true;
  }
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::directory_proc_wrapper(void* arg) {
  auto client = reinterpret_cast<DatagramClient<StaticConfig>*>(arg);
  client->directory_proc();
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::directory_proc() {
  ::mica::util::lcore.pin_thread(directory_refresh_lcore_);

  if (directory_refresh_interval_ == 0) return;

  while (!stopping_) {
    update_servers_config();

    // TODO: Use watch instead of get.
    for (uint32_t i = 0; i < directory_refresh_interval_ && !stopping_; i++)
      sleep(1);
  }
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::probe_reachability() {
  size_t lcore_id = ::mica::util::lcore.lcore_id();

  printf("probing reachability on lcore %zu...\n", lcore_id);

  // TODO: Implement on-demand probing.

  auto& thread_state = thread_states_[lcore_id];

  thread_state.endpoint_count = 0;
  for (auto eid : network_->get_endpoints()) {
    if (network_->get_endpoint_info(eid).owner_lcore_id != lcore_id) continue;
    thread_state.rx_tx_states[thread_state.endpoint_count].eid = eid;
    thread_state.rx_tx_states[thread_state.endpoint_count].pending_tx.count = 0;
    thread_state.endpoint_count++;
  }

  printf("%" PRIu16 " local endpoints for lcore %zu\n",
         thread_state.endpoint_count, lcore_id);

  for (size_t server_index = 0; server_index < servers_.size();
       server_index++) {
    auto& s = servers_[server_index];

    thread_state.request_states.push_back(RequestState());
    auto& rs = thread_state.request_states.back();

    for (uint16_t remote_eindex = 0; remote_eindex < s.endpoint_count;
         remote_eindex++) {
      auto& sei = s.endpoints[remote_eindex];

      std::vector<uint16_t> valid_local_eindices;

      uint64_t start = stopwatch_.now();
      uint64_t last_probe_sent =
          start - stopwatch_.c_1_usec() * StaticConfig::kProbeInterval;
      while (stopwatch_.diff_in_cycles(stopwatch_.now(), start) <
             stopwatch_.c_1_usec() * StaticConfig::kProbeTimeout) {
        // Send probes.
        if (stopwatch_.diff_in_cycles(stopwatch_.now(), last_probe_sent) >=
            stopwatch_.c_1_usec() * StaticConfig::kProbeInterval) {
          last_probe_sent = stopwatch_.now();
          printf(
              "probing local endpoints for lcore %zu to reach remote "
              "endpoint %" PRIu32 " on server %s\n",
              lcore_id, s.endpoints[remote_eindex].eid, s.server_name.c_str());

          for (uint16_t eindex = 0; eindex < thread_state.endpoint_count;
               eindex++) {
            // Skip if this local endpoint is found to be valid already.
            if (std::find(valid_local_eindices.begin(),
                          valid_local_eindices.end(),
                          eindex) != valid_local_eindices.end())
              continue;

            auto& rx_tx_state = thread_state.rx_tx_states[eindex];
            probe_send(rx_tx_state.eid, sei);
          }
        }

        // Receive probes.
        for (uint16_t eindex = 0; eindex < thread_state.endpoint_count;
             eindex++) {
          // Skip if this local endpoint is found to be valid already.
          if (std::find(valid_local_eindices.begin(),
                        valid_local_eindices.end(),
                        eindex) != valid_local_eindices.end())
            continue;

          auto& rx_tx_state = thread_state.rx_tx_states[eindex];
          if (probe_receive(rx_tx_state.eid, sei))
            valid_local_eindices.push_back(eindex);
        }

        // No more endpoints to probe.
        // if (valid_local_eindices.size() ==
        //     static_cast<size_t>(thread_state.endpoint_count))
        if (valid_local_eindices.size() > 0) break;
      }

      if (valid_local_eindices.size() == 0) {
        rs.local_eid_index[remote_eindex] =
            StaticConfig::kMaxEndpointsPerServer;

        printf(
            "no local endpoint for lcore %zu could reach remote endpoint "
            "%" PRIu32 " on server %s\n",
            lcore_id, s.endpoints[remote_eindex].eid, s.server_name.c_str());
      } else {
        // Take the first valid local endpoint.
        uint16_t eindex = valid_local_eindices[0];
        auto& rx_tx_state = thread_state.rx_tx_states[eindex];
        rs.local_eid_index[remote_eindex] = eindex;

        // if (StaticConfig::kVerbose)
        {
          printf("local endpoint %3" PRIu32 " <-> remote endpoint %3" PRIu32
                 " on server %s\n",
                 rx_tx_state.eid, sei.eid, s.server_name.c_str());
        }
      }
    }

    make_new_pending_request_batch(rs.c);
    rs.c.remote_eid_index = 0;
    rs.c.round_robin = 1;

    for (uint16_t partition_id = 0; partition_id < s.partition_count;
         partition_id++) {
      make_new_pending_request_batch(rs.e[partition_id]);
      rs.e[partition_id].remote_eid_index =
          StaticConfig::kMaxEndpointsPerServer;
      rs.e[partition_id].round_robin = 0;
    }
  }

  update_remote_eid();
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::update_remote_eid() {
  size_t lcore_id = ::mica::util::lcore.lcore_id();

  auto& thread_state = thread_states_[lcore_id];
  for (size_t server_index = 0; server_index < servers_.size();
       server_index++) {
    auto& s = servers_[server_index];

    auto& rs = thread_state.request_states[server_index];

    for (uint16_t partition_id = 0; partition_id < s.partition_count;
         partition_id++) {
      for (uint16_t remote_eindex = 0; remote_eindex < s.endpoint_count;
           remote_eindex++) {
        if (s.endpoints[remote_eindex].owner_lcore_id !=
            s.partitions[partition_id].owner_lcore_id)
          continue;
        if (rs.e[partition_id].remote_eid_index == remote_eindex) continue;

        rs.e[partition_id].remote_eid_index = remote_eindex;

        printf("remote endpoint %2" PRIu32 " <-> partition %" PRIu16
               " on server %s\n",
               s.endpoints[remote_eindex].eid, partition_id,
               s.server_name.c_str());
        break;
      }
      if (rs.e[partition_id].remote_eid_index ==
          StaticConfig::kMaxEndpointsPerServer) {
        printf("no remote endpoint can handle remote partition %" PRIu16
               " on server %s\n",
               partition_id, s.server_name.c_str());
      }
    }
  }
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::probe_send(EndpointId eid,
                                              ServerEndpointInfo& sei) {
  auto buf = network_->allocate();
  assert(buf != nullptr);

  RequestBatchBuilder<PacketBuffer> b(buf);

  b.set_src_mac_addr(network_->get_endpoint_info(eid).mac_addr);
  b.set_dest_mac_addr(sei.mac_addr);

  b.set_src_ipv4_addr(
      rte_cpu_to_be_32(network_->get_endpoint_info(eid).ipv4_addr));
  b.set_dest_ipv4_addr(rte_cpu_to_be_32(sei.ipv4_addr));

  b.set_src_udp_port(
      rte_cpu_to_be_16(network_->get_endpoint_info(eid).udp_port));
  b.set_dest_udp_port(rte_cpu_to_be_16(sei.udp_port));

  b.set_request();

  // This opaque will not match against any upcoming requests as the event id
  // is too high.
  uint32_t opaque =
      (static_cast<uint32_t>(kNullRD) << 16) | static_cast<uint32_t>(sei.eid);

  b.append_request(Operation::kNoopRead, Result::kSuccess, opaque, 0, nullptr,
                   0, nullptr, 0);

  b.finalize();

  network_->send(eid, &buf, 1);
}

template <class StaticConfig>
bool DatagramClient<StaticConfig>::probe_receive(EndpointId eid,
                                                 ServerEndpointInfo& sei) {
  bool connected = false;

  // The same opaque from above.
  uint32_t opaque =
      (static_cast<uint32_t>(kNullRD) << 16) | static_cast<uint32_t>(sei.eid);

  while (true) {
    std::array<PacketBuffer*, StaticConfig::kRXBurst> bufs;
    uint16_t count =
        network_->receive(eid, bufs.data(), StaticConfig::kRXBurst);
    if (count == 0) break;

    for (uint16_t i = 0; i < count; i++) {
      RequestBatchReader<PacketBuffer> r(bufs[i]);

      if (r.is_valid() && r.is_response()) {
        if (r.find_next() && r.get_opaque() == opaque) connected = true;
      }

      network_->release(bufs[i]);
    }

    if (connected) break;
  }

  return connected;
}

template <class StaticConfig>
bool DatagramClient<StaticConfig>::can_request(uint64_t key_hash) const {
  // TODO: Use key_hash to check per-partition limit.
  (void)key_hash;
  size_t lcore_id = ::mica::util::lcore.lcore_id();
  auto& thread_state = thread_states_[lcore_id];
  return thread_state.outstanding_request_count <
         StaticConfig::kMaxOutstandingRequestCount;
}

template <class StaticConfig>
template <class ResponseHandler>
void DatagramClient<StaticConfig>::handle_response(ResponseHandler& rh) {
  size_t lcore_id = ::mica::util::lcore.lcore_id();
  auto& thread_state = thread_states_[lcore_id];

  uint64_t now = stopwatch_.now();

  std::array<PacketBuffer*, StaticConfig::kRXBurst> bufs;

  for (uint16_t eindex = 0; eindex < thread_state.endpoint_count; eindex++) {
    auto& rx_tx_state = thread_state.rx_tx_states[eindex];

    check_pending_tx_min(rx_tx_state);
    check_pending_tx_timeout(rx_tx_state, now);

    while (true) {
      uint16_t count;
      if (!StaticConfig::kSkipRX)
        count = network_->receive(rx_tx_state.eid, bufs.data(),
                                  StaticConfig::kRXBurst);
      else
        count = 0;

      for (uint16_t i = 0; i < count; i++) {
        if (i + 1 != count) {
          auto next_buf = bufs[static_cast<size_t>(i + 1)];
          __builtin_prefetch(next_buf->get_data(), 0, 0);
          __builtin_prefetch(next_buf->get_data() + 64, 0, 0);
          // __builtin_prefetch(next_buf->get_data() + 128, 0, 0);
          // __builtin_prefetch(next_buf->get_data() + 192, 0, 0);
        }

        RequestBatchReader<PacketBuffer> r(bufs[i]);

        if (!r.is_valid() || !r.is_response()) continue;

        if (StaticConfig::kVerbose)
          printf("lcore %2zu received a batch\n",
                 ::mica::util::lcore.lcore_id());

        while (r.find_next()) {
          uint32_t opaque = r.get_opaque();

          // Check if the opaque contains the valid request descriptor.
          auto rd = static_cast<RequestDescriptor>(opaque >> 16);
          if (rd >= StaticConfig::kMaxOutstandingRequestCount) {
            // Ignore probes.
            continue;
          }

          // Ignore if the event epoch does not match.
          uint16_t epoch = opaque & ((1 << 16) - 1);
          if (thread_state.rd_items[rd].epoch != epoch) continue;

          // Call the handler.
          auto result = r.get_result();
          rh.handle(rd, result, r.get_value(), r.get_value_length(),
                    thread_state.rd_items[rd].arg);

          release_rd(thread_state, rd);

          worker_stats_[lcore_id].num_operations_done++;
          if (result == Result::kSuccess)
            worker_stats_[lcore_id].num_operations_succeeded++;
          else if (result == Result::kRejected)
            worker_stats_[lcore_id].num_operations_rejected++;
        }
      }

      for (uint16_t i = 0; i < count; i++) network_->release(bufs[i]);

      if (lcore_id == 0 &&
          ++thread_state.report_status_check >=
              thread_state.report_status_check_max) {
        thread_state.report_status_check = 0;

        double time_diff =
            stopwatch_.diff(now, thread_state.last_status_report);
        if (time_diff >= 1.) {
          thread_state.last_status_report = now;
          report_status(time_diff);
        }
      }

      if (count < StaticConfig::kRXBurst) break;
    }
  }

  // Check timed out requests.
  while (thread_state.rd_active_first != kNullRD &&
         (StaticConfig::kSkipRX ||
          static_cast<int64_t>(stopwatch_.diff_in_cycles(
              now, thread_state.rd_items[thread_state.rd_active_first]
                       .expire_time)) >= 0)) {
    auto rd = thread_state.rd_active_first;

    // Call the handler.
    rh.handle(rd, Result::kTimedOut, nullptr, 0, thread_state.rd_items[rd].arg);

    release_rd(thread_state, rd);

    worker_stats_[lcore_id].num_operations_done++;
    worker_stats_[lcore_id].num_operations_timeout++;
  }

  if (thread_state.need_to_update_remote_eid) {
    thread_state.need_to_update_remote_eid = false;
    update_remote_eid();
  }

  worker_stats_[lcore_id].alive = 1;
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::noop_read(uint64_t key_hash, const char* key,
                                        size_t key_length,
                                        const Argument& arg) {
  return append_request(Operation::kNoopRead, key_hash, key, key_length,
                        nullptr, 0, arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::noop_write(uint64_t key_hash, const char* key,
                                         size_t key_length, const char* value,
                                         size_t value_length,
                                         const Argument& arg) {
  return append_request(Operation::kNoopWrite, key_hash, key, key_length, value,
                        value_length, arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::del(uint64_t key_hash, const char* key,
                                  size_t key_length, const Argument& arg) {
  return append_request(Operation::kDelete, key_hash, key, key_length, nullptr,
                        0, arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::get(uint64_t key_hash, const char* key,
                                  size_t key_length, const Argument& arg) {
  return append_request(Operation::kGet, key_hash, key, key_length, nullptr, 0,
                        arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::increment(uint64_t key_hash, const char* key,
                                        size_t key_length, uint64_t increment,
                                        const Argument& arg) {
  return append_request(Operation::kIncrement, key_hash, key, key_length,
                        &increment, sizeof(uint64_t), arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::set(uint64_t key_hash, const char* key,
                                  size_t key_length, const char* value,
                                  size_t value_length, bool overwrite,
                                  const Argument& arg) {
  return append_request(overwrite ? Operation::kSet : Operation::kAdd, key_hash,
                        key, key_length, value, value_length, arg);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::test(uint64_t key_hash, const char* key,
                                   size_t key_length, const Argument& arg) {
  return append_request(Operation::kTest, key_hash, key, key_length, nullptr, 0,
                        arg);
}

template <class StaticConfig>
uint32_t DatagramClient<StaticConfig>::calc_server_index(uint64_t key_hash,
                                                         size_t server_count) {
  return static_cast<uint32_t>(((key_hash >> 32) & ((1 << 16) - 1)) %
                               server_count);
}

template <class StaticConfig>
uint16_t DatagramClient<StaticConfig>::calc_partition_id(
    uint64_t key_hash, size_t partition_count) {
  return static_cast<uint16_t>((key_hash >> 48) % partition_count);
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::append_request(
    Operation operation, uint64_t key_hash, const char* key, size_t key_length,
    const char* value, size_t value_length, const Argument& arg) {
  assert(can_request(key_hash));

  size_t lcore_id = ::mica::util::lcore.lcore_id();
  auto& thread_state = thread_states_[lcore_id];

  uint64_t now = stopwatch_.now();

  // Get the node.
  auto server_index = calc_server_index(key_hash, servers_.size());
  auto& s = servers_[server_index];
  auto& request_state = thread_state.request_states[server_index];

  // See if we should care about the partition.
  bool spread_request;
  if (StaticConfig::kIgnoreServerPartition)
    spread_request = true;
  else {
    if (operation == Operation::kNoopRead || operation == Operation::kGet ||
        operation == Operation::kTest)
      spread_request = s.concurrent_read != 0;
    else
      spread_request = s.concurrent_write != 0;
  }

  PendingRequestBatch* prb;
  if (spread_request) {
    prb = &request_state.c;
    if (StaticConfig::kVerbose)
      printf("appending a concurrent request to server %s\n",
             s.server_name.c_str());
  } else {
    // Get partition that may hold the key.
    auto partition_id = calc_partition_id(key_hash, s.partition_count);
    prb = &request_state.e[partition_id];
    if (StaticConfig::kVerbose)
      printf("appending an exclusive request to partition %" PRIu16
             " on server %s\n",
             partition_id, s.server_name.c_str());
  }
  // auto& rx_tx_state = thread_state.rx_tx_states[prb->remote_eid_index];

  if (!prb->b.has_sufficient_space(key_length, value_length)) {
    if (StaticConfig::kVerbose)
      printf("flushing the pending batch due to lack of space\n");

    flush_pending_request_batch(thread_state, server_index, *prb, now);
    make_new_pending_request_batch(*prb);

    // Give up if the item is still too large.
    if (!prb->b.has_sufficient_space(key_length, value_length)) return kNullRD;
  }

  // Allocate an event id.
  RequestDescriptor rd = allocate_rd(thread_state, now, arg);

  // Generate an opaque number.
  uint32_t opaque = (static_cast<uint32_t>(rd) << 16) |
                    static_cast<uint32_t>(thread_state.rd_items[rd].epoch);

  // Append a request.
  prb->b.append_request(operation, Result::kSuccess, opaque, key_hash, key,
                        key_length, value, value_length);

  if (prb->b.get_count() >= StaticConfig::kMaxRequestBatchSize) {
    if (StaticConfig::kVerbose)
      printf("flushing the pending batch due to batch size limit\n");
    flush_pending_request_batch(thread_state, server_index, *prb, now);
    make_new_pending_request_batch(*prb);
  }

  return rd;
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::init_rds(ThreadState& thread_state) {
  // Initialize the unused request descriptors.
  for (RequestDescriptor rd = 0;
       rd < static_cast<RequestDescriptor>(
                StaticConfig::kMaxOutstandingRequestCount);
       rd++) {
    thread_state.rd_items[rd].next = static_cast<RequestDescriptor>(rd + 1);
    thread_state.rd_items[rd].epoch = 0;
  }
  thread_state.rd_items[StaticConfig::kMaxOutstandingRequestCount - 1].next =
      kNullRD;

  thread_state.rd_unused_first = 0;
  thread_state.rd_active_first = kNullRD;
  thread_state.rd_active_last = kNullRD;

  thread_state.outstanding_request_count = 0;
}

template <class StaticConfig>
typename DatagramClient<StaticConfig>::RequestDescriptor
DatagramClient<StaticConfig>::allocate_rd(ThreadState& thread_state,
                                          uint64_t now, const Argument& arg) {
  assert(thread_state.rd_unused_first != kNullRD);

  RequestDescriptor rd = thread_state.rd_unused_first;
  thread_state.rd_unused_first = thread_state.rd_items[rd].next;

  thread_state.rd_items[rd].prev = thread_state.rd_active_last;
  thread_state.rd_items[rd].next = kNullRD;

  if (thread_state.rd_active_first == kNullRD) {
    assert(thread_state.rd_active_last == kNullRD);
    thread_state.rd_active_first = rd;
    thread_state.rd_active_last = rd;
  } else {
    assert(thread_state.rd_active_last != kNullRD);
    thread_state.rd_items[thread_state.rd_active_last].next = rd;
    thread_state.rd_active_last = rd;
  }

  thread_state.rd_items[rd].expire_time =
      now + stopwatch_.c_1_usec() * StaticConfig::kRequestTimeout;
  thread_state.rd_items[rd].arg = arg;

  thread_state.outstanding_request_count++;
  if (StaticConfig::kVerbose)
    printf("allocate_rd(): lcore %zu: outstanding_request_count: %" PRIu16 "\n",
           ::mica::util::lcore.lcore_id(),
           thread_state.outstanding_request_count);

  return rd;
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::release_rd(ThreadState& thread_state,
                                              RequestDescriptor rd) {
  if (thread_state.rd_items[rd].next != kNullRD)
    thread_state.rd_items[thread_state.rd_items[rd].next].prev =
        thread_state.rd_items[rd].prev;
  if (thread_state.rd_items[rd].prev != kNullRD)
    thread_state.rd_items[thread_state.rd_items[rd].prev].next =
        thread_state.rd_items[rd].next;

  if (thread_state.rd_active_first == rd)
    thread_state.rd_active_first = thread_state.rd_items[rd].next;
  if (thread_state.rd_active_last == rd)
    thread_state.rd_active_last = thread_state.rd_items[rd].prev;

  // Prepend this event into the unused event list.
  thread_state.rd_items[rd].next = thread_state.rd_unused_first;
  thread_state.rd_unused_first = rd;

  // Increment the epoch of the completed event so that any responses do not
  // match against this event.
  thread_state.rd_items[rd].epoch++;

  thread_state.outstanding_request_count--;
  if (StaticConfig::kVerbose)
    printf("release_rd(): lcore %zu: outstanding_request_count: %" PRIu16 "\n",
           ::mica::util::lcore.lcore_id(),
           thread_state.outstanding_request_count);
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::make_new_pending_request_batch(
    PendingRequestBatch& prb) {
  if (StaticConfig::kVerbose) printf("make_new_pending_request_batch()\n");
  auto buf = network_->allocate();
  assert(buf != nullptr);

  __builtin_prefetch(buf->get_data(), 1, 0);
  __builtin_prefetch(buf->get_data() + 64, 1, 0);
  // __builtin_prefetch(buf->get_data() + 128, 1, 0);
  // __builtin_prefetch(buf->get_data() + 192, 1, 0);

  prb.b = RequestBatchBuilder<PacketBuffer>(buf);
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::release_pending_request_batch(
    PendingRequestBatch& prb) {
  network_->release(prb.b.get_buffer());
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::flush_pending_request_batch(
    ThreadState& thread_state, uint32_t server_index, PendingRequestBatch& prb,
    uint64_t now) {
  assert(prb.b.get_count() != 0);

  // No available remote endpoint.
  if (prb.remote_eid_index == StaticConfig::kMaxEndpointsPerServer) {
    if (StaticConfig::kVerbose) printf("no remote endpoint found\n");
    network_->release(prb.b.get_buffer());
    return;
  }

  auto& request_state = thread_state.request_states[server_index];
  uint16_t eindex = request_state.local_eid_index[prb.remote_eid_index];

  // No available local endpoint.
  if (eindex == StaticConfig::kMaxEndpointsPerServer) {
    if (StaticConfig::kVerbose) printf("no local endpoint found\n");
    network_->release(prb.b.get_buffer());
    return;
  }

  auto& s = servers_[server_index];
  const ServerEndpointInfo& sei = s.endpoints[prb.remote_eid_index];

  auto& rx_tx_state = thread_state.rx_tx_states[eindex];

  if (StaticConfig::kVerbose) {
    // printf("server_index: %" PRIu32 "\n", server_index);
    // printf("remote_eid_index: %" PRIu16 "\n", prb.remote_eid_index);
    // printf("eindex: %" PRIu16 "\n", eindex);
    printf("using local endpoint %" PRIu32 " for remote endpoint %" PRIu32
           " on server %s\n",
           rx_tx_state.eid, sei.eid, s.server_name.c_str());
  }

  if (rx_tx_state.pending_tx.count == 0)
    rx_tx_state.pending_tx.oldest_time = now;

  prb.b.set_src_mac_addr(network_->get_endpoint_info(rx_tx_state.eid).mac_addr);
  prb.b.set_dest_mac_addr(sei.mac_addr);

  prb.b.set_src_ipv4_addr(
      rte_cpu_to_be_32(network_->get_endpoint_info(rx_tx_state.eid).ipv4_addr));
  prb.b.set_dest_ipv4_addr(rte_cpu_to_be_32(sei.ipv4_addr));

  prb.b.set_src_udp_port(
      rte_cpu_to_be_16(network_->get_endpoint_info(rx_tx_state.eid).udp_port));
  prb.b.set_dest_udp_port(rte_cpu_to_be_16(sei.udp_port));

  prb.b.set_request();

  prb.b.finalize();

  // Moves the buffer to PendingTX.
  rx_tx_state.pending_tx.bufs[rx_tx_state.pending_tx.count] =
      prb.b.get_buffer();
  rx_tx_state.pending_tx.count++;

  if (StaticConfig::kVerbose)
    printf("local endpoint %" PRIu32 ": pending_tx.count: %" PRIu16 "\n",
           rx_tx_state.eid, rx_tx_state.pending_tx.count);

  if (prb.round_robin) {
    prb.remote_eid_index++;
    if (prb.remote_eid_index == s.endpoint_count) prb.remote_eid_index = 0;
  }

  check_pending_tx_full(rx_tx_state);
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::check_pending_tx_full(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count < StaticConfig::kTXBurst) return;

  // Send all pending packets to the networks
  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;

  if (StaticConfig::kVerbose)
    printf("lcore %2zu sent a batch (burst)\n", ::mica::util::lcore.lcore_id());
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::check_pending_tx_min(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count < StaticConfig::kTXMinBurst) return;

  // Send all pending packets to the networks
  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;

  if (StaticConfig::kVerbose)
    printf("lcore %2zu sent a batch (burst)\n", ::mica::util::lcore.lcore_id());
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::check_pending_tx_timeout(
    RXTXState& rx_tx_state, uint64_t now) {
  if (rx_tx_state.pending_tx.count == 0) return;

  if (stopwatch_.diff_in_cycles(now, rx_tx_state.pending_tx.oldest_time) <
      stopwatch_.c_1_usec() * StaticConfig::kTXBurstTimeout)
    return;

  // Send all pending packets to the networks
  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;

  if (StaticConfig::kVerbose)
    printf("lcore %2zu sent a batch (timeout)\n",
           ::mica::util::lcore.lcore_id());
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::release_pending_tx(RXTXState& rx_tx_state) {
  for (uint16_t i = 0; i < rx_tx_state.pending_tx.count; i++)
    network_->release(rx_tx_state.pending_tx.bufs[i]);
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::reset_status() {
  worker_stats_.resize(0);
  worker_stats_.resize(::mica::util::lcore.lcore_count(),
                       {0, 0, 0, 0, 0, 0, 0, 0, 0});

  endpoint_stats_.resize(0);
  endpoint_stats_.resize(Network::kMaxEndpointCount, {0, 0, 0, 0, 0});

  auto eids = network_->get_endpoints();
  for (size_t i = 0; i < eids.size(); i++) {
    auto eid = eids[i];
    auto& ei = network_->get_endpoint_info(eid);
    endpoint_stats_[eid].last_rx_bursts = ei.rx_bursts;
    endpoint_stats_[eid].last_rx_packets = ei.rx_packets;
    endpoint_stats_[eid].last_tx_bursts = ei.tx_bursts;
    endpoint_stats_[eid].last_tx_packets = ei.tx_packets;
    endpoint_stats_[eid].last_tx_dropped = ei.tx_dropped;
  }
}

template <class StaticConfig>
void DatagramClient<StaticConfig>::report_status(double time_diff) {
  uint64_t total_alive = 0;
  uint64_t total_operations_done = 0;
  uint64_t total_operations_succeeded = 0;
  uint64_t total_operations_rejected = 0;
  uint64_t total_operations_timeout = 0;

  uint64_t rx_bursts = 0;
  uint64_t rx_packets = 0;
  uint64_t tx_bursts = 0;
  uint64_t tx_packets = 0;
  uint64_t tx_dropped = 0;

  for (size_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
       lcore_id++) {
    {
      uint64_t& v = worker_stats_[lcore_id].alive;
      total_alive += v;
      v = 0;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_done;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_done;
      total_operations_done += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_succeeded;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_succeeded;
      total_operations_succeeded += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_rejected;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_rejected;
      total_operations_rejected += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_timeout;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_timeout;
      total_operations_timeout += v - last_v;
      last_v = v;
    }
  }

  auto eids = network_->get_endpoints();
  for (size_t i = 0; i < eids.size(); i++) {
    auto eid = eids[i];
    auto& ei = network_->get_endpoint_info(eid);
    {
      uint64_t v = ei.rx_bursts;
      uint64_t& last_v = endpoint_stats_[eid].last_rx_bursts;
      rx_bursts += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.rx_packets;
      uint64_t& last_v = endpoint_stats_[eid].last_rx_packets;
      rx_packets += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_bursts;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_bursts;
      tx_bursts += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_packets;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_packets;
      tx_packets += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_dropped;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_dropped;
      tx_dropped += v - last_v;
      last_v = v;
    }
  }

  time_diff = std::max(0.01, time_diff);

  double success_rate =
      static_cast<double>(total_operations_succeeded) /
      static_cast<double>(std::max(total_operations_done, uint64_t(1)));
  double reject_rate =
      static_cast<double>(total_operations_rejected) /
      static_cast<double>(std::max(total_operations_done, uint64_t(1)));
  double timeout_rate =
      static_cast<double>(total_operations_timeout) /
      static_cast<double>(std::max(total_operations_done, uint64_t(1)));

  printf("tput=%7.3lf Mops",
         static_cast<double>(total_operations_done) / time_diff / 1000000.);
  printf(", success_rate=%6.2lf%%", success_rate * 100.);
  printf(", reject_rate=%6.2lf%%", reject_rate * 100.);
  printf(", timeout_rate=%6.2lf%%", timeout_rate * 100.);
  printf(", RX=%7.3lf Mpps (%5.2lf ppb)",
         static_cast<double>(rx_packets) / time_diff / 1000000.,
         static_cast<double>(rx_packets) /
             static_cast<double>(std::max(rx_bursts, uint64_t(1))));
  printf(", TX=%7.3lf Mpps (%5.2lf ppb)",
         static_cast<double>(tx_packets) / time_diff / 1000000.,
         static_cast<double>(tx_packets) /
             static_cast<double>(std::max(tx_bursts, uint64_t(1))));
  printf(", TX_drop=%7.3lf Mpps",
         static_cast<double>(tx_dropped) / time_diff / 1000000.);
  printf(", threads=%2" PRIu64 "/%2zu", total_alive,
         ::mica::util::lcore.lcore_count());

  printf("\n");
  if (flush_status_report_) fflush(stdout);
}
}
}

#endif

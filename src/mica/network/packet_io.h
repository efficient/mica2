#pragma once
#ifndef MICA_NETWORK_PACKET_IO_H_
#define MICA_NETWORK_PACKET_IO_H_

#include <memory>
#include "mica/common.h"

namespace mica {
namespace network {
class PacketBufferInterface {
 public:
  uint16_t get_length() const;
  uint16_t get_headroom() const;
  uint16_t get_tailroom() const;

  char* get_data();
  const char* get_data() const;

  void set_length(uint16_t len);
  char* prepend(uint16_t len);
  char* append(uint16_t len);
  char* adj(uint16_t len);
  char* trim(uint16_t len);
};

class PacketIOInterface {
 public:
  typedef PacketBufferInterface PacketBuffer;

  typedef uint32_t EndpointId;
  static constexpr EndpointId kInvalidEndpointId =
      std::numeric_limits<EndpointId>::max();

  struct EndpointInfo {
    uint16_t owner_lcore_id;

    volatile uint64_t rx_bursts;
    volatile uint64_t rx_packets;

    volatile uint64_t tx_bursts;
    volatile uint64_t tx_packets;
    volatile uint64_t tx_dropped;
  } __attribute__((aligned(128)));

  // static constexpr uint16_t kMaxBurstSize = 1;

  std::vector<EndpointId> get_endpoints() const;
  const EndpointInfo& get_endpoint_info(EndpointId eid) const;

  void start();
  void stop();

  PacketBuffer* allocate();
  PacketBuffer* clone(PacketBuffer* buf);
  void release(PacketBuffer* buf);

  uint16_t receive(EndpointId eid, PacketBuffer** bufs, uint16_t buf_count);
  uint16_t send(EndpointId eid, PacketBuffer** bufs, uint16_t buf_count);
};
}
}

#endif
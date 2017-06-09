#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_PROTOCOL_H_
#define MICA_DATAGRAM_DATAGRAM_PROTOCOL_H_

#include "mica/common.h"
#include <rte_common.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include "mica/processor/types.h"
#include "mica/table/types.h"

namespace mica {
namespace datagram {
struct RequestBatchHeader {
  // 0
  uint8_t header[sizeof(ether_hdr) + sizeof(ipv4_hdr) + sizeof(udp_hdr)];
  // 42
  uint8_t magic;  // 0x78 for requests; 0x79 for responses.
  uint8_t num_requests;
  // 44
  uint32_t reserved0;
  // 48
  // KeyValueRequestHeader
};

struct RequestHeader {
  // 0
  uint8_t operation;  // ::mica::processor::Operation
  uint8_t result;     // ::mica::table::Result
  // 2
  uint16_t reserved0;
  // 4
  uint32_t kv_length_vec;  // key_length: 8, value_length: 24
  // 8
  uint64_t key_hash;
  // 16
  uint32_t opaque;
  uint32_t reserved1;
  // 24
  // Key-value data
};

// A helper class that reads the information from a buffer.
template <class PacketBuffer>
class RequestBatchReader {
 public:
  typedef ::mica::processor::Operation Operation;
  typedef ::mica::table::Result Result;

  explicit RequestBatchReader(const PacketBuffer* buf)
      : buf_(buf), h_(nullptr) {}

  bool is_valid() const {
    auto mac = reinterpret_cast<const ether_hdr*>(buf_->get_data());
    auto bh = reinterpret_cast<const RequestBatchHeader*>(buf_->get_data());
    auto ip =
        reinterpret_cast<const ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    auto udp = reinterpret_cast<const udp_hdr*>(
        buf_->get_data() + sizeof(ether_hdr) + sizeof(ipv4_hdr));

    auto len = buf_->get_length();

    if (len < sizeof(RequestBatchHeader)) {
      // printf("too short packet length\n");
      return false;
    }

    if (mac->ether_type != rte_be_to_cpu_16(ETHER_TYPE_IPv4)) {
      // printf("invalid network layer protocol\n");
      return false;
    }

    if (ip->version_ihl != (0x40 | 0x05)) {
      // printf("invalid IP layer protocol\n");
      return false;
    }

    if (ip->packet_id != 0 || ip->fragment_offset != 0) {
      // printf("ignoring fragmented IP packet\n");
      return false;
    }

    if (rte_be_to_cpu_16(ip->total_length) != len - sizeof(ether_hdr)) {
      // printf("invalid IP packet length\n");
      return false;
    }

    if (ip->next_proto_id != IPPROTO_UDP) {
      // printf("invalid transport layer protocol\n");
      return false;
    }

    if (rte_be_to_cpu_16(udp->dgram_len) !=
        len - sizeof(struct ether_hdr) - sizeof(ipv4_hdr)) {
      // printf("invalid UDP datagram length\n");
      return false;
    }

    if (bh->magic != 0x78 && bh->magic != 0x79) {
      printf("invalid magic\n");
      return false;
    }

    return true;
  }

  const ether_addr& get_src_mac_addr() const {
    auto mac = reinterpret_cast<const ether_hdr*>(buf_->get_data());
    return mac->s_addr;
  }

  const ether_addr& get_dest_mac_addr() const {
    auto mac = reinterpret_cast<const ether_hdr*>(buf_->get_data());
    return mac->d_addr;
  }

  uint32_t get_src_ipv4_addr() const {
    auto ip =
        reinterpret_cast<const ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    return ip->src_addr;
  }

  uint32_t get_dest_ipv4_addr() const {
    auto ip =
        reinterpret_cast<const ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    return ip->dst_addr;
  }

  uint16_t get_src_udp_port() const {
    auto udp = reinterpret_cast<const udp_hdr*>(
        buf_->get_data() + sizeof(ether_hdr) + sizeof(ipv4_hdr));
    return udp->src_port;
  }

  uint16_t get_dest_udp_port() const {
    auto udp = reinterpret_cast<const udp_hdr*>(
        buf_->get_data() + sizeof(ether_hdr) + sizeof(ipv4_hdr));
    return udp->dst_port;
  }

  bool is_request() const {
    auto bh = reinterpret_cast<const RequestBatchHeader*>(buf_->get_data());
    return bh->magic == 0x78;
  }

  bool is_response() const {
    auto bh = reinterpret_cast<const RequestBatchHeader*>(buf_->get_data());
    return bh->magic == 0x79;
  }

  bool find_next() {
    if (h_ == nullptr)
      h_ = reinterpret_cast<const RequestHeader*>(buf_->get_data() +
                                                  sizeof(RequestBatchHeader));
    else
      h_ = reinterpret_cast<const RequestHeader*>(
          reinterpret_cast<const char*>(h_) + sizeof(RequestHeader) +
          ::mica::util::roundup<8>(get_key_length()) +
          ::mica::util::roundup<8>(get_value_length()));

    auto len = buf_->get_length();
    if (reinterpret_cast<const char*>(h_) + sizeof(RequestHeader) -
            buf_->get_data() >
        len)
      return false;
    if (reinterpret_cast<const char*>(h_) + sizeof(RequestHeader) +
            ::mica::util::roundup<8>(get_key_length()) +
            ::mica::util::roundup<8>(get_value_length()) - buf_->get_data() >
        len)
      return false;

    return true;
  }

  Operation get_operation() const {
    return static_cast<Operation>(h_->operation);
  }

  Result get_result() const { return static_cast<Result>(h_->result); }

  uint32_t get_opaque() const { return h_->opaque; }

  uint64_t get_key_hash() const { return h_->key_hash; }

  const char* get_key() const {
    return reinterpret_cast<const char*>(h_) + sizeof(RequestHeader);
  }

  size_t get_key_length() const { return h_->kv_length_vec >> 24; }

  const char* get_value() const {
    return get_key() + ::mica::util::roundup<8>(get_key_length());
  }

  size_t get_value_length() const {
    return h_->kv_length_vec & ((1 << 24) - 1);
  }

 private:
  const PacketBuffer* buf_;
  const RequestHeader* h_;
};

// A helper class that builds a new request batch.  This is not a "writer"
// because it provides a method to directly write key and value data into the
// buffer.
template <class PacketBuffer>
class RequestBatchBuilder {
 public:
  typedef ::mica::processor::Operation Operation;
  typedef ::mica::table::Result Result;

  static constexpr uint16_t kMaxValueLength = ETHER_MAX_LEN - ETHER_CRC_LEN -
                                              sizeof(RequestBatchHeader) -
                                              sizeof(RequestHeader);

  RequestBatchBuilder()
      : buf_(nullptr), h_(nullptr), remaining_space_(0), count_(0) {}

  explicit RequestBatchBuilder(PacketBuffer* buf)
      : buf_(buf),
        h_(reinterpret_cast<RequestHeader*>(buf_->get_data() +
                                            sizeof(RequestBatchHeader))),
        remaining_space_(kMaxValueLength),
        count_(0) {}

  RequestBatchBuilder(const RequestBatchBuilder& o)
      : buf_(o.buf_),
        h_(o.h_),
        remaining_space_(o.remaining_space_),
        count_(o.count_) {}

  // TODO: Support a packet template?

  PacketBuffer* get_buffer() { return buf_; }

  void set_src_mac_addr(const ether_addr& v) {
    auto mac = reinterpret_cast<ether_hdr*>(buf_->get_data());
    mac->s_addr = v;
  }

  void set_dest_mac_addr(const ether_addr& v) {
    auto mac = reinterpret_cast<ether_hdr*>(buf_->get_data());
    mac->d_addr = v;
  }

  void set_src_ipv4_addr(uint32_t v) {
    auto ip = reinterpret_cast<ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    ip->src_addr = v;
  }

  void set_dest_ipv4_addr(uint32_t v) {
    auto ip = reinterpret_cast<ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    ip->dst_addr = v;
  }

  void set_src_udp_port(uint16_t v) const {
    auto udp = reinterpret_cast<udp_hdr*>(buf_->get_data() + sizeof(ether_hdr) +
                                          sizeof(ipv4_hdr));
    udp->src_port = v;
  }

  void set_dest_udp_port(uint16_t v) const {
    auto udp = reinterpret_cast<udp_hdr*>(buf_->get_data() + sizeof(ether_hdr) +
                                          sizeof(ipv4_hdr));
    udp->dst_port = v;
  }

  void set_request() {
    auto bh = reinterpret_cast<RequestBatchHeader*>(buf_->get_data());
    bh->magic = 0x78;
  }

  void set_response() {
    auto bh = reinterpret_cast<RequestBatchHeader*>(buf_->get_data());
    bh->magic = 0x79;
  }

  size_t get_count() const { return count_; }

  bool has_sufficient_space(size_t key_length, size_t value_length) const {
    return ::mica::util::roundup<8>(key_length) +
               ::mica::util::roundup<8>(value_length) <=
           remaining_space_;
  }

  char* get_out_key() {
    auto p = reinterpret_cast<char*>(h_) + sizeof(RequestHeader);
    return p;
  }

  size_t get_max_out_key_length() { return remaining_space_; }

  char* get_out_value(size_t key_length) {
    auto p = reinterpret_cast<char*>(h_) + sizeof(RequestHeader) +
             ::mica::util::roundup<8>(key_length);
    return p;
  }

  size_t get_max_out_value_length(size_t key_length) {
    return remaining_space_ - ::mica::util::roundup<8>(key_length);
  }

  void append_request_no_key_value(Operation operation, Result result,
                                   uint32_t opaque, uint64_t key_hash,
                                   size_t key_length, size_t value_length) {
    assert(has_sufficient_space(key_length, value_length));
    assert(count_ != 0xff);
    assert(key_length < (1 << 8));
    assert(value_length < (1 << 24));

    h_->operation = static_cast<uint8_t>(operation);
    h_->result = static_cast<uint8_t>(result);
    h_->key_hash = key_hash;
    h_->opaque = opaque;
    h_->reserved0 = 0;
    h_->kv_length_vec =
        static_cast<uint32_t>((key_length << 24) | value_length);

    auto p = reinterpret_cast<char*>(h_) + sizeof(RequestHeader) +
             ::mica::util::roundup<8>(key_length) +
             ::mica::util::roundup<8>(value_length);

    h_ = reinterpret_cast<RequestHeader*>(p);
    count_++;
  }

  void append_request(Operation operation, Result result, uint32_t opaque,
                      uint64_t key_hash, const char* key, size_t key_length,
                      const char* value, size_t value_length) {
    assert(has_sufficient_space(key_length, value_length));
    assert(count_ != 0xff);
    assert(key_length < (1 << 8));
    assert(value_length < (1 << 24));

    h_->operation = static_cast<uint8_t>(operation);
    h_->result = static_cast<uint8_t>(result);
    h_->key_hash = key_hash;
    h_->opaque = opaque;
    h_->reserved0 = 0;
    h_->kv_length_vec =
        static_cast<uint32_t>((key_length << 24) | value_length);

    auto p = reinterpret_cast<char*>(h_) + sizeof(RequestHeader);
    ::mica::util::memcpy<8>(p, key, ::mica::util::roundup<8>(key_length));
    p += ::mica::util::roundup<8>(key_length);
    ::mica::util::memcpy<8>(p, value, ::mica::util::roundup<8>(value_length));
    p += ::mica::util::roundup<8>(value_length);

    h_ = reinterpret_cast<RequestHeader*>(p);
    count_++;
  }

  void finalize() {
    auto mac = reinterpret_cast<ether_hdr*>(buf_->get_data());
    auto bh = reinterpret_cast<RequestBatchHeader*>(buf_->get_data());
    auto ip = reinterpret_cast<ipv4_hdr*>(buf_->get_data() + sizeof(ether_hdr));
    auto udp = reinterpret_cast<udp_hdr*>(buf_->get_data() + sizeof(ether_hdr) +
                                          sizeof(ipv4_hdr));

    size_t packet_length =
        static_cast<size_t>(reinterpret_cast<char*>(h_) - buf_->get_data());
    buf_->set_length(static_cast<uint16_t>(packet_length));
    ip->total_length = rte_cpu_to_be_16(
        static_cast<uint16_t>(packet_length - sizeof(ether_hdr)));
    udp->dgram_len = rte_cpu_to_be_16(static_cast<uint16_t>(
        packet_length - sizeof(ether_hdr) - sizeof(ipv4_hdr)));

    mac->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

    ip->version_ihl = 0x40 | 0x05;
    ip->type_of_service = 0;  // XXX: Do we want to use a different ToS?
    ip->packet_id = 0;
    ip->fragment_offset = 0;
    ip->time_to_live = 64;
    ip->next_proto_id = IPPROTO_UDP;
    ip->hdr_checksum = 0;  // XXX: To be updated.

    udp->dgram_cksum = 0;

    bh->num_requests = static_cast<uint8_t>(count_);

    // TODO: Update IP header checksum or use TX checksum offloading.
  }

 private:
  PacketBuffer* buf_;
  RequestHeader* h_;
  uint16_t remaining_space_;
  // We use a separate count instead of num_requests in the packet data.  This
  // delays touching the packet memory as long as possible to make prefetching
  // more effective.
  uint16_t count_;
};
}
}

#endif
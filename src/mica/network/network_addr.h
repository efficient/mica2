#pragma once
#ifndef MICA_NETWORK_ADDR_H_
#define MICA_NETWORK_ADDR_H_

namespace mica {
namespace network {
class NetworkAddress {
 public:
  static uint32_t parse_ipv4_addr(const char* s) {
    char* p = const_cast<char*>(s);
    uint32_t v = 0;
    for (size_t i = 0; i < 4; i++, p++) {
      assert(p && *p != '\0');
      v = (v << 8) + static_cast<uint32_t>(
                         ::mica::util::safe_cast<uint8_t>(strtoul(p, &p, 10)));
    }
    return v;
  }

  static ether_addr parse_mac_addr(const char* s) {
    char* p = const_cast<char*>(s);
    ether_addr v;
    for (size_t i = 0; i < 6; i++, p++) {
      assert(p && *p != '\0');
      v.addr_bytes[i] = static_cast<uint32_t>(
          ::mica::util::safe_cast<uint8_t>(strtoul(p, &p, 16)));
    }
    return v;
  }

  static std::string str_ipv4_addr(uint32_t addr) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d.%d.%d.%d", addr >> 24, (addr >> 16) & 255,
             (addr >> 8) & 255, addr & 255);
    return std::string(buf);
  }

  static std::string str_mac_addr(const ether_addr& addr) {
    char buf[18];
    snprintf(buf, sizeof(buf), "%02x:%02x:%02x:%02x:%02x:%02x",
             addr.addr_bytes[0], addr.addr_bytes[1], addr.addr_bytes[2],
             addr.addr_bytes[3], addr.addr_bytes[4], addr.addr_bytes[5]);
    return std::string(buf);
  }
};
}
}

#endif
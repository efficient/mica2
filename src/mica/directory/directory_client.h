#pragma once
#ifndef MICA_DIRECTORY_DIRECTORY_CLIENT_H_
#define MICA_DIRECTORY_DIRECTORY_CLIENT_H_

#include <string>
#include <vector>
#include <memory>
#include "mica/common.h"
#include "mica/util/config.h"
#include "mica/directory/etcd_client.h"

// Configuration file entries for DirectoryClient:
//
//  * etcd_addr (string): The IP address or hostname of etcd. Default =
//  "localhost"
//  * etcd_port (integer): The port number of etcd. Default = 2379
//  * etcd_prefix (string): The key prefix in etcd. Default = "/mica"
//  * hostname (string): The unique hostname of the local node.  Must not
//  contain any slash or dot. Default = (detected by gethostname())
//  * append_pid (bool): Append "." + getpid() to the hostname.  Default = true
//  * ttl (integer): TTL of the server information in seconds.  Default = 2
//  * verbose (bool): Print verbose messages.  Default = false

namespace mica {
namespace directory {
class DirectoryClient {
 public:
  DirectoryClient(const ::mica::util::Config& config);
  ~DirectoryClient();

  std::string get_hostname() const;

  void register_server(std::string info);
  void unregister_server();

  void refresh_server() const;

  std::vector<std::string> get_server_list() const;
  std::string get_server_info(std::string name) const;

 private:
  ::mica::util::Config config_;

  std::string etcd_addr_;
  uint16_t etcd_port_;
  std::string etcd_prefix_;
  std::string hostname_;
  uint32_t ttl_;
  bool verbose_;

  std::unique_ptr<EtcdClient> etcd_client_;

  bool registered_;
  std::string info_;
};
}
}

#endif
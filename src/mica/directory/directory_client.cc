// #pragma once
#ifndef MICA_DIRECTORY_DIRECTORY_CLIENT_CC_
#define MICA_DIRECTORY_DIRECTORY_CLIENT_CC_

#include <sys/types.h>
#include <unistd.h>
#include "mica/util/safe_cast.h"
#include "mica/directory/directory_client.h"

namespace mica {
namespace directory {
DirectoryClient::DirectoryClient(const ::mica::util::Config& config)
    : config_(config), registered_(false) {
  etcd_addr_ = config_.get("etcd_addr").get_str("localhost");
  etcd_port_ = ::mica::util::safe_cast<uint16_t>(
      config_.get("etcd_port").get_uint64(2379));
  etcd_prefix_ = config_.get("etcd_prefix").get_str("/mica");

  {
    auto c = config_.get("hostname");
    if (c.exists())
      hostname_ = c.get_str();
    else {
      char buf[128];
      int ret = gethostname(buf, sizeof(buf));
      if (ret < 0) {
        fprintf(stderr, "error: gethostname() failed\n");
        return;
      }
      hostname_ = buf;
    }
  }

  if (config_.get("append_pid").get_bool(true)) {
    char pid[16];
    snprintf(pid, sizeof(pid), ".%u", getpid());
    hostname_ += pid;
  }

  ttl_ = ::mica::util::safe_cast<uint32_t>(config_.get("ttl").get_uint64(2));

  verbose_ = config_.get("verbose").get_bool(false);

  try {
    etcd_client_ = std::make_unique<EtcdClient>(etcd_addr_, etcd_port_);
  } catch (...) {
    fprintf(
        stderr,
        "error: failed to initialize etcd client to the server at %s:%" PRIu16
        "\n",
        etcd_addr_.c_str(), etcd_port_);
  }

  if (verbose_) printf("hostname: %s\n", hostname_.c_str());
}

DirectoryClient::~DirectoryClient() {
  if (registered_) unregister_server();
}

std::string DirectoryClient::get_hostname() const { return hostname_; }

void DirectoryClient::register_server(std::string info) {
  // Allow registering the server multiple times (with new info).
  info_ = info;

  try {
    auto reply = etcd_client_->Set(
        (etcd_prefix_ + "/servers/" + hostname_).c_str(), info_.c_str(), ttl_);

    if (verbose_)
      printf("DirectoryClient::register_server(): %s\n",
             reply.body().dump().c_str());
  } catch (...) {
    fprintf(stderr, "error: failed to register the server node\n");
  }

  registered_ = true;
}

void DirectoryClient::refresh_server() const {
  // TODO: Avoid sending the same information.
  try {
    auto reply = etcd_client_->Set(
        (etcd_prefix_ + "/servers/" + hostname_).c_str(), info_.c_str(), ttl_);

    if (verbose_)
      printf("DirectoryClient::refresh_server(): %s\n",
             reply.body().dump().c_str());
  } catch (...) {
    fprintf(stderr, "error: failed to refresh the server node\n");
  }
}

std::vector<std::string> DirectoryClient::get_server_list() const {
  try {
    std::string prefix = etcd_prefix_ + "/servers/";
    auto reply = etcd_client_->Get((prefix).c_str());

    if (verbose_)
      printf("DirectoryClient::get_server_list(): %s\n",
             reply.body().dump().c_str());

    std::vector<std::string> v;
    auto c = reply.body().get("node").get("nodes");
    if (!c.is_array()) {
      fprintf(stderr, "DirectoryClient::get_server_list(): found no servers\n");
      return v;
    }
    for (size_t i = 0; i < c.size(); i++) {
      v.push_back(c.get(i).get("key").get_str().substr(prefix.size()));
      if (verbose_)
        printf("DirectoryClient::get_server_list(): found server name: %s\n",
               v.back().c_str());
    }
    return v;
  } catch (...) {
    fprintf(stderr, "error: failed to get the server node list\n");
    return std::vector<std::string>();
  }
}

std::string DirectoryClient::get_server_info(std::string name) const {
  try {
    auto reply = etcd_client_->Get((etcd_prefix_ + "/servers/" + name).c_str());

    if (verbose_)
      printf("DirectoryClient::get_server_info(): %s\n",
             reply.body().dump().c_str());

    return reply.body().get("node").get("value").get_str("");
  } catch (...) {
    fprintf(stderr, "error: failed to get the server node information: %s\n",
            name.c_str());
    return std::string();
  }
}

void DirectoryClient::unregister_server() {
  assert(registered_);

  try {
    auto reply = etcd_client_->Delete((etcd_prefix_ + "/servers/").c_str());

    if (verbose_)
      printf("DirectoryClient::unregister_server(): %s\n",
             reply.body().dump().c_str());
  } catch (...) {
    fprintf(stderr, "error: failed to unregister the server node\n");
  }

  registered_ = false;
}
}
}

#endif
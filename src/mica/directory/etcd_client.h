#pragma once
#ifndef MICA_DIRECTORY_ETCD_CLIENT_H_
#define MICA_DIRECTORY_ETCD_CLIENT_H_

#include "mica/util/config.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#include "mica/directory/etcdcpp/etcd_mod.h"
#pragma GCC diagnostic pop

namespace mica {
namespace directory {
class EtcdReply {
 public:
  EtcdReply(const std::string& reply)
      : header_(), body_(::mica::util::Config::load(reply, "<EtcdReply>")) {}

  EtcdReply(const std::string& header, const std::string& reply)
      : header_(header),
        body_(::mica::util::Config::load(reply, "<EtcdReply>")) {}

  ~EtcdReply() {}

  bool ok() const { return !body_.get("errorCode").exists(); }

  const std::string& header() const { return header_; }
  const ::mica::util::Config& body() const { return body_; }

 private:
  std::string header_;
  const ::mica::util::Config body_;
};

typedef ::etcd::Client<EtcdReply> EtcdClient;
}
}

#endif

#include "mica/datagram/datagram_server.h"
#include "mica/processor/simple_processor.h"
#include "mica/table/sqlite.h"
#include "mica/util/lcore.h"

struct DPDKConfig : public ::mica::network::BasicDPDKConfig {
  static constexpr bool kVerbose = true;
};

struct SimpleProcessorConfig
    : public ::mica::processor::BasicSimpleProcessorConfig {
  // static constexpr bool kVerbose = true;
  typedef ::mica::table::Sqlite<> Table;
};

struct DatagramServerConfig
    : public ::mica::datagram::BasicDatagramServerConfig {
  typedef ::mica::processor::SimpleProcessor<SimpleProcessorConfig> Processor;
  typedef ::mica::network::DPDK<DPDKConfig> Network;
  // static constexpr bool kVerbose = true;
};

typedef ::mica::datagram::DatagramServer<DatagramServerConfig> Server;

int main() {
  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("server_sqlite.json");

  Server::DirectoryClient dir_client(config.get("dir_client"));

  SimpleProcessorConfig::Table table(config.get("table"));
  DatagramServerConfig::Processor processor(config.get("processor"), &table);

  DatagramServerConfig::Network network(config.get("network"));
  network.start();

  Server server(config.get("server"), &processor, &network, &dir_client);
  server.run();

  network.stop();

  return EXIT_SUCCESS;
}
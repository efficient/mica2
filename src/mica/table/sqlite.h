#pragma once
#ifndef MICA_TABLE_SQLITE_H_
#define MICA_TABLE_SQLITE_H_

#include <cstdio>
#include <atomic>
#include <sqlite3.h>
#include "mica/table/table.h"
#include "mica/util/config.h"
#include "mica/util/memcpy.h"

// Configuration file entries for Sqlite:
//
//  * path (integer): The path of sqlite file.

namespace mica {
namespace table {
struct BasicSqliteConfig {
  // Be verbose.
  static constexpr bool kVerbose = false;
};

template <class StaticConfig = BasicSqliteConfig>
class Sqlite : public TableInterface {
 public:
  // ltable_impl/init.h
  Sqlite(const ::mica::util::Config& config) : sqlite_(nullptr) {
    auto path = config.get("path").get_str().c_str();
    auto ret = sqlite3_open_v2(
        path, &sqlite_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    if (ret != SQLITE_OK) {
      fprintf(stderr, "failed to open %s\n", path);
      assert(false);
      return;
    }

    if (!sqlite3_get_autocommit(sqlite_)) {
      fprintf(stderr, "failed to verify autocommit mode\n");
      assert(false);
      return;
    }

    char* errmsg;
    ret = sqlite3_exec(sqlite_,
                       "CREATE TABLE IF NOT EXISTS t(keyhash INTEGER, key BLOB "
                       "UNIQUE, value BLOB);",
                       nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
      fprintf(stderr, "failed to create table: %s\n", errmsg);
      assert(false);
      return;
    }

    ret =
        sqlite3_exec(sqlite_, "CREATE INDEX IF NOT EXISTS t_idx ON t(keyhash);",
                     nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
      fprintf(stderr, "failed to create index: %s\n", errmsg);
      assert(false);
      return;
    }
  }

  ~Sqlite() {
    if (sqlite_ != nullptr) {
      if (sqlite3_close_v2(sqlite_) != SQLITE_OK) {
        fprintf(stderr, "failed to close\n");
        assert(false);
      }
    }
  }

  void reset() {
    ScopedLock l(this);

    char* errmsg;
    auto ret =
        sqlite3_exec(sqlite_, "DELETE FROM t;", nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
      fprintf(stderr, "failed to clear table: %s\n", errmsg);
      assert(false);
      return;
    }
  }

  Result del(uint64_t key_hash, const char* key, size_t key_length) {
    ScopedLock l(this);

    sqlite3_stmt* stmt;
    auto sql = "DELETE FROM t WHERE keyhash=? AND key=?;";

    auto ret = sqlite3_prepare_v2(sqlite_, sql, -1, &stmt, nullptr);
    if (ret != SQLITE_OK) {
      assert(false);
      return Result::kError;
    }

    Result result;
    do {
      ret = sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(key_hash));
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }
      ret = sqlite3_bind_blob(stmt, 2, key, static_cast<int>(key_length),
                              nullptr);
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }

      ret = sqlite3_step(stmt);
      if (ret != SQLITE_DONE) {
        assert(false);
        result = Result::kError;
        break;
      }

      if (sqlite3_changes(sqlite_) == 1)
        result = Result::kSuccess;
      else
        result = Result::kNotFound;
    } while (0);

    ret = sqlite3_finalize(stmt);
    if (ret != SQLITE_OK) {
      assert(false);
      result = Result::kError;
    }

    return result;
  }

  Result get(uint64_t key_hash, const char* key, size_t key_length,
             char* out_value, size_t in_value_length, size_t* out_value_length,
             bool allow_mutation) const {
    ScopedLock l(this);
    (void)allow_mutation;

    sqlite3_stmt* stmt;
    auto sql = "SELECT value FROM t where keyhash=? AND key=?";

    auto ret = sqlite3_prepare_v2(sqlite_, sql, -1, &stmt, nullptr);
    if (ret != SQLITE_OK) {
      assert(false);
      return Result::kError;
    }

    Result result;
    do {
      ret = sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(key_hash));
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }
      ret = sqlite3_bind_blob(stmt, 2, key, static_cast<int>(key_length),
                              nullptr);
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }

      ret = sqlite3_step(stmt);
      if (ret != SQLITE_ROW && ret != SQLITE_DONE) {
        assert(false);
        result = Result::kError;
        break;
      }

      if (ret == SQLITE_ROW) {
        result = Result::kSuccess;
        *out_value_length =
            std::min(in_value_length,
                     static_cast<size_t>(sqlite3_column_bytes(stmt, 0)));
        ::mica::util::memcpy(out_value, sqlite3_column_blob(stmt, 0),
                             *out_value_length);
      } else  // (ret == SQLITE_DONE)
        result = Result::kNotFound;
    } while (0);

    ret = sqlite3_finalize(stmt);
    if (ret != SQLITE_OK) {
      assert(false);
      result = Result::kError;
    }
    return result;
  }

  Result increment(uint64_t key_hash, const char* key, size_t key_length,
                   uint64_t increment, uint64_t* out_value) {
    // TODO: Implement.
    (void)key_hash;
    (void)key;
    (void)key_length;
    (void)increment;
    (void)out_value;
    return Result::kNotSupported;
  }

  Result set(uint64_t key_hash, const char* key, size_t key_length,
             const char* value, size_t value_length, bool overwrite) {
    ScopedLock l(this);

    sqlite3_stmt* stmt;
    const char* sql;
    if (overwrite)
      sql = "INSERT OR REPLACE INTO t(keyhash, key, value) VALUES(?, ?, ?);";
    else
      sql = "INSERT OR FAIL INTO t(keyhash, key, value) VALUES(?, ?, ?);";

    auto ret = sqlite3_prepare_v2(sqlite_, sql, -1, &stmt, nullptr);
    if (ret != SQLITE_OK) {
      assert(false);
      return Result::kError;
    }

    Result result;
    do {
      ret = sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(key_hash));
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }
      ret = sqlite3_bind_blob(stmt, 2, key, static_cast<int>(key_length),
                              nullptr);
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }
      ret = sqlite3_bind_blob(stmt, 3, value, static_cast<int>(value_length),
                              nullptr);
      if (ret != SQLITE_OK) {
        assert(false);
        result = Result::kError;
        break;
      }

      ret = sqlite3_step(stmt);
      if (ret != SQLITE_DONE) {
        assert(false);
        result = Result::kError;
        break;
      }

      if (sqlite3_changes(sqlite_) == 1)
        result = Result::kSuccess;
      else
        result = Result::kNotFound;
    } while (0);

    ret = sqlite3_finalize(stmt);
    if (ret != SQLITE_OK) {
      assert(false);
      result = Result::kError;
    }
    return result;
  }

  Result test(uint64_t key_hash, const char* key, size_t key_length) const {
    // TODO: Implement.
    (void)key_hash;
    (void)key;
    (void)key_length;
    return Result::kNotSupported;
  }

  void prefetch_table(uint64_t key_hash) const { (void)key_hash; }
  void prefetch_pool(uint64_t key_hash) const { (void)key_hash; }

  // ltable_impl/info.h
  void print_buckets() const {}
  void print_stats() const {}
  void reset_stats(bool reset_count) { (void)reset_count; }

 private:
  ::mica::util::Config config_;

  sqlite3* sqlite_;
  mutable std::atomic_flag lock_ = ATOMIC_FLAG_INIT;

  void lock() const {
    while (lock_.test_and_set(std::memory_order_acquire)) ::mica::util::pause();
  }

  void unlock() const { lock_.clear(std::memory_order_release); }

  struct ScopedLock {
    ScopedLock(const Sqlite<StaticConfig>* sqlite) : sqlite(sqlite) {
      sqlite->lock();
    }
    ~ScopedLock() { sqlite->unlock(); }
    const Sqlite<StaticConfig>* sqlite;
  };
} __attribute__((aligned(128)));  // To prevent false sharing caused by
                                  // adjacent cacheline prefetching.
}
}

#endif
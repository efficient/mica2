#pragma once
#ifndef MICA_PROCESSOR_PARTITIONS_RECENT_KEY_HASH_H_
#define MICA_PROCESSOR_PARTITIONS_RECENT_KEY_HASH_H_

namespace mica {
namespace processor {
template <class StaticConfig>
bool Partitions<StaticConfig>::is_recent_key_hash(uint16_t lcore_id,
                                                  uint64_t key_hash) const {
  // TODO: Introduce seeded hashing to key_hash so that we can avoid collisions
  // of a few popular items competing for the same bucket.
  // TODO: Adjust associativity dynamically depending on the request style.
  // Low associativity for either extremely skew (e.g., single key) or no skew
  // (i.e., no benefit), high associativity for moderate skew.

  // We need to shift key_hash only by 16 bits to obtain uint16_t, but 32-bit
  // shifting is faster by 1--2 clocks.
  size_t index =
      ((key_hash >> 32) & (StaticConfig::kRecentKeyHashBuckets - 1)) *
      StaticConfig::kRecentKeyHashAssociativity;
  uint16_t tag = static_cast<uint16_t>(key_hash);

  if (StaticConfig::kRecentKeyHashAssociativity == 1) {
    return recent_key_hashes_[lcore_id].v[index] == tag;
  } else {
    for (size_t i = 0; i < StaticConfig::kRecentKeyHashAssociativity; i++)
      if (recent_key_hashes_[lcore_id].v[index + i] == tag) return true;
    return false;
  }
}

template <class StaticConfig>
void Partitions<StaticConfig>::update_recent_key_hash(uint16_t lcore_id,
                                                      uint64_t key_hash) {
  size_t index =
      ((key_hash >> 32) & (StaticConfig::kRecentKeyHashBuckets - 1)) *
      StaticConfig::kRecentKeyHashAssociativity;
  uint16_t tag = static_cast<uint16_t>(key_hash);

  if (recent_key_hashes_[lcore_id].v[index] == tag) return;

  if (StaticConfig::kRecentKeyHashAssociativity > 1) {
    size_t i;
    for (i = 1; i < StaticConfig::kRecentKeyHashAssociativity - 1; i++)
      if (recent_key_hashes_[lcore_id].v[index + i] == tag) break;

    // Shift key hashes to the right by 1 slot.
    for (size_t j = i; j > 0; j--)
      recent_key_hashes_[lcore_id].v[index + j] =
          recent_key_hashes_[lcore_id].v[index + j - 1];
  }

  // Put the new entry.
  recent_key_hashes_[lcore_id].v[index] = tag;
}
}
}

#endif

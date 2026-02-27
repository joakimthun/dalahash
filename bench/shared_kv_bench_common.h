#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace kvbench {

struct XorShift64 {
    uint64_t state;

    explicit XorShift64(uint64_t seed) : state(seed ? seed : 0x9e3779b97f4a7c15ull) {}

    uint64_t next() {
        uint64_t x = state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state = x;
        return x;
    }
};

inline uint64_t mix_seed(uint64_t seed, uint64_t value) {
    uint64_t x = seed ^ (value + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2));
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

inline char random_ascii(XorShift64 *rng) {
    static constexpr char kAlphabet[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789"
        "-_";
    return kAlphabet[rng->next() % (sizeof(kAlphabet) - 1u)];
}

inline std::vector<std::string> make_corpus(uint32_t count, uint32_t item_size,
                                            uint64_t seed, bool inject_index_prefix) {
    std::vector<std::string> out;
    out.reserve(count);
    XorShift64 rng(seed);

    for (uint32_t i = 0; i < count; i++) {
        std::string s(item_size, '\0');
        for (uint32_t j = 0; j < item_size; j++) {
            s[j] = random_ascii(&rng);
        }

        if (inject_index_prefix && item_size > 0) {
            uint32_t n = static_cast<uint32_t>(std::min<size_t>(item_size, sizeof(uint64_t)));
            uint64_t tag = mix_seed(seed, i);
            for (uint32_t b = 0; b < n; b++) {
                s[b] = static_cast<char>((tag >> (b * 8u)) & 0xffu);
            }
        }

        out.push_back(std::move(s));
    }

    return out;
}

inline uint64_t derive_capacity_bytes(uint32_t dataset_size, uint32_t key_size,
                                      uint32_t value_size) {
    static constexpr uint64_t kMinCapacity = 64ull << 20; // 64 MiB
    const uint64_t per_item = static_cast<uint64_t>(key_size) +
                              static_cast<uint64_t>(value_size) + 128ull;

    uint64_t estimate = 0;
    if (dataset_size > 0 && per_item <= std::numeric_limits<uint64_t>::max() / dataset_size) {
        estimate = static_cast<uint64_t>(dataset_size) * per_item;
    } else {
        estimate = std::numeric_limits<uint64_t>::max();
    }

    return estimate > kMinCapacity ? estimate : kMinCapacity;
}

} // namespace kvbench

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace kvbench {

// Shared helpers for benchmark-only data generation and sizing.
//
// Why this file exists:
// - Keep corpus generation identical across single-thread and multi-thread benches.
// - Keep benchmark setup deterministic so results are comparable across runs.
// - Centralize capacity estimation so large argument sets can be added consistently.
//
// Why everything is header-inline:
// - These are tiny helpers used by multiple benchmark translation units.
// - Inline avoids creating another library target just for bench utilities.
// - Deterministic behavior stays local and obvious to benchmark authors.

// Small deterministic PRNG for synthetic benchmark data.
//
// Why xorshift64:
// - Very cheap integer operations.
// - Good enough statistical quality for benchmark key/value generation.
// - Stable and portable bit-exact sequence for a given seed.
//
// Important:
// - This is not cryptographically secure.
// - We care about reproducibility and speed, not unpredictability.
struct XorShift64 {
    uint64_t state;

    // Zero seed is remapped to a fixed non-zero constant because xorshift
    // would otherwise stay at zero forever.
    explicit XorShift64(uint64_t seed) : state(seed ? seed : 0x9e3779b97f4a7c15ull) {}

    // Returns next pseudo-random 64-bit value and updates internal state.
    // The shift constants are the classic xorshift64 triplet.
    uint64_t next() {
        uint64_t x = state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state = x;
        return x;
    }
};

// Mix one value into an existing seed using an avalanche-style hash finalizer.
//
// Why this exists:
// - Different benchmark dimensions (dataset size, worker id, index) need derived
//   seeds that do not produce trivially correlated streams.
// - We want deterministic but well-diffused seed derivation.
//
// Behavior:
// - Pure function: same inputs always produce the same output.
// - Fast enough to use per benchmark setup and per-item tagging.
inline uint64_t mix_seed(uint64_t seed, uint64_t value) {
    uint64_t x = seed ^ (value + 0x9e3779b97f4a7c15ull + (seed << 6) + (seed >> 2));
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

// Draw one printable byte from a fixed alphabet.
//
// Why printable ASCII:
// - Easier debugging when dumping keys/values.
// - Avoids control-byte surprises in tooling and logs.
// - Still gives sufficient entropy for benchmark corpus generation.
inline char random_ascii(XorShift64* rng) {
    static constexpr char kAlphabet[] = "abcdefghijklmnopqrstuvwxyz"
                                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                        "0123456789"
                                        "-_";
    return kAlphabet[rng->next() % (sizeof(kAlphabet) - 1u)];
}

// Build a deterministic corpus of fixed-size strings.
//
// Parameters:
// - count: number of items to generate.
// - item_size: byte length of each item.
// - seed: base RNG seed controlling deterministic output.
// - inject_index_prefix:
//   - true: overwrite first up to 8 bytes with a per-index tag.
//   - false: leave bytes fully RNG-driven.
//
// Why `inject_index_prefix` exists:
// - For keys, forcing an index-derived prefix strongly reduces accidental
//   collisions and pathological duplicates.
// - This stabilizes lookup-hit benchmarks by making key identity more explicit.
// - For values, this is usually unnecessary, so callers can disable it.
//
// Determinism contract:
// - Identical parameters always produce byte-identical corpus content.
// - That makes benchmark comparisons repeatable across machines/runs.
inline std::vector<std::string> make_corpus(uint32_t count, uint32_t item_size, uint64_t seed,
                                            bool inject_index_prefix) {
    std::vector<std::string> out;
    // Reserve once so generation cost is not polluted by repeated vector growth.
    out.reserve(count);
    XorShift64 rng(seed);

    for (uint32_t i = 0; i < count; i++) {
        // Pre-size string and fill in-place to avoid append-time branching.
        std::string s(item_size, '\0');
        for (uint32_t j = 0; j < item_size; j++) {
            s[j] = random_ascii(&rng);
        }

        if (inject_index_prefix && item_size > 0) {
            // Tag up to first 8 bytes with a deterministic index-derived value.
            // Using min(item_size, 8) keeps short strings valid.
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

// Estimate store capacity for benchmark setup.
//
// Goal:
// - Provide enough capacity so preload-heavy benchmarks (especially GET-hit
//   workloads) retain the intended dataset instead of triggering eviction during
//   setup.
//
// Method:
// 1. Estimate raw bytes per item as key + value + approximate node metadata.
// 2. Round up to the store's allocation class size model, because actual memory
//    usage is class-sized, not exact raw payload-sized.
// 3. Multiply by dataset size.
// 4. Add 12.5% headroom to absorb table/control overhead and admission variance.
// 5. Enforce a minimum capacity floor for tiny benchmark cases.
//
// Overflow policy:
// - If any multiplication/addition would overflow uint64_t, saturate to max.
// - Saturation keeps behavior defined and avoids silent wraparound.
//
// Why benchmark-local sizing (instead of hardcoded constant):
// - Argument matrices vary from tiny to multi-million entries.
// - Dynamic sizing keeps each case comparable while avoiding universal over-allocation.
inline uint64_t derive_capacity_bytes(uint32_t dataset_size, uint32_t key_size, uint32_t value_size) {
    static constexpr uint64_t kMinCapacity = 64ull << 20; // 64 MiB
    // Approximate fixed node overhead used by shared_kv_store node layout.
    static constexpr uint32_t kNodeHeaderEstimate = 64u;
    // Mirrors allocator class geometry used by the store implementation.
    // Capacity must account for class rounding to avoid underestimation.
    static constexpr uint32_t kClassSizes[] = {64u,   128u,  256u,  512u,   1024u,
                                               2048u, 4096u, 8192u, 16384u, 32768u};

    // Baseline per-item footprint before class rounding.
    const uint64_t raw_item_size = static_cast<uint64_t>(key_size) + static_cast<uint64_t>(value_size) +
                                   static_cast<uint64_t>(kNodeHeaderEstimate);

    // Round up to nearest supported class size.
    uint64_t per_item = raw_item_size;
    for (uint32_t class_size : kClassSizes) {
        if (raw_item_size <= static_cast<uint64_t>(class_size)) {
            per_item = static_cast<uint64_t>(class_size);
            break;
        }
    }

    uint64_t estimate = 0;
    if (dataset_size > 0 && per_item <= std::numeric_limits<uint64_t>::max() / dataset_size) {
        estimate = static_cast<uint64_t>(dataset_size) * per_item;
        // Add 12.5% headroom to reduce preload-time eviction risk in hit-path benches.
        const uint64_t headroom = estimate / 8u;
        if (headroom <= std::numeric_limits<uint64_t>::max() - estimate) {
            estimate += headroom;
        } else {
            estimate = std::numeric_limits<uint64_t>::max();
        }
    } else {
        // Saturate on overflow risk or degenerate multiplication cases.
        estimate = std::numeric_limits<uint64_t>::max();
    }

    // Keep small benchmark cases from using unrealistically tiny capacities.
    return estimate > kMinCapacity ? estimate : kMinCapacity;
}

} // namespace kvbench

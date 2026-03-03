#include "memcached/memcached_command.h"
#include "memcached/memcached_parse.h"
#include "memcached/memcached_response.h"
#include "store/store.h"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

static constexpr uint8_t LEGACY_KEY_SEED = 7;
static constexpr uint8_t LEGACY_VALUE_SEED = 11;
static constexpr uint8_t META_KEY_SEED = 17;
static constexpr uint8_t META_VALUE_SEED = 19;

static std::string make_ascii_payload(uint32_t len, uint8_t seed) {
    std::string out;
    out.resize(len);
    for (uint32_t i = 0; i < len; i++)
        out[i] = static_cast<char>('a' + ((i + seed) % 26u));
    return out;
}

static std::string make_legacy_get_command(uint32_t key_len) {
    const std::string key = make_ascii_payload(key_len, LEGACY_KEY_SEED);
    std::string cmd;
    cmd.reserve(16u + key.size());
    cmd.append("get ");
    cmd.append(key);
    cmd.append("\r\n");
    return cmd;
}

static std::string make_legacy_set_command(uint32_t key_len, uint32_t value_len) {
    const std::string key = make_ascii_payload(key_len, LEGACY_KEY_SEED);
    const std::string value = make_ascii_payload(value_len, LEGACY_VALUE_SEED);
    std::string cmd;
    cmd.reserve(48u + key.size() + value.size());
    cmd.append("set ");
    cmd.append(key);
    cmd.append(" 0 0 ");
    cmd.append(std::to_string(value_len));
    cmd.append("\r\n");
    cmd.append(value);
    cmd.append("\r\n");
    return cmd;
}

static std::string make_meta_get_command(uint32_t key_len) {
    const std::string key = make_ascii_payload(key_len, META_KEY_SEED);
    std::string cmd;
    cmd.reserve(24u + key.size());
    cmd.append("mg ");
    cmd.append(key);
    cmd.append(" v f s\r\n");
    return cmd;
}

static std::string make_meta_set_command(uint32_t key_len, uint32_t value_len) {
    const std::string key = make_ascii_payload(key_len, META_KEY_SEED);
    const std::string value = make_ascii_payload(value_len, META_VALUE_SEED);
    std::string cmd;
    cmd.reserve(48u + key.size() + value.size());
    cmd.append("ms ");
    cmd.append(key);
    cmd.push_back(' ');
    cmd.append(std::to_string(value_len));
    cmd.append(" F42\r\n");
    cmd.append(value);
    cmd.append("\r\n");
    return cmd;
}

static bool parse_command(const std::string& input, McCommand* cmd, uint32_t* consumed) {
    if (!cmd || !consumed)
        return false;
    *consumed = 0;
    return mc_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()), cmd,
                    consumed) == McParseResult::OK &&
           *consumed == static_cast<uint32_t>(input.size());
}

// Legacy GET miss returns "END\r\n", meta GET miss returns "EN\r\n".
// Detect these to distinguish misses from hits in benchmarks.
static bool is_miss_response(const uint8_t* data, uint32_t len) {
    if (len == 5 && std::memcmp(data, "END\r\n", 5) == 0)
        return true;
    if (len == 4 && std::memcmp(data, "EN\r\n", 4) == 0)
        return true;
    return false;
}

static void add_key_args(benchmark::internal::Benchmark* b) {
    b->Arg(8);
    b->Arg(16);
    b->Arg(64);
    b->Arg(256);
}

static void add_key_value_args(benchmark::internal::Benchmark* b) {
    b->Args({8, 16});
    b->Args({16, 64});
    b->Args({32, 256});
    b->Args({64, 1024});
    b->Args({64, 4096});
}

static void add_value_args(benchmark::internal::Benchmark* b) {
    b->Arg(16);
    b->Arg(64);
    b->Arg(256);
    b->Arg(1024);
    b->Arg(4096);
}

static void BM_McParseLegacyGet(benchmark::State& state) {
    if (state.range(0) <= 0) {
        state.SkipWithError("invalid key length");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const std::string cmd_bytes = make_legacy_get_command(key_len);

    McCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        if (!parse_command(cmd_bytes, &cmd, &consumed)) {
            state.SkipWithError("mc_parse failed for legacy get");
            break;
        }
        benchmark::DoNotOptimize(cmd.key.data);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(cmd_bytes.size()));
}

static void BM_McParseLegacySet(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid set lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string cmd_bytes = make_legacy_set_command(key_len, value_len);

    McCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        if (!parse_command(cmd_bytes, &cmd, &consumed)) {
            state.SkipWithError("mc_parse failed for legacy set");
            break;
        }
        benchmark::DoNotOptimize(cmd.value.data);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(cmd_bytes.size()));
}

static void BM_McParseMetaGet(benchmark::State& state) {
    if (state.range(0) <= 0) {
        state.SkipWithError("invalid key length");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const std::string cmd_bytes = make_meta_get_command(key_len);

    McCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        if (!parse_command(cmd_bytes, &cmd, &consumed)) {
            state.SkipWithError("mc_parse failed for meta get");
            break;
        }
        benchmark::DoNotOptimize(cmd.meta_flag_count);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(cmd_bytes.size()));
}

static void BM_McParseMetaSet(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid meta set lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string cmd_bytes = make_meta_set_command(key_len, value_len);

    McCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        if (!parse_command(cmd_bytes, &cmd, &consumed)) {
            state.SkipWithError("mc_parse failed for meta set");
            break;
        }
        benchmark::DoNotOptimize(cmd.value.data);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(cmd_bytes.size()));
}

static void BM_McCommandLegacyGetHit(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid command lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string set_cmd_bytes = make_legacy_set_command(key_len, value_len);
    const std::string get_cmd_bytes = make_legacy_get_command(key_len);

    McCommand set_cmd = {};
    McCommand get_cmd = {};
    uint32_t consumed = 0;
    if (!parse_command(set_cmd_bytes, &set_cmd, &consumed) ||
        !parse_command(get_cmd_bytes, &get_cmd, &consumed)) {
        state.SkipWithError("command parse failed");
        return;
    }

    Store store;
    std::vector<uint8_t> out(value_len + key_len + 128u, 0);
    if (mc_command_execute(&set_cmd, &store, 1, out.data(), static_cast<uint32_t>(out.size())) == 0) {
        state.SkipWithError("preload set failed");
        return;
    }

    uint64_t now_ms = 2;
    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written =
            mc_command_execute(&get_cmd, &store, now_ms++, out.data(), static_cast<uint32_t>(out.size()));
        if (written == 0 || is_miss_response(out.data(), written)) {
            state.SkipWithError("legacy get missed (expected hit)");
            break;
        }
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(written));
}

static void BM_McCommandLegacySet(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid command lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string set_cmd_bytes = make_legacy_set_command(key_len, value_len);

    McCommand set_cmd = {};
    uint32_t consumed = 0;
    if (!parse_command(set_cmd_bytes, &set_cmd, &consumed)) {
        state.SkipWithError("command parse failed");
        return;
    }

    Store store;
    std::vector<uint8_t> out(32u, 0);

    uint64_t now_ms = 1;
    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written =
            mc_command_execute(&set_cmd, &store, now_ms++, out.data(), static_cast<uint32_t>(out.size()));
        if (written == 0) {
            state.SkipWithError("legacy set execute failed");
            break;
        }
        benchmark::DoNotOptimize(written);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(set_cmd.key.len + set_cmd.value.len));
}

static void BM_McCommandMetaGetValue(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid command lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string set_cmd_bytes = make_meta_set_command(key_len, value_len);
    const std::string get_cmd_bytes = make_meta_get_command(key_len);

    McCommand set_cmd = {};
    McCommand get_cmd = {};
    uint32_t consumed = 0;
    if (!parse_command(set_cmd_bytes, &set_cmd, &consumed) ||
        !parse_command(get_cmd_bytes, &get_cmd, &consumed)) {
        state.SkipWithError("command parse failed");
        return;
    }

    Store store;
    std::vector<uint8_t> out(value_len + key_len + 128u, 0);
    if (mc_command_execute(&set_cmd, &store, 1, out.data(), static_cast<uint32_t>(out.size())) == 0) {
        state.SkipWithError("preload meta set failed");
        return;
    }

    uint64_t now_ms = 2;
    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written =
            mc_command_execute(&get_cmd, &store, now_ms++, out.data(), static_cast<uint32_t>(out.size()));
        if (written == 0 || is_miss_response(out.data(), written)) {
            state.SkipWithError("meta get missed (expected hit)");
            break;
        }
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(written));
}

static void BM_McCommandMetaSet(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid command lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string set_cmd_bytes = make_meta_set_command(key_len, value_len);

    McCommand set_cmd = {};
    uint32_t consumed = 0;
    if (!parse_command(set_cmd_bytes, &set_cmd, &consumed)) {
        state.SkipWithError("command parse failed");
        return;
    }

    Store store;
    std::vector<uint8_t> out(32u, 0);

    uint64_t now_ms = 1;
    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written =
            mc_command_execute(&set_cmd, &store, now_ms++, out.data(), static_cast<uint32_t>(out.size()));
        if (written == 0) {
            state.SkipWithError("meta set execute failed");
            break;
        }
        benchmark::DoNotOptimize(written);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(set_cmd.key.len + set_cmd.value.len));
}

static void BM_McWriteValue(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0) {
        state.SkipWithError("invalid response lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string key = make_ascii_payload(key_len, 23);
    const std::string value = make_ascii_payload(value_len, 29);
    std::vector<uint8_t> out(key_len + value_len + 128u, 0);

    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written = mc_write_value(
            out.data(), reinterpret_cast<const uint8_t*>(key.data()), static_cast<uint32_t>(key.size()), 42,
            reinterpret_cast<const uint8_t*>(value.data()), static_cast<uint32_t>(value.size()));
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(written));
}

static void BM_McWriteVa(benchmark::State& state) {
    if (state.range(0) < 0) {
        state.SkipWithError("invalid value length");
        return;
    }
    const uint32_t value_len = static_cast<uint32_t>(state.range(0));
    const std::string value = make_ascii_payload(value_len, 31);
    std::vector<uint8_t> out(value_len + 128u, 0);

    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written = mc_write_va(out.data(), reinterpret_cast<const uint8_t*>(value.data()),
                              static_cast<uint32_t>(value.size()), "f42 s64");
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(written));
}

static void BM_McWriteHd(benchmark::State& state) {
    uint8_t out[8] = {};

    for (auto _ : state) {
        (void)_;
        uint32_t written = mc_write_hd(out);
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * 4);
}

BENCHMARK(BM_McParseLegacyGet)->Apply(add_key_args);
BENCHMARK(BM_McParseLegacySet)->Apply(add_key_value_args);
BENCHMARK(BM_McParseMetaGet)->Apply(add_key_args);
BENCHMARK(BM_McParseMetaSet)->Apply(add_key_value_args);
BENCHMARK(BM_McCommandLegacyGetHit)->Apply(add_key_value_args);
BENCHMARK(BM_McCommandLegacySet)->Apply(add_key_value_args);
BENCHMARK(BM_McCommandMetaGetValue)->Apply(add_key_value_args);
BENCHMARK(BM_McCommandMetaSet)->Apply(add_key_value_args);
BENCHMARK(BM_McWriteValue)->Apply(add_key_value_args);
BENCHMARK(BM_McWriteVa)->Apply(add_value_args);
BENCHMARK(BM_McWriteHd);

} // namespace

BENCHMARK_MAIN();

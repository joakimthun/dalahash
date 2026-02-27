#include "redis/resp.h"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

static std::string make_ascii_payload(uint32_t len, uint8_t seed) {
    std::string out;
    out.resize(len);
    for (uint32_t i = 0; i < len; i++) {
        out[i] = static_cast<char>('a' + ((i + seed) % 26u));
    }
    return out;
}

static void append_bulk(std::string* out, const char* data, uint32_t len) {
    if (!out)
        return;
    out->push_back('$');
    out->append(std::to_string(len));
    out->append("\r\n");
    if (len > 0)
        out->append(data, len);
    out->append("\r\n");
}

static std::string make_get_command(uint32_t key_len) {
    const std::string key = make_ascii_payload(key_len, 3);
    std::string cmd;
    cmd.reserve(32u + key.size());
    cmd.append("*2\r\n");
    append_bulk(&cmd, "GET", 3);
    append_bulk(&cmd, key.data(), static_cast<uint32_t>(key.size()));
    return cmd;
}

static std::string make_set_command(uint32_t key_len, uint32_t value_len) {
    const std::string key = make_ascii_payload(key_len, 7);
    const std::string value = make_ascii_payload(value_len, 11);
    std::string cmd;
    cmd.reserve(48u + key.size() + value.size());
    cmd.append("*3\r\n");
    append_bulk(&cmd, "SET", 3);
    append_bulk(&cmd, key.data(), static_cast<uint32_t>(key.size()));
    append_bulk(&cmd, value.data(), static_cast<uint32_t>(value.size()));
    return cmd;
}

static void add_get_args(benchmark::internal::Benchmark* b) {
    b->Arg(3);
    b->Arg(16);
    b->Arg(64);
    b->Arg(256);
    b->Arg(1024);
}

static void add_set_args(benchmark::internal::Benchmark* b) {
    b->Args({3, 3});
    b->Args({16, 64});
    b->Args({32, 256});
    b->Args({64, 1024});
    b->Args({64, 4096});
}

static void add_write_bulk_args(benchmark::internal::Benchmark* b) {
    b->Arg(0);
    b->Arg(16);
    b->Arg(64);
    b->Arg(256);
    b->Arg(1024);
    b->Arg(4096);
    b->Arg(16384);
}

static void add_pipeline_args(benchmark::internal::Benchmark* b) {
    b->Args({2, 16, 64});
    b->Args({4, 16, 64});
    b->Args({8, 16, 64});
    b->Args({4, 32, 256});
    b->Args({8, 64, 1024});
}

static void add_mixed_pipeline_args(benchmark::internal::Benchmark* b) {
    b->Args({4, 16, 64});
    b->Args({8, 16, 64});
    b->Args({8, 32, 256});
    b->Args({16, 32, 256});
    b->Args({16, 128, 256});
    b->Args({16, 64, 1024});
}

static void BM_RespParseGet(benchmark::State& state) {
    if (state.range(0) < 0) {
        state.SkipWithError("invalid key length");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const std::string cmd_bytes = make_get_command(key_len);
    const uint8_t* input = reinterpret_cast<const uint8_t*>(cmd_bytes.data());
    const uint32_t input_len = static_cast<uint32_t>(cmd_bytes.size());

    RespCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        const RespParseResult result = resp_parse(input, input_len, &cmd, &consumed);
        if (result != RespParseResult::OK || consumed != input_len) {
            state.SkipWithError("resp_parse failed for GET");
            break;
        }
        benchmark::DoNotOptimize(cmd.argc);
        benchmark::DoNotOptimize(cmd.args[1].data);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(input_len));
}

static void BM_RespParseSet(benchmark::State& state) {
    if (state.range(0) < 0 || state.range(1) < 0) {
        state.SkipWithError("invalid SET lengths");
        return;
    }
    const uint32_t key_len = static_cast<uint32_t>(state.range(0));
    const uint32_t value_len = static_cast<uint32_t>(state.range(1));
    const std::string cmd_bytes = make_set_command(key_len, value_len);
    const uint8_t* input = reinterpret_cast<const uint8_t*>(cmd_bytes.data());
    const uint32_t input_len = static_cast<uint32_t>(cmd_bytes.size());

    RespCommand cmd = {};
    uint32_t consumed = 0;
    for (auto _ : state) {
        (void)_;
        const RespParseResult result = resp_parse(input, input_len, &cmd, &consumed);
        if (result != RespParseResult::OK || consumed != input_len) {
            state.SkipWithError("resp_parse failed for SET");
            break;
        }
        benchmark::DoNotOptimize(cmd.argc);
        benchmark::DoNotOptimize(cmd.args[2].data);
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(input_len));
}

static void BM_RespWriteBulk(benchmark::State& state) {
    if (state.range(0) < 0) {
        state.SkipWithError("invalid payload length");
        return;
    }
    const uint32_t payload_len = static_cast<uint32_t>(state.range(0));
    const std::string payload = make_ascii_payload(payload_len, 19);
    std::vector<uint8_t> out(payload_len + 32u, 0);
    const uint8_t* data = payload_len == 0 ? nullptr : reinterpret_cast<const uint8_t*>(payload.data());

    uint32_t written = 0;
    for (auto _ : state) {
        (void)_;
        written = resp_write_bulk(out.data(), data, payload_len);
        if (written == 0) {
            state.SkipWithError("resp_write_bulk returned zero");
            break;
        }
        benchmark::DoNotOptimize(written);
        benchmark::DoNotOptimize(out.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(written));
}

static void BM_RespParsePipeline(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0 || state.range(2) < 0) {
        state.SkipWithError("invalid pipeline args");
        return;
    }
    const uint32_t command_count = static_cast<uint32_t>(state.range(0));
    const uint32_t key_len = static_cast<uint32_t>(state.range(1));
    const uint32_t value_len = static_cast<uint32_t>(state.range(2));

    std::string pipeline;
    for (uint32_t i = 0; i < command_count; i++) {
        pipeline += make_set_command(key_len, value_len);
    }

    const uint8_t* input = reinterpret_cast<const uint8_t*>(pipeline.data());
    const uint32_t input_len = static_cast<uint32_t>(pipeline.size());

    RespCommand cmd = {};
    uint32_t command_bytes = 0;
    for (auto _ : state) {
        (void)_;
        uint32_t offset = 0;
        for (uint32_t i = 0; i < command_count; i++) {
            const RespParseResult result =
                resp_parse(input + offset, input_len - offset, &cmd, &command_bytes);
            if (result != RespParseResult::OK || command_bytes == 0) {
                state.SkipWithError("resp_parse failed for pipelined input");
                break;
            }
            offset += command_bytes;
            benchmark::DoNotOptimize(cmd.argc);
            benchmark::DoNotOptimize(cmd.args[2].data);
            benchmark::DoNotOptimize(command_bytes);
        }
        if (state.skipped())
            break;
        if (offset != input_len) {
            state.SkipWithError("pipeline parse did not consume full input");
            break;
        }
        benchmark::DoNotOptimize(offset);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * static_cast<int64_t>(command_count)));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(input_len));
}

static void BM_RespParseMixedPipeline(benchmark::State& state) {
    if (state.range(0) <= 0 || state.range(1) < 0 || state.range(2) < 0) {
        state.SkipWithError("invalid mixed pipeline args");
        return;
    }
    const uint32_t command_count = static_cast<uint32_t>(state.range(0));
    const uint32_t key_len = static_cast<uint32_t>(state.range(1));
    const uint32_t value_len = static_cast<uint32_t>(state.range(2));
    const uint32_t gets_per_iteration = (command_count + 1u) / 2u;
    const uint32_t sets_per_iteration = command_count / 2u;

    std::vector<std::string> commands;
    commands.reserve(command_count);

    std::string pipeline;
    for (uint32_t i = 0; i < command_count; i++) {
        if ((i & 1u) == 0u) {
            commands.push_back(make_get_command(key_len));
        } else {
            commands.push_back(make_set_command(key_len, value_len));
        }
        pipeline += commands.back();
    }

    const uint8_t* input = reinterpret_cast<const uint8_t*>(pipeline.data());
    const uint32_t input_len = static_cast<uint32_t>(pipeline.size());

    RespCommand cmd = {};
    uint32_t command_bytes = 0;
    for (auto _ : state) {
        (void)_;
        uint32_t offset = 0;
        for (uint32_t i = 0; i < command_count; i++) {
            const RespParseResult result =
                resp_parse(input + offset, input_len - offset, &cmd, &command_bytes);
            if (result != RespParseResult::OK || command_bytes == 0) {
                state.SkipWithError("resp_parse failed for mixed pipelined input");
                break;
            }
            offset += command_bytes;
            const int arg_idx = cmd.argc == 3 ? 2 : 1;
            benchmark::DoNotOptimize(cmd.argc);
            benchmark::DoNotOptimize(cmd.args[0].data);
            benchmark::DoNotOptimize(cmd.args[arg_idx].data);
            benchmark::DoNotOptimize(command_bytes);
        }
        if (state.skipped())
            break;
        if (offset != input_len) {
            state.SkipWithError("mixed pipeline parse did not consume full input");
            break;
        }
        benchmark::DoNotOptimize(offset);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * static_cast<int64_t>(command_count)));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(input_len));
    state.counters["get_ops"] =
        benchmark::Counter(static_cast<double>(state.iterations()) * static_cast<double>(gets_per_iteration),
                           benchmark::Counter::kIsRate);
    state.counters["set_ops"] =
        benchmark::Counter(static_cast<double>(state.iterations()) * static_cast<double>(sets_per_iteration),
                           benchmark::Counter::kIsRate);
}

BENCHMARK(BM_RespParseGet)->Apply(add_get_args)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RespParseSet)->Apply(add_set_args)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RespWriteBulk)->Apply(add_write_bulk_args)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RespParsePipeline)->Apply(add_pipeline_args)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RespParseMixedPipeline)->Apply(add_mixed_pipeline_args)->Unit(benchmark::kNanosecond);

} // namespace

BENCHMARK_MAIN();

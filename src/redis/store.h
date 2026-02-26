// store.h — Redis-facing wrapper around shared kv store.

#pragma once

#include "kv/shared_kv_store.h"

#include <string_view>

enum class StoreSetStatus : uint8_t {
    OK = 0,
    OOM = 1,
    INVALID = 2,
};

struct StoreValueView {
    const uint8_t *data;
    uint32_t len;
};

struct Store {
    KvStore *kv = nullptr;
    uint32_t worker_id = 0;
    bool owns_kv = false;

    Store() = default;

    Store(const Store &) = delete;
    Store &operator=(const Store &) = delete;

    Store(Store &&other) noexcept {
        kv = other.kv;
        worker_id = other.worker_id;
        owns_kv = other.owns_kv;
        other.kv = nullptr;
        other.worker_id = 0;
        other.owns_kv = false;
    }

    Store &operator=(Store &&other) noexcept {
        if (this == &other) return *this;
        if (owns_kv && kv) kv_store_destroy(kv);
        kv = other.kv;
        worker_id = other.worker_id;
        owns_kv = other.owns_kv;
        other.kv = nullptr;
        other.worker_id = 0;
        other.owns_kv = false;
        return *this;
    }

    ~Store() {
        if (owns_kv && kv) kv_store_destroy(kv);
        kv = nullptr;
        worker_id = 0;
        owns_kv = false;
    }
};

inline bool store_ensure_local(Store *s) {
    if (!s) return false;
    if (s->kv) return true;

    KvStoreConfig cfg = {
        .capacity_bytes = 8ull << 20, // tests/local fallback
        .shard_count = 16,
        .buckets_per_shard = 0,
        .worker_count = 1,
    };

    KvStore *kv = kv_store_create(&cfg);
    if (!kv) return false;
    kv_store_register_worker(kv, 0);

    s->kv = kv;
    s->worker_id = 0;
    s->owns_kv = true;
    return true;
}

inline void store_reset(Store *s) {
    if (!s) return;
    if (s->owns_kv && s->kv) kv_store_destroy(s->kv);
    s->kv = nullptr;
    s->worker_id = 0;
    s->owns_kv = false;
}

inline void store_bind_shared(Store *s, KvStore *kv, uint32_t worker_id) {
    if (!s) return;
    if (s->owns_kv && s->kv) kv_store_destroy(s->kv);
    s->kv = kv;
    s->worker_id = worker_id;
    s->owns_kv = false;
}

inline void store_quiescent(Store *s) {
    if (!s || !s->kv) return;
    kv_store_quiescent(s->kv, s->worker_id);
}

inline bool store_get_at(Store *s, std::string_view key, uint64_t now_ms, StoreValueView *out) {
    if (!out) return false;
    out->data = nullptr;
    out->len = 0;
    if (!store_ensure_local(s)) return false;

    KvValueView view = {};
    KvGetStatus st = kv_store_get(s->kv, s->worker_id, key, now_ms, &view);
    if (st != KvGetStatus::HIT) return false;
    out->data = view.data;
    out->len = view.len;
    return true;
}

inline bool store_get(Store *s, std::string_view key, StoreValueView *out) {
    return store_get_at(s, key, kv_time_now_ms(), out);
}

inline StoreSetStatus store_set_at(Store *s, std::string_view key, std::string_view value,
                                   uint64_t now_ms) {
    if (!store_ensure_local(s)) return StoreSetStatus::INVALID;
    KvSetStatus st = kv_store_set(s->kv, s->worker_id, key, value, now_ms, nullptr);
    if (st == KvSetStatus::OK) return StoreSetStatus::OK;
    if (st == KvSetStatus::OOM) return StoreSetStatus::OOM;
    return StoreSetStatus::INVALID;
}

inline StoreSetStatus store_set(Store *s, std::string_view key, std::string_view value) {
    return store_set_at(s, key, value, kv_time_now_ms());
}

inline StoreSetStatus store_set_expire_after_ms_at(Store *s, std::string_view key,
                                                   std::string_view value, uint64_t ttl_ms,
                                                   uint64_t now_ms) {
    if (!store_ensure_local(s)) return StoreSetStatus::INVALID;
    KvSetOptions opts = {.mode = KvExpireMode::AFTER_MS, .value_ms = ttl_ms};
    KvSetStatus st = kv_store_set(s->kv, s->worker_id, key, value, now_ms, &opts);
    if (st == KvSetStatus::OK) return StoreSetStatus::OK;
    if (st == KvSetStatus::OOM) return StoreSetStatus::OOM;
    return StoreSetStatus::INVALID;
}

inline StoreSetStatus store_set_expire_after_ms(Store *s, std::string_view key,
                                                std::string_view value, uint64_t ttl_ms) {
    return store_set_expire_after_ms_at(s, key, value, ttl_ms, kv_time_now_ms());
}

inline StoreSetStatus store_set_expire_at_ms_at(Store *s, std::string_view key,
                                                std::string_view value, uint64_t at_ms,
                                                uint64_t now_ms) {
    if (!store_ensure_local(s)) return StoreSetStatus::INVALID;
    KvSetOptions opts = {.mode = KvExpireMode::AT_MS, .value_ms = at_ms};
    KvSetStatus st = kv_store_set(s->kv, s->worker_id, key, value, now_ms, &opts);
    if (st == KvSetStatus::OK) return StoreSetStatus::OK;
    if (st == KvSetStatus::OOM) return StoreSetStatus::OOM;
    return StoreSetStatus::INVALID;
}

inline StoreSetStatus store_set_expire_at_ms(Store *s, std::string_view key,
                                             std::string_view value, uint64_t at_ms) {
    return store_set_expire_at_ms_at(s, key, value, at_ms, kv_time_now_ms());
}

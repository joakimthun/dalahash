/* store.h — Per-thread key-value store (v1: single-threaded, no locking).
 *
 * Each worker owns its own Store; all access is from one thread, so no
 * synchronisation is needed. v2 will introduce a shared store. */

#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

struct Store {
    std::unordered_map<std::string, std::string> data;
};

/* Returns a pointer directly into the map node (no copy of the value).
 * The pointer is stable until the next store_set on the same key or store
 * destruction. Returns nullptr on miss.
 * Note: std::string(key) allocates on every call; a map with heterogeneous
 * lookup (find(string_view)) would avoid this allocation on the lookup path. */
inline const std::string *store_get(Store *s, std::string_view key) {
    auto it = s->data.find(std::string(key));
    if (it == s->data.end()) return nullptr;
    return &it->second;
}

/* Inserts or overwrites key → value. Both string_views are copied into
 * heap-allocated std::strings owned by the map. */
inline void store_set(Store *s, std::string_view key, std::string_view value) {
    s->data[std::string(key)] = std::string(value);
}

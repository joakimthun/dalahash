// memcached_command.cpp — Memcached command dispatch.
//
// Client flags encoding: stored values are prefixed with a 4-byte big-endian
// flags field. On SET, build [4-byte flags][value]. On GET, first 4 bytes are
// flags, remainder is the actual value.

#include "memcached_command.h"
#include "base/assert.h"
#include "memcached_response.h"

#include <cstring>

static constexpr uint32_t FLAGS_PREFIX_SIZE = 4;
static constexpr uint32_t MIN_ERROR_BUF_SIZE = 7;         // "ERROR\r\n"
static constexpr uint32_t MC_EXPTIME_THRESHOLD = 2592000; // 30 days in seconds

// Write error response if buffer is large enough, otherwise return 0.
static uint32_t safe_write_error(uint8_t* out_buf, uint32_t out_buf_size) {
    if (out_buf_size < MIN_ERROR_BUF_SIZE)
        return 0;
    return mc_write_error(out_buf);
}

// Encode 4-byte big-endian flags prefix.
static void encode_flags(uint8_t* out, uint32_t flags) {
    out[0] = static_cast<uint8_t>((flags >> 24) & 0xFF);
    out[1] = static_cast<uint8_t>((flags >> 16) & 0xFF);
    out[2] = static_cast<uint8_t>((flags >> 8) & 0xFF);
    out[3] = static_cast<uint8_t>(flags & 0xFF);
}

// Decode 4-byte big-endian flags prefix.
static uint32_t decode_flags(const uint8_t* data) {
    return (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) |
           (static_cast<uint32_t>(data[2]) << 8) | static_cast<uint32_t>(data[3]);
}

// Fast decimal formatting into stack buffer. Returns length.
static uint32_t uint_to_str_buf(char* buf, uint32_t val) {
    char tmp[10];
    uint32_t n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + (val % 10));
        val /= 10;
    } while (val > 0);
    for (uint32_t i = 0; i < n; i++)
        buf[i] = tmp[n - 1 - i];
    return n;
}

// Check if a meta flag token starts with the given character.
static bool has_meta_flag(const McCommand* cmd, char flag_char) {
    for (int i = 0; i < cmd->meta_flag_count; i++) {
        if (cmd->meta_flags[i].len > 0 && cmd->meta_flags[i].data[0] == static_cast<uint8_t>(flag_char))
            return true;
    }
    return false;
}

// Validate and extract numeric value from ALL meta flags matching flag_char.
// Checks every token starting with flag_char — fails if any is malformed (bare
// letter, non-numeric digits, overflow). Uses last-wins for the output value.
// Sets *found to true if at least one matching token was present.
// Returns false on validation failure.
static bool meta_flag_validate_u32(const McCommand* cmd, char flag_char, uint32_t* out, bool* found) {
    *found = false;
    for (int i = 0; i < cmd->meta_flag_count; i++) {
        if (cmd->meta_flags[i].len > 0 && cmd->meta_flags[i].data[0] == static_cast<uint8_t>(flag_char)) {
            *found = true;
            if (cmd->meta_flags[i].len < 2)
                return false; // bare flag letter with no digits
            uint64_t val = 0;
            for (uint32_t j = 1; j < cmd->meta_flags[i].len; j++) {
                uint8_t ch = cmd->meta_flags[i].data[j];
                if (ch < '0' || ch > '9')
                    return false;
                val = val * 10 + (ch - '0');
                if (val > UINT32_MAX)
                    return false;
            }
            *out = static_cast<uint32_t>(val);
        }
    }
    return true;
}

static uint32_t handle_legacy_get(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                  uint32_t out_buf_size) {
    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    StoreValueView val = {};
    if (!store_get_at(store, key, now_ms, &val) || val.len < FLAGS_PREFIX_SIZE) {
        // Miss — just END
        if (out_buf_size < 5)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_end(out_buf);
    }

    uint32_t flags = decode_flags(val.data);
    const uint8_t* real_val = val.data + FLAGS_PREFIX_SIZE;
    uint32_t real_len = val.len - FLAGS_PREFIX_SIZE;

    // Estimate: "VALUE " + key + " " + flags(10) + " " + len(10) + "\r\n" + data + "\r\nEND\r\n"
    uint64_t needed = 6ull + cmd->key.len + 1 + 10 + 1 + 10 + 2 + real_len + 2 + 5;
    if (needed > out_buf_size)
        return safe_write_error(out_buf, out_buf_size);

    return mc_write_value(out_buf, cmd->key.data, cmd->key.len, flags, real_val, real_len);
}

static uint32_t handle_legacy_set(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                  uint32_t out_buf_size) {
    // Build stored value: [4-byte flags][value data]
    // Guard against uint32_t overflow from extreme cmd->value.len.
    if (cmd->value.len > UINT32_MAX - FLAGS_PREFIX_SIZE)
        return safe_write_error(out_buf, out_buf_size);
    uint32_t combined_len = FLAGS_PREFIX_SIZE + cmd->value.len;
    // Use stack buffer for reasonable sizes, heap for large.
    uint8_t stack_buf[8192];
    uint8_t* combined = stack_buf;
    bool heap = false;
    if (combined_len > sizeof(stack_buf)) {
        combined = new (std::nothrow) uint8_t[combined_len];
        if (!combined) {
            if (out_buf_size < 12)
                return safe_write_error(out_buf, out_buf_size);
            return mc_write_not_stored(out_buf);
        }
        heap = true;
    }
    encode_flags(combined, cmd->client_flags);
    if (cmd->value.len > 0)
        std::memcpy(combined + FLAGS_PREFIX_SIZE, cmd->value.data, cmd->value.len);

    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    std::string_view value(reinterpret_cast<const char*>(combined), combined_len);

    StoreSetStatus st;
    if (cmd->exptime > MC_EXPTIME_THRESHOLD) {
        // Absolute Unix epoch timestamp.
        uint64_t at_ms = static_cast<uint64_t>(cmd->exptime) * 1000ULL;
        st = store_set_expire_at_ms_at(store, key, value, at_ms, now_ms);
    } else if (cmd->exptime > 0) {
        // Relative TTL in seconds.
        uint64_t ttl_ms = static_cast<uint64_t>(cmd->exptime) * 1000ULL;
        st = store_set_expire_after_ms_at(store, key, value, ttl_ms, now_ms);
    } else {
        st = store_set_at(store, key, value, now_ms);
    }

    if (heap)
        delete[] combined;

    if (st != StoreSetStatus::OK) {
        if (out_buf_size < 12)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_not_stored(out_buf);
    }

    if (cmd->noreply)
        return 0;
    if (out_buf_size < 8)
        return safe_write_error(out_buf, out_buf_size);
    return mc_write_stored(out_buf);
}

static uint32_t handle_legacy_delete(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                     uint32_t out_buf_size) {
    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    StoreDeleteStatus st = store_delete_at(store, key, now_ms);

    if (cmd->noreply)
        return 0;

    if (st == StoreDeleteStatus::OK) {
        if (out_buf_size < 9)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_deleted(out_buf);
    }
    if (out_buf_size < 11)
        return safe_write_error(out_buf, out_buf_size);
    return mc_write_not_found(out_buf);
}

static uint32_t handle_meta_get(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                uint32_t out_buf_size) {
    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    StoreValueView val = {};
    bool hit = store_get_at(store, key, now_ms, &val) && val.len >= FLAGS_PREFIX_SIZE;

    if (!hit) {
        if (out_buf_size < 4)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_en(out_buf);
    }

    uint32_t flags = decode_flags(val.data);
    const uint8_t* real_val = val.data + FLAGS_PREFIX_SIZE;
    uint32_t real_len = val.len - FLAGS_PREFIX_SIZE;

    // Build metadata flags string (shared by VA and HD responses).
    char extra[128];
    uint32_t ep = 0;

    if (has_meta_flag(cmd, 'f')) {
        extra[ep++] = 'f';
        ep += uint_to_str_buf(extra + ep, flags);
    }
    if (has_meta_flag(cmd, 's')) {
        if (ep > 0)
            extra[ep++] = ' ';
        extra[ep++] = 's';
        ep += uint_to_str_buf(extra + ep, real_len);
    }
    if (has_meta_flag(cmd, 'k')) {
        // Only emit k token if the full key fits in the scratch buffer.
        uint32_t k_needed = 1 + cmd->key.len; // 'k' + key bytes
        if (ep + (ep > 0 ? 1 : 0) + k_needed < sizeof(extra)) {
            if (ep > 0)
                extra[ep++] = ' ';
            extra[ep++] = 'k';
            if (cmd->key.len > 0) {
                std::memcpy(extra + ep, cmd->key.data, cmd->key.len);
                ep += cmd->key.len;
            }
        }
    }
    extra[ep] = '\0';

    if (has_meta_flag(cmd, 'v')) {
        uint64_t needed = 3ull + 10 + 1 + ep + 2 + real_len + 2;
        if (needed > out_buf_size)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_va(out_buf, real_val, real_len, extra);
    }

    // No value requested — respond with HD + any metadata flags.
    uint64_t needed = 2ull + (ep > 0 ? 1 + ep : 0) + 2;
    if (needed > out_buf_size)
        return safe_write_error(out_buf, out_buf_size);
    if (ep > 0)
        return mc_write_hd_flags(out_buf, extra);
    return mc_write_hd(out_buf);
}

static uint32_t handle_meta_set(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                uint32_t out_buf_size) {
    // Extract optional F (flags) and T (TTL) from meta flags.
    // Validates every matching token — rejects if any is malformed.
    uint32_t flags = 0;
    bool has_f = false;
    if (!meta_flag_validate_u32(cmd, 'F', &flags, &has_f))
        return safe_write_error(out_buf, out_buf_size);
    uint32_t ttl = 0;
    bool has_ttl = false;
    if (!meta_flag_validate_u32(cmd, 'T', &ttl, &has_ttl))
        return safe_write_error(out_buf, out_buf_size);

    // Build stored value: [4-byte flags][value data]
    if (cmd->value.len > UINT32_MAX - FLAGS_PREFIX_SIZE)
        return safe_write_error(out_buf, out_buf_size);
    uint32_t combined_len = FLAGS_PREFIX_SIZE + cmd->value.len;
    uint8_t stack_buf[8192];
    uint8_t* combined = stack_buf;
    bool heap = false;
    if (combined_len > sizeof(stack_buf)) {
        combined = new (std::nothrow) uint8_t[combined_len];
        if (!combined) {
            if (out_buf_size < 4)
                return safe_write_error(out_buf, out_buf_size);
            return mc_write_ns(out_buf);
        }
        heap = true;
    }
    encode_flags(combined, flags);
    if (cmd->value.len > 0)
        std::memcpy(combined + FLAGS_PREFIX_SIZE, cmd->value.data, cmd->value.len);

    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    std::string_view value(reinterpret_cast<const char*>(combined), combined_len);

    StoreSetStatus st;
    if (has_ttl && ttl > 0) {
        uint64_t ttl_ms = static_cast<uint64_t>(ttl) * 1000ULL;
        st = store_set_expire_after_ms_at(store, key, value, ttl_ms, now_ms);
    } else {
        st = store_set_at(store, key, value, now_ms);
    }

    if (heap)
        delete[] combined;

    if (st != StoreSetStatus::OK) {
        if (out_buf_size < 4)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_ns(out_buf);
    }

    if (has_meta_flag(cmd, 'q'))
        return 0; // quiet mode

    if (out_buf_size < 4)
        return safe_write_error(out_buf, out_buf_size);
    return mc_write_hd(out_buf);
}

static uint32_t handle_meta_delete(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                                   uint32_t out_buf_size) {
    std::string_view key(reinterpret_cast<const char*>(cmd->key.data), cmd->key.len);
    StoreDeleteStatus st = store_delete_at(store, key, now_ms);

    if (has_meta_flag(cmd, 'q'))
        return 0; // quiet mode

    if (st == StoreDeleteStatus::OK) {
        if (out_buf_size < 4)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_hd(out_buf);
    }
    if (out_buf_size < 4)
        return safe_write_error(out_buf, out_buf_size);
    return mc_write_nf(out_buf);
}

uint32_t mc_command_execute(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                            uint32_t out_buf_size) {
    ASSERT(cmd != nullptr, "mc_command_execute requires command");
    ASSERT(store != nullptr, "mc_command_execute requires store");
    ASSERT(out_buf != nullptr || out_buf_size == 0, "mc_command_execute null out with non-zero size");
    if (!cmd || !store || (!out_buf && out_buf_size > 0))
        return 0;

    switch (cmd->type) {
    case McCommandType::GET:
        return handle_legacy_get(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::SET:
        return handle_legacy_set(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::DELETE:
        return handle_legacy_delete(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::VERSION:
        if (out_buf_size < 22)
            return safe_write_error(out_buf, out_buf_size);
        return mc_write_version(out_buf);
    case McCommandType::META_GET:
        return handle_meta_get(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::META_SET:
        return handle_meta_set(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::META_DELETE:
        return handle_meta_delete(cmd, store, now_ms, out_buf, out_buf_size);
    case McCommandType::META_NOOP:
        if (out_buf_size < 4)
            return 0;
        return mc_write_mn(out_buf);
    }

    return safe_write_error(out_buf, out_buf_size);
}

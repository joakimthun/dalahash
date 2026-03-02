// memcached_response.h — Response formatters for memcached text protocol.

#pragma once

#include <cstdint>

// Legacy text protocol responses.
uint32_t mc_write_stored(uint8_t* out);
uint32_t mc_write_not_stored(uint8_t* out);
uint32_t mc_write_deleted(uint8_t* out);
uint32_t mc_write_not_found(uint8_t* out);
uint32_t mc_write_end(uint8_t* out);
uint32_t mc_write_error(uint8_t* out);
uint32_t mc_write_version(uint8_t* out);

// Legacy GET hit: "VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n"
uint32_t mc_write_value(uint8_t* out, const uint8_t* key, uint32_t key_len, uint32_t flags,
                        const uint8_t* value, uint32_t value_len);

// Meta protocol responses.
// VA <size> [flags]\r\n<data>\r\n
uint32_t mc_write_va(uint8_t* out, const uint8_t* value, uint32_t value_len, const char* extra_flags);
// HD\r\n — stored/deleted success
uint32_t mc_write_hd(uint8_t* out);
// HD [flags]\r\n — hit with metadata but no value
uint32_t mc_write_hd_flags(uint8_t* out, const char* extra_flags);
// EN\r\n — not found (meta get)
uint32_t mc_write_en(uint8_t* out);
// NS\r\n — not stored
uint32_t mc_write_ns(uint8_t* out);
// NF\r\n — not found (meta delete)
uint32_t mc_write_nf(uint8_t* out);
// MN\r\n — meta noop
uint32_t mc_write_mn(uint8_t* out);

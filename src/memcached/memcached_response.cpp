// memcached_response.cpp — Memcached text protocol response formatters.

#include "memcached_response.h"
#include "base/assert.h"

#include <cstring>

// Fast decimal formatting for uint32_t. Returns bytes written.
static uint32_t uint_to_str(uint8_t* out, uint32_t val) {
    uint8_t tmp[10];
    uint32_t n = 0;
    do {
        tmp[n++] = static_cast<uint8_t>('0' + (val % 10));
        val /= 10;
    } while (val > 0);
    for (uint32_t i = 0; i < n; i++)
        out[i] = tmp[n - 1 - i];
    return n;
}

uint32_t mc_write_stored(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_stored requires output buffer");
    std::memcpy(out, "STORED\r\n", 8);
    return 8;
}

uint32_t mc_write_not_stored(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_not_stored requires output buffer");
    std::memcpy(out, "NOT_STORED\r\n", 12);
    return 12;
}

uint32_t mc_write_deleted(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_deleted requires output buffer");
    std::memcpy(out, "DELETED\r\n", 9);
    return 9;
}

uint32_t mc_write_not_found(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_not_found requires output buffer");
    std::memcpy(out, "NOT_FOUND\r\n", 11);
    return 11;
}

uint32_t mc_write_end(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_end requires output buffer");
    std::memcpy(out, "END\r\n", 5);
    return 5;
}

uint32_t mc_write_error(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_error requires output buffer");
    std::memcpy(out, "ERROR\r\n", 7);
    return 7;
}

uint32_t mc_write_version(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_version requires output buffer");
    static constexpr char ver[] = "VERSION dalahash-1.0\r\n";
    static constexpr uint32_t ver_len = sizeof(ver) - 1;
    std::memcpy(out, ver, ver_len);
    return ver_len;
}

// "VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n"
uint32_t mc_write_value(uint8_t* out, const uint8_t* key, uint32_t key_len, uint32_t flags,
                        const uint8_t* value, uint32_t value_len) {
    ASSERT(out != nullptr, "mc_write_value requires output buffer");
    uint32_t n = 0;
    std::memcpy(out + n, "VALUE ", 6);
    n += 6;
    if (key_len > 0)
        std::memcpy(out + n, key, key_len);
    n += key_len;
    out[n++] = ' ';
    n += uint_to_str(out + n, flags);
    out[n++] = ' ';
    n += uint_to_str(out + n, value_len);
    out[n++] = '\r';
    out[n++] = '\n';
    if (value_len > 0)
        std::memcpy(out + n, value, value_len);
    n += value_len;
    out[n++] = '\r';
    out[n++] = '\n';
    std::memcpy(out + n, "END\r\n", 5);
    n += 5;
    return n;
}

// "VA <size> [extra_flags]\r\n<data>\r\n"
uint32_t mc_write_va(uint8_t* out, const uint8_t* value, uint32_t value_len, const char* extra_flags) {
    ASSERT(out != nullptr, "mc_write_va requires output buffer");
    uint32_t n = 0;
    std::memcpy(out + n, "VA ", 3);
    n += 3;
    n += uint_to_str(out + n, value_len);
    if (extra_flags && extra_flags[0] != '\0') {
        out[n++] = ' ';
        uint32_t fl = static_cast<uint32_t>(std::strlen(extra_flags));
        std::memcpy(out + n, extra_flags, fl);
        n += fl;
    }
    out[n++] = '\r';
    out[n++] = '\n';
    if (value_len > 0)
        std::memcpy(out + n, value, value_len);
    n += value_len;
    out[n++] = '\r';
    out[n++] = '\n';
    return n;
}

uint32_t mc_write_hd(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_hd requires output buffer");
    std::memcpy(out, "HD\r\n", 4);
    return 4;
}

uint32_t mc_write_hd_flags(uint8_t* out, const char* extra_flags) {
    ASSERT(out != nullptr, "mc_write_hd_flags requires output buffer");
    uint32_t n = 0;
    std::memcpy(out + n, "HD", 2);
    n += 2;
    if (extra_flags && extra_flags[0] != '\0') {
        out[n++] = ' ';
        uint32_t fl = static_cast<uint32_t>(std::strlen(extra_flags));
        std::memcpy(out + n, extra_flags, fl);
        n += fl;
    }
    out[n++] = '\r';
    out[n++] = '\n';
    return n;
}

uint32_t mc_write_en(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_en requires output buffer");
    std::memcpy(out, "EN\r\n", 4);
    return 4;
}

uint32_t mc_write_ns(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_ns requires output buffer");
    std::memcpy(out, "NS\r\n", 4);
    return 4;
}

uint32_t mc_write_nf(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_nf requires output buffer");
    std::memcpy(out, "NF\r\n", 4);
    return 4;
}

uint32_t mc_write_mn(uint8_t* out) {
    ASSERT(out != nullptr, "mc_write_mn requires output buffer");
    std::memcpy(out, "MN\r\n", 4);
    return 4;
}

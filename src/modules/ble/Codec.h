/**
 * Copyright (c) 2006-2026 LOVE Development Team
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 **/

#ifndef LOVE_BLE_CODEC_H
#define LOVE_BLE_CODEC_H

// LOVE
#include "common/Variant.h"

// C++
#include <cstdint>
#include <vector>

namespace love
{
namespace ble
{

// Protocol spec Section 11: Binary codec for application payloads.
//
// Format: version byte (0x01) followed by encoded value.
// Type tags: Nil(0x00), False(0x01), True(0x02), Number(0x03),
//            String(0x04), Array(0x05), Map(0x06).

class Codec
{
public:

	static constexpr uint8_t VERSION = 0x01;
	static constexpr int MAX_DEPTH = 64;

	// Tag constants matching spec Section 11.2
	static constexpr uint8_t TAG_NIL    = 0x00;
	static constexpr uint8_t TAG_FALSE  = 0x01;
	static constexpr uint8_t TAG_TRUE   = 0x02;
	static constexpr uint8_t TAG_NUMBER = 0x03;
	static constexpr uint8_t TAG_STRING = 0x04;
	static constexpr uint8_t TAG_ARRAY  = 0x05;
	static constexpr uint8_t TAG_MAP    = 0x06;

	// Encode a Variant into codec bytes. Returns false on error.
	static bool encode(const Variant &value, std::vector<uint8_t> &out);

	// Decode codec bytes into a Variant. Returns false on error.
	static bool decode(const uint8_t *data, size_t length, Variant &out);

private:

	static bool encodeValue(const Variant &value, std::vector<uint8_t> &out, int depth);
	static bool decodeValue(const uint8_t *data, size_t length, size_t &offset, Variant &out, int depth);

}; // Codec

} // ble
} // love

#endif // LOVE_BLE_CODEC_H

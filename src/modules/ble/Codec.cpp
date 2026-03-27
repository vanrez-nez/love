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

// LOVE
#include "Codec.h"

// C++
#include <algorithm>
#include <cstring>

namespace love
{
namespace ble
{

// Little-endian helpers

static void writeU32LE(std::vector<uint8_t> &out, uint32_t v)
{
	out.push_back((uint8_t)(v & 0xFF));
	out.push_back((uint8_t)((v >> 8) & 0xFF));
	out.push_back((uint8_t)((v >> 16) & 0xFF));
	out.push_back((uint8_t)((v >> 24) & 0xFF));
}

static bool readU32LE(const uint8_t *data, size_t length, size_t &offset, uint32_t &out)
{
	if (offset + 4 > length)
		return false;

	out = (uint32_t)data[offset]
	    | ((uint32_t)data[offset + 1] << 8)
	    | ((uint32_t)data[offset + 2] << 16)
	    | ((uint32_t)data[offset + 3] << 24);
	offset += 4;
	return true;
}

static void writeF64LE(std::vector<uint8_t> &out, double v)
{
	uint8_t bytes[8];
	memcpy(bytes, &v, 8);
	// Ensure little-endian output.
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
	for (int i = 0; i < 4; i++)
		std::swap(bytes[i], bytes[7 - i]);
#endif
	out.insert(out.end(), bytes, bytes + 8);
}

static bool readF64LE(const uint8_t *data, size_t length, size_t &offset, double &out)
{
	if (offset + 8 > length)
		return false;

	uint8_t bytes[8];
	memcpy(bytes, data + offset, 8);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
	for (int i = 0; i < 4; i++)
		std::swap(bytes[i], bytes[7 - i]);
#endif
	memcpy(&out, bytes, 8);
	offset += 8;
	return true;
}

// Determine if a SharedTable is a contiguous 1-based array.
// Per spec Section 11.3: Tables with contiguous 1-based integer keys encode as Arrays.

static bool isArray(const Variant::SharedTable *table, int &count)
{
	count = 0;
	if (table->pairs.empty())
	{
		// Empty table encodes as Array with count 0.
		return true;
	}

	// Check if all keys are consecutive numbers 1..N
	int maxKey = 0;
	for (const auto &pair : table->pairs)
	{
		if (pair.first.getType() != Variant::NUMBER)
			return false;

		double key = pair.first.getData().number;
		int ikey = (int)key;
		if ((double)ikey != key || ikey < 1)
			return false;

		if (ikey > maxKey)
			maxKey = ikey;
	}

	if (maxKey != (int)table->pairs.size())
		return false;

	count = maxKey;
	return true;
}

bool Codec::encode(const Variant &value, std::vector<uint8_t> &out)
{
	out.clear();
	out.push_back(VERSION);
	return encodeValue(value, out, 0);
}

bool Codec::encodeValue(const Variant &value, std::vector<uint8_t> &out, int depth)
{
	if (depth > MAX_DEPTH)
		return false;

	switch (value.getType())
	{
	case Variant::NIL:
		out.push_back(TAG_NIL);
		return true;

	case Variant::BOOLEAN:
		out.push_back(value.getData().boolean ? TAG_TRUE : TAG_FALSE);
		return true;

	case Variant::NUMBER:
		out.push_back(TAG_NUMBER);
		writeF64LE(out, value.getData().number);
		return true;

	case Variant::STRING:
	{
		out.push_back(TAG_STRING);
		const char *str = value.getData().string->str;
		size_t len = value.getData().string->len;
		writeU32LE(out, (uint32_t)len);
		out.insert(out.end(), (const uint8_t *)str, (const uint8_t *)str + len);
		return true;
	}

	case Variant::SMALLSTRING:
	{
		out.push_back(TAG_STRING);
		const char *str = value.getData().smallstring.str;
		size_t len = value.getData().smallstring.len;
		writeU32LE(out, (uint32_t)len);
		out.insert(out.end(), (const uint8_t *)str, (const uint8_t *)str + len);
		return true;
	}

	case Variant::TABLE:
	{
		Variant::SharedTable *table = value.getData().table;
		int arrayCount = 0;

		if (isArray(table, arrayCount))
		{
			// Encode as Array (spec tag 0x05)
			out.push_back(TAG_ARRAY);
			writeU32LE(out, (uint32_t)arrayCount);

			// Elements must be in index order 1..N
			for (int i = 1; i <= arrayCount; i++)
			{
				// Find element with key i
				const Variant *elem = nullptr;
				for (const auto &pair : table->pairs)
				{
					if (pair.first.getType() == Variant::NUMBER && (int)pair.first.getData().number == i)
					{
						elem = &pair.second;
						break;
					}
				}
				if (!elem)
					return false;
				if (!encodeValue(*elem, out, depth + 1))
					return false;
			}
		}
		else
		{
			// Encode as Map (spec tag 0x06)
			// Keys must be strings, sorted lexicographically.
			std::vector<std::pair<std::string, const Variant *>> entries;
			for (const auto &pair : table->pairs)
			{
				std::string key;
				if (pair.first.getType() == Variant::STRING)
					key = std::string(pair.first.getData().string->str, pair.first.getData().string->len);
				else if (pair.first.getType() == Variant::SMALLSTRING)
					key = std::string(pair.first.getData().smallstring.str, pair.first.getData().smallstring.len);
				else
					return false; // Map keys must be strings.

				entries.push_back({key, &pair.second});
			}

			std::sort(entries.begin(), entries.end(),
				[](const std::pair<std::string, const Variant *> &a,
				   const std::pair<std::string, const Variant *> &b) {
					return a.first < b.first;
				});

			out.push_back(TAG_MAP);
			writeU32LE(out, (uint32_t)entries.size());

			for (const auto &entry : entries)
			{
				// Encode key as string
				out.push_back(TAG_STRING);
				writeU32LE(out, (uint32_t)entry.first.size());
				out.insert(out.end(), entry.first.begin(), entry.first.end());

				// Encode value
				if (!encodeValue(*entry.second, out, depth + 1))
					return false;
			}
		}
		return true;
	}

	default:
		return false;
	}
}

bool Codec::decode(const uint8_t *data, size_t length, Variant &out)
{
	if (length < 1)
		return false;

	if (data[0] != VERSION)
		return false;

	size_t offset = 1;
	if (!decodeValue(data, length, offset, out, 0))
		return false;

	// Reject trailing bytes after a complete decode (spec Section 11.3 rule 4).
	if (offset != length)
		return false;

	return true;
}

bool Codec::decodeValue(const uint8_t *data, size_t length, size_t &offset, Variant &out, int depth)
{
	if (depth > MAX_DEPTH)
		return false;

	if (offset >= length)
		return false;

	uint8_t tag = data[offset++];

	switch (tag)
	{
	case TAG_NIL:
		out = Variant();
		return true;

	case TAG_FALSE:
		out = Variant(false);
		return true;

	case TAG_TRUE:
		out = Variant(true);
		return true;

	case TAG_NUMBER:
	{
		double v;
		if (!readF64LE(data, length, offset, v))
			return false;
		out = Variant(v);
		return true;
	}

	case TAG_STRING:
	{
		uint32_t len;
		if (!readU32LE(data, length, offset, len))
			return false;
		if (offset + len > length)
			return false;
		out = Variant((const char *)(data + offset), (size_t)len);
		offset += len;
		return true;
	}

	case TAG_ARRAY:
	{
		uint32_t count;
		if (!readU32LE(data, length, offset, count))
			return false;

		Variant::SharedTable *table = new Variant::SharedTable();
		for (uint32_t i = 0; i < count; i++)
		{
			Variant val;
			if (!decodeValue(data, length, offset, val, depth + 1))
			{
				table->release();
				return false;
			}
			// 1-based index keys
			table->pairs.push_back({Variant((double)(i + 1)), val});
		}
		out = Variant(table);
		return true;
	}

	case TAG_MAP:
	{
		uint32_t count;
		if (!readU32LE(data, length, offset, count))
			return false;

		Variant::SharedTable *table = new Variant::SharedTable();
		for (uint32_t i = 0; i < count; i++)
		{
			// Key must be a string
			Variant key;
			if (!decodeValue(data, length, offset, key, depth + 1))
			{
				table->release();
				return false;
			}
			if (key.getType() != Variant::STRING && key.getType() != Variant::SMALLSTRING)
			{
				table->release();
				return false;
			}

			Variant val;
			if (!decodeValue(data, length, offset, val, depth + 1))
			{
				table->release();
				return false;
			}

			table->pairs.push_back({key, val});
		}
		out = Variant(table);
		return true;
	}

	default:
		return false;
	}
}

} // ble
} // love

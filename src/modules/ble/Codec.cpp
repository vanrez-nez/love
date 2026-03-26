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

#include "Codec.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

namespace love
{
namespace ble
{
namespace codec
{

namespace
{

static const uint8_t VERSION = 1;
static const int MAX_DEPTH = 64;

enum Tag : uint8_t
{
	TAG_NIL = 0,
	TAG_FALSE = 1,
	TAG_TRUE = 2,
	TAG_NUMBER = 3,
	TAG_STRING = 4,
	TAG_ARRAY = 5,
	TAG_MAP = 6,
};

struct Writer
{
	std::vector<uint8_t> bytes;

	void writeByte(uint8_t value)
	{
		bytes.push_back(value);
	}

	void writeU32(uint32_t value)
	{
		bytes.push_back((uint8_t) (value & 0xFF));
		bytes.push_back((uint8_t) ((value >> 8) & 0xFF));
		bytes.push_back((uint8_t) ((value >> 16) & 0xFF));
		bytes.push_back((uint8_t) ((value >> 24) & 0xFF));
	}

	void writeDouble(double value)
	{
		uint64_t bits = 0;
		std::memcpy(&bits, &value, sizeof(bits));

		for (int i = 0; i < 8; i++)
			bytes.push_back((uint8_t) ((bits >> (i * 8)) & 0xFF));
	}

	void writeString(const char *data, size_t len)
	{
		writeU32((uint32_t) len);
		bytes.insert(bytes.end(), data, data + len);
	}

	void writeString(const std::string &value)
	{
		writeString(value.data(), value.size());
	}
};

struct Reader
{
	const std::vector<uint8_t> &bytes;
	size_t offset = 0;

	bool readByte(uint8_t &value)
	{
		if (offset >= bytes.size())
			return false;

		value = bytes[offset++];
		return true;
	}

	bool readU32(uint32_t &value)
	{
		if (offset + 4 > bytes.size())
			return false;

		value = (uint32_t) bytes[offset]
			| ((uint32_t) bytes[offset + 1] << 8)
			| ((uint32_t) bytes[offset + 2] << 16)
			| ((uint32_t) bytes[offset + 3] << 24);

		offset += 4;
		return true;
	}

	bool readDouble(double &value)
	{
		if (offset + 8 > bytes.size())
			return false;

		uint64_t bits = 0;

		for (int i = 0; i < 8; i++)
			bits |= (uint64_t) bytes[offset + i] << (i * 8);

		offset += 8;
		std::memcpy(&value, &bits, sizeof(value));
		return true;
	}

	bool readString(std::string &value)
	{
		uint32_t len = 0;
		if (!readU32(len))
			return false;

		if ((size_t) len > bytes.size() - offset)
			return false;

		value.assign((const char *) bytes.data() + offset, len);
		offset += len;
		return true;
	}
};

static bool getStringValue(const Variant &value, std::string &out)
{
	switch (value.getType())
	{
	case Variant::STRING:
		out.assign(value.getData().string->str, value.getData().string->len);
		return true;
	case Variant::SMALLSTRING:
		out.assign(value.getData().smallstring.str, value.getData().smallstring.len);
		return true;
	default:
		return false;
	}
}

static bool getArrayIndex(const Variant &key, size_t &index)
{
	if (key.getType() != Variant::NUMBER)
		return false;

	double raw = key.getData().number;
	if (raw < 1.0 || std::floor(raw) != raw || raw > (double) std::numeric_limits<uint32_t>::max())
		return false;

	index = (size_t) raw;
	return true;
}

static bool encodeVariant(Writer &writer, const Variant &input, std::string &error, int depth);

static bool encodeTable(Writer &writer, const Variant::SharedTable *table, std::string &error, int depth)
{
	if (depth > MAX_DEPTH)
	{
		error = "Payload nesting is too deep.";
		return false;
	}

	if (table == nullptr)
	{
		error = "Payload table is missing.";
		return false;
	}

	if (table->pairs.empty())
	{
		writer.writeByte(TAG_ARRAY);
		writer.writeU32(0);
		return true;
	}

	bool sawArrayKey = false;
	bool sawMapKey = false;
	size_t maxIndex = 0;
	std::vector<std::pair<size_t, Variant>> arrayEntries;
	std::vector<std::pair<std::string, Variant>> mapEntries;

	for (const auto &pair : table->pairs)
	{
		size_t index = 0;
		std::string key;

		if (getArrayIndex(pair.first, index))
		{
			sawArrayKey = true;
			maxIndex = std::max(maxIndex, index);
			arrayEntries.emplace_back(index, pair.second);
			continue;
		}

		if (getStringValue(pair.first, key))
		{
			sawMapKey = true;
			mapEntries.emplace_back(key, pair.second);
			continue;
		}

		error = "Payload tables may only use array indices or string keys.";
		return false;
	}

	if (sawArrayKey && sawMapKey)
	{
		error = "Mixed array/map payload tables are not supported.";
		return false;
	}

	if (sawArrayKey)
	{
		std::sort(arrayEntries.begin(), arrayEntries.end(), [](const auto &a, const auto &b) {
			return a.first < b.first;
		});

		if (arrayEntries.size() != maxIndex)
		{
			error = "Array payload tables must use contiguous 1-based indices.";
			return false;
		}

		for (size_t i = 0; i < arrayEntries.size(); i++)
		{
			if (arrayEntries[i].first != i + 1)
			{
				error = "Array payload tables must use contiguous 1-based indices.";
				return false;
			}
		}

		writer.writeByte(TAG_ARRAY);
		writer.writeU32((uint32_t) arrayEntries.size());

		for (const auto &entry : arrayEntries)
		{
			if (!encodeVariant(writer, entry.second, error, depth + 1))
				return false;
		}

		return true;
	}

	std::sort(mapEntries.begin(), mapEntries.end(), [](const auto &a, const auto &b) {
		return a.first < b.first;
	});

	for (size_t i = 1; i < mapEntries.size(); i++)
	{
		if (mapEntries[i - 1].first == mapEntries[i].first)
		{
			error = "Payload tables may not contain duplicate string keys.";
			return false;
		}
	}

	writer.writeByte(TAG_MAP);
	writer.writeU32((uint32_t) mapEntries.size());

	for (const auto &entry : mapEntries)
	{
		writer.writeString(entry.first);
		if (!encodeVariant(writer, entry.second, error, depth + 1))
			return false;
	}

	return true;
}

static bool encodeVariant(Writer &writer, const Variant &input, std::string &error, int depth)
{
	switch (input.getType())
	{
	case Variant::NIL:
		writer.writeByte(TAG_NIL);
		return true;
	case Variant::BOOLEAN:
		writer.writeByte(input.getData().boolean ? TAG_TRUE : TAG_FALSE);
		return true;
	case Variant::NUMBER:
		writer.writeByte(TAG_NUMBER);
		writer.writeDouble(input.getData().number);
		return true;
	case Variant::STRING:
		writer.writeByte(TAG_STRING);
		writer.writeString(input.getData().string->str, input.getData().string->len);
		return true;
	case Variant::SMALLSTRING:
		writer.writeByte(TAG_STRING);
		writer.writeString(input.getData().smallstring.str, input.getData().smallstring.len);
		return true;
	case Variant::TABLE:
		return encodeTable(writer, input.getData().table, error, depth);
	default:
		error = "Payload contains unsupported Lua values.";
		return false;
	}
}

static bool decodeVariant(Reader &reader, Variant &output, std::string &error, int depth)
{
	if (depth > MAX_DEPTH)
	{
		error = "Payload nesting is too deep.";
		return false;
	}

	uint8_t tag = 0;
	if (!reader.readByte(tag))
	{
		error = "Payload ended unexpectedly.";
		return false;
	}

	switch ((Tag) tag)
	{
	case TAG_NIL:
		output = Variant();
		return true;
	case TAG_FALSE:
		output = Variant(false);
		return true;
	case TAG_TRUE:
		output = Variant(true);
		return true;
	case TAG_NUMBER:
	{
		double value = 0.0;
		if (!reader.readDouble(value))
		{
			error = "Payload number was truncated.";
			return false;
		}

		output = Variant(value);
		return true;
	}
	case TAG_STRING:
	{
		std::string value;
		if (!reader.readString(value))
		{
			error = "Payload string was truncated.";
			return false;
		}

		output = Variant(value);
		return true;
	}
	case TAG_ARRAY:
	{
		uint32_t count = 0;
		if (!reader.readU32(count))
		{
			error = "Payload array length was truncated.";
			return false;
		}

		Variant::SharedTable *table = new Variant::SharedTable();
		table->pairs.reserve(count);

		for (uint32_t i = 0; i < count; i++)
		{
			Variant value;
			if (!decodeVariant(reader, value, error, depth + 1))
			{
				table->release();
				return false;
			}

			table->pairs.emplace_back(Variant((double) (i + 1)), value);
		}

		output = Variant(table);
		return true;
	}
	case TAG_MAP:
	{
		uint32_t count = 0;
		if (!reader.readU32(count))
		{
			error = "Payload map length was truncated.";
			return false;
		}

		Variant::SharedTable *table = new Variant::SharedTable();
		table->pairs.reserve(count);

		for (uint32_t i = 0; i < count; i++)
		{
			std::string key;
			Variant value;

			if (!reader.readString(key))
			{
				table->release();
				error = "Payload map key was truncated.";
				return false;
			}

			if (!decodeVariant(reader, value, error, depth + 1))
			{
				table->release();
				return false;
			}

			table->pairs.emplace_back(Variant(key), value);
		}

		output = Variant(table);
		return true;
	}
	default:
		error = "Payload uses an unknown codec tag.";
		return false;
	}
}

} // namespace

bool encode(const Variant &input, std::vector<uint8_t> &output, std::string &error)
{
	Writer writer;
	writer.writeByte(VERSION);

	if (!encodeVariant(writer, input, error, 0))
		return false;

	output = std::move(writer.bytes);
	return true;
}

bool decode(const std::vector<uint8_t> &input, Variant &output, std::string &error)
{
	Reader reader{input, 0};

	uint8_t version = 0;
	if (!reader.readByte(version))
	{
		error = "Payload is empty.";
		return false;
	}

	if (version != VERSION)
	{
		error = "Payload codec version is unsupported.";
		return false;
	}

	if (!decodeVariant(reader, output, error, 0))
		return false;

	if (reader.offset != input.size())
	{
		error = "Payload contains trailing bytes.";
		return false;
	}

	return true;
}

} // codec
} // ble
} // love

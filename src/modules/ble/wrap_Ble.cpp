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
#include "common/config.h"
#include "wrap_Ble.h"
#include "Ble.h"
#include "Codec.h"

#if defined(LOVE_IOS)
#include "apple/Ble.h"
#elif defined(LOVE_ANDROID)
#include "android/Ble.h"
#endif

namespace love
{
namespace ble
{

#define instance() (Module::getInstance<Ble>(Module::M_BLE))

static int w_state(lua_State *L)
{
	Ble::RadioState state = instance()->getState();
	const char *str;
	if (!Ble::getConstant(state, str))
		return luaL_error(L, "Unknown radio state.");
	lua_pushstring(L, str);
	return 1;
}

static int w_host(lua_State *L)
{
	// Expects a table argument: {room=, max=, transport=}
	luaL_checktype(L, 1, LUA_TTABLE);

	lua_getfield(L, 1, "room");
	std::string roomName = luaL_optstring(L, -1, "Room");
	lua_pop(L, 1);

	lua_getfield(L, 1, "max");
	int maxClients = (int)luaL_optinteger(L, -1, 4);
	lua_pop(L, 1);

	lua_getfield(L, 1, "transport");
	const char *transportStr = luaL_optstring(L, -1, "reliable");
	lua_pop(L, 1);

	Ble::Transport transport = Ble::TRANSPORT_RELIABLE;
	if (!Ble::getConstant(transportStr, transport))
		return luax_enumerror(L, "transport", Ble::getConstants(transport), transportStr);

	luax_catchexcept(L, [&]() { instance()->host(roomName, maxClients, transport); });
	return 0;
}

static int w_scan(lua_State *L)
{
	luax_catchexcept(L, [&]() { instance()->scan(); });
	return 0;
}

static int w_join(lua_State *L)
{
	const char *roomId = luaL_checkstring(L, 1);
	luax_catchexcept(L, [&]() { instance()->join(roomId); });
	return 0;
}

static int w_leave(lua_State *L)
{
	luax_catchexcept(L, [&]() { instance()->leave(); });
	return 0;
}

static int w_broadcast(lua_State *L)
{
	const char *msgType = luaL_checkstring(L, 1);

	// Encode Lua value at index 2 via Codec
	Variant v = luax_checkvariant(L, 2);
	std::vector<uint8_t> payload;
	if (!Codec::encode(v, payload))
		return luaL_error(L, "Failed to encode payload.");

	luax_catchexcept(L, [&]() { instance()->broadcast(msgType, payload); });
	return 0;
}

static int w_send(lua_State *L)
{
	const char *peerId = luaL_checkstring(L, 1);
	const char *msgType = luaL_checkstring(L, 2);

	// Encode Lua value at index 3 via Codec
	Variant v = luax_checkvariant(L, 3);
	std::vector<uint8_t> payload;
	if (!Codec::encode(v, payload))
		return luaL_error(L, "Failed to encode payload.");

	luax_catchexcept(L, [&]() { instance()->send(peerId, msgType, payload); });
	return 0;
}

// Push a single BleEvent as a Lua table onto the stack.
static void pushBleEvent(lua_State *L, const Ble::BleEvent &event)
{
	lua_createtable(L, 0, (int)event.fields.size() + 1);

	lua_pushstring(L, event.type.c_str());
	lua_setfield(L, -2, "type");

	for (const auto &pair : event.fields)
	{
		// For "payload" fields that are codec-encoded bytes, decode to Lua value.
		// All other fields are pushed as Variants.
		if (pair.first == "payload" && pair.second.getType() == Variant::STRING)
		{
			const char *data = pair.second.getData().string->str;
			size_t len = pair.second.getData().string->len;

			Variant decoded;
			if (Codec::decode((const uint8_t *)data, len, decoded))
				luax_pushvariant(L, decoded);
			else
				lua_pushnil(L);
		}
		else
		{
			luax_pushvariant(L, pair.second);
		}
		lua_setfield(L, -2, pair.first.c_str());
	}
}

static int w_poll(lua_State *L)
{
	Ble *ble = instance();
	Ble::BleEvent event;

	lua_newtable(L);
	int idx = 1;

	while (ble->pollEvent(event))
	{
		pushBleEvent(L, event);
		lua_rawseti(L, -2, idx++);
	}

	return 1;
}

static int w_local_id(lua_State *L)
{
	std::string id = instance()->getLocalId();
	lua_pushstring(L, id.c_str());
	return 1;
}

static int w_is_host(lua_State *L)
{
	lua_pushboolean(L, instance()->isHost());
	return 1;
}

static int w_peers(lua_State *L)
{
	std::vector<Ble::PeerInfo> peers = instance()->getPeers();

	lua_createtable(L, (int)peers.size(), 0);
	for (int i = 0; i < (int)peers.size(); i++)
	{
		lua_createtable(L, 0, 3);

		lua_pushstring(L, peers[i].peerId.c_str());
		lua_setfield(L, -2, "peer_id");

		lua_pushboolean(L, peers[i].isHost);
		lua_setfield(L, -2, "is_host");

		lua_pushstring(L, peers[i].status.c_str());
		lua_setfield(L, -2, "status");

		lua_rawseti(L, -2, i + 1);
	}

	return 1;
}

static int w_address(lua_State *L)
{
	std::string addr = instance()->getAddress();
	lua_pushstring(L, addr.c_str());
	return 1;
}

static const luaL_Reg functions[] =
{
	{ "state", w_state },
	{ "host", w_host },
	{ "scan", w_scan },
	{ "join", w_join },
	{ "leave", w_leave },
	{ "broadcast", w_broadcast },
	{ "send", w_send },
	{ "poll", w_poll },
	{ "local_id", w_local_id },
	{ "is_host", w_is_host },
	{ "peers", w_peers },
	{ "address", w_address },
	{ nullptr, nullptr }
};

extern "C" int luaopen_love_ble(lua_State *L)
{
	Ble *inst = instance();
	if (inst == nullptr)
	{
#if defined(LOVE_IOS)
		luax_catchexcept(L, [&]() { inst = new love::ble::apple::Ble(); });
#elif defined(LOVE_ANDROID)
		luax_catchexcept(L, [&]() { inst = new love::ble::android::Ble(); });
#else
		return luaL_error(L, "BLE is not supported on this platform.");
#endif
	}
	else
	{
		inst->retain();
	}

	WrappedModule w;
	w.module = inst;
	w.name = "ble";
	w.type = &Module::type;
	w.functions = functions;
	w.types = nullptr;

	return luax_register_module(L, w);
}

} // ble
} // love

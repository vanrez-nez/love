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

#include "wrap_Ble.h"

#include "Ble.h"

#if __has_include("BleVersion.gen.h")
#include "BleVersion.gen.h"
#endif
#ifndef BLE_BUILD_ID
#define BLE_BUILD_ID "unknown"
#endif

#ifdef LOVE_ANDROID
#include "android/Ble.h"
#elif defined(LOVE_IOS)
#include "apple/Ble.h"
#else
#include "sdl/Ble.h"
#endif

namespace love
{
namespace ble
{

#define instance() (Module::getInstance<Ble>(Module::M_BLE))

static Ble::Transport luax_checktransport(lua_State *L, int index)
{
	std::string transportName = luax_checkstring(L, index);
	Ble::Transport transport = Ble::TRANSPORT_RELIABLE;

	if (!Ble::getConstant(transportName.c_str(), transport))
		luax_enumerror(L, "BLE transport", Ble::getConstants(transport), transportName.c_str());

	return transport;
}

static Ble::ReliabilityConfig luax_readreliabilityconfig(lua_State *L, int tableIndex)
{
	Ble::ReliabilityConfig config;

	lua_getfield(L, tableIndex, "heartbeat_interval");
	if (lua_isnumber(L, -1))
		config.heartbeatInterval = lua_tonumber(L, -1);
	lua_pop(L, 1);

	lua_getfield(L, tableIndex, "fragment_spacing_ms");
	if (lua_isnumber(L, -1))
		config.fragmentSpacingMs = (int) lua_tonumber(L, -1);
	lua_pop(L, 1);

	lua_getfield(L, tableIndex, "dedup_window");
	if (lua_isnumber(L, -1))
		config.dedupWindow = (int) lua_tonumber(L, -1);
	lua_pop(L, 1);

	return config;
}

static void luax_pushpeers(lua_State *L, const std::vector<Ble::PeerInfo> &peers)
{
	lua_newtable(L);

	int index = 1;
	for (const Ble::PeerInfo &peer : peers)
	{
		lua_newtable(L);

		luax_pushstring(L, peer.peerId);
		lua_setfield(L, -2, "peer_id");

		luax_pushboolean(L, peer.isHost);
		lua_setfield(L, -2, "is_host");

		lua_rawseti(L, -2, index++);
	}
}

static void luax_pushevent(lua_State *L, const Ble::Event &event)
{
	lua_newtable(L);

	luax_pushstring(L, event.type);
	lua_setfield(L, -2, "type");

	for (const auto &field : event.fields)
	{
		luax_pushvariant(L, field.second);
		lua_setfield(L, -2, field.first.c_str());
	}
}

static int w_state(lua_State *L)
{
	const char *state = nullptr;

	luax_catchexcept(L, [&]() {
		if (!Ble::getConstant(instance()->getRadioState(), state))
			throw love::Exception("Unknown BLE radio state.");
	});

	lua_pushstring(L, state);
	return 1;
}

static int w_host(lua_State *L)
{
	luaL_checktype(L, 1, LUA_TTABLE);

	lua_getfield(L, 1, "room");
	std::string room = luax_checkstring(L, -1);
	lua_pop(L, 1);

	lua_getfield(L, 1, "max");
	int maxClients = luax_checkint(L, -1);
	lua_pop(L, 1);

	Ble::Transport transport = Ble::TRANSPORT_RELIABLE;
	lua_getfield(L, 1, "transport");
	if (!lua_isnoneornil(L, -1))
		transport = luax_checktransport(L, -1);
	lua_pop(L, 1);

	Ble::ReliabilityConfig reliability = luax_readreliabilityconfig(L, 1);

	luax_catchexcept(L, [&]() { instance()->host(room, maxClients, transport, reliability); });
	return 0;
}

static int w_scan(lua_State *L)
{
	luax_catchexcept(L, [&]() { instance()->scan(); });
	return 0;
}

static int w_join(lua_State *L)
{
	std::string roomId = luax_checkstring(L, 1);

	Ble::ReliabilityConfig reliability;
	if (lua_istable(L, 2))
		reliability = luax_readreliabilityconfig(L, 2);

	luax_catchexcept(L, [&]() { instance()->join(roomId, reliability); });
	return 0;
}

static int w_leave(lua_State *L)
{
	luax_catchexcept(L, [&]() { instance()->leave(); });
	return 0;
}

static int w_broadcast(lua_State *L)
{
	std::string msgType = luax_checkstring(L, 1);
	Variant payload = luax_checkvariant(L, 2, false);

	if (payload.getType() == Variant::UNKNOWN)
		return luaL_error(L, "Payload can't be stored safely.");

	luax_catchexcept(L, [&]() { instance()->broadcast(msgType, payload); });
	return 0;
}

static int w_send(lua_State *L)
{
	std::string peerId = luax_checkstring(L, 1);
	std::string msgType = luax_checkstring(L, 2);
	Variant payload = luax_checkvariant(L, 3, false);

	if (payload.getType() == Variant::UNKNOWN)
		return luaL_error(L, "Payload can't be stored safely.");

	luax_catchexcept(L, [&]() { instance()->send(peerId, msgType, payload); });
	return 0;
}

static int w_poll(lua_State *L)
{
	lua_newtable(L);

	Ble::Event event;
	int index = 1;

	while (instance()->poll(event))
	{
		luax_pushevent(L, event);
		lua_rawseti(L, -2, index++);
	}

	return 1;
}

static int w_localId(lua_State *L)
{
	std::string peerId;
	luax_catchexcept(L, [&]() { peerId = instance()->getLocalId(); });
	luax_pushstring(L, peerId);
	return 1;
}

static int w_address(lua_State *L)
{
	std::string address;
	luax_catchexcept(L, [&]() { address = instance()->getDeviceAddress(); });
	luax_pushstring(L, address);
	return 1;
}

static int w_isHost(lua_State *L)
{
	bool host = false;
	luax_catchexcept(L, [&]() { host = instance()->isHost(); });
	luax_pushboolean(L, host);
	return 1;
}

static int w_peers(lua_State *L)
{
	std::vector<Ble::PeerInfo> peers;
	luax_catchexcept(L, [&]() { peers = instance()->getPeers(); });
	luax_pushpeers(L, peers);
	return 1;
}

static int w_debugState(lua_State *L)
{
	std::string state;
	std::string address;
	luax_catchexcept(L, [&]() {
		state = instance()->getDebugState();
		address = instance()->getDeviceAddress();
	});
	std::string out = "build=" BLE_BUILD_ID "\n"
		"address=" + address + "\n" + state;
	luax_pushstring(L, out);
	return 1;
}

static const luaL_Reg functions[] =
{
	{ "state", w_state },
	{ "debug_state", w_debugState },
	{ "host", w_host },
	{ "scan", w_scan },
	{ "join", w_join },
	{ "leave", w_leave },
	{ "broadcast", w_broadcast },
	{ "send", w_send },
	{ "poll", w_poll },
	{ "localId", w_localId },
	{ "local_id", w_localId },
	{ "address", w_address },
	{ "isHost", w_isHost },
	{ "is_host", w_isHost },
	{ "peers", w_peers },
	{ nullptr, nullptr }
};

extern "C" int luaopen_love_ble(lua_State *L)
{
	Ble *module = instance();
	if (module == nullptr)
	{
		luax_catchexcept(L, [&]() {
#ifdef LOVE_ANDROID
			module = new love::ble::android::Ble();
#elif defined(LOVE_IOS)
			module = new love::ble::apple::Ble();
#else
			module = new love::ble::sdl::Ble();
#endif
		});
	}
	else
		module->retain();

	WrappedModule w;
	w.module = module;
	w.name = "ble";
	w.type = &Module::type;
	w.functions = functions;
	w.types = nullptr;

	int n = luax_register_module(L, w);

	lua_newtable(L);
	lua_pushstring(L, "reliable");
	lua_setfield(L, -2, "RELIABLE");
	lua_pushstring(L, "resilient");
	lua_setfield(L, -2, "RESILIENT");
	lua_setfield(L, -2, "TRANSPORT");

	return n;
}

} // ble
} // love

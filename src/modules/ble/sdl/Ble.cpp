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

#include "Ble.h"

namespace love
{
namespace ble
{
namespace sdl
{

Ble::Ble()
	: love::ble::Ble("love.ble.sdl")
{
}

Ble::RadioState Ble::getRadioState() const
{
	return RADIO_UNSUPPORTED;
}

void Ble::host(const std::string &room, int maxClients, Transport transport)
{
	LOVE_UNUSED(room);
	LOVE_UNUSED(maxClients);
	LOVE_UNUSED(transport);

	pushError("host_failed", "BLE hosting is not implemented in this build.");
}

void Ble::scan()
{
	pushError("scan_failed", "BLE scanning is not implemented in this build.");
}

void Ble::join(const std::string &roomId)
{
	LOVE_UNUSED(roomId);
	pushError("join_failed", "BLE joining is not implemented in this build.");
}

void Ble::leave()
{
}

void Ble::broadcast(const std::string &msgType, const Variant &payload)
{
	LOVE_UNUSED(msgType);
	LOVE_UNUSED(payload);
}

void Ble::send(const std::string &peerId, const std::string &msgType, const Variant &payload)
{
	LOVE_UNUSED(peerId);
	LOVE_UNUSED(msgType);
	LOVE_UNUSED(payload);
}

std::string Ble::getLocalId() const
{
	return "";
}

std::string Ble::getDeviceAddress() const
{
	return "";
}

bool Ble::isHost() const
{
	return false;
}

std::vector<love::ble::Ble::PeerInfo> Ble::getPeers() const
{
	return {};
}

} // sdl
} // ble
} // love

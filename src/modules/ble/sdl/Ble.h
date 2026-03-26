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

#ifndef LOVE_BLE_SDL_H
#define LOVE_BLE_SDL_H

#include "../Ble.h"

namespace love
{
namespace ble
{
namespace sdl
{

class Ble : public love::ble::Ble
{
public:

	Ble();
	virtual ~Ble() {}

	RadioState getRadioState() const override;
	void host(const std::string &room, int maxClients, Transport transport) override;
	void scan() override;
	void join(const std::string &roomId) override;
	void leave() override;
	void broadcast(const std::string &msgType, const Variant &payload) override;
	void send(const std::string &peerId, const std::string &msgType, const Variant &payload) override;
	std::string getLocalId() const override;
	std::string getDeviceAddress() const override;
	bool isHost() const override;
	std::vector<PeerInfo> getPeers() const override;
};

} // sdl
} // ble
} // love

#endif // LOVE_BLE_SDL_H

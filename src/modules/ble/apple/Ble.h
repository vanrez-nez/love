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

#ifndef LOVE_BLE_APPLE_BLE_H
#define LOVE_BLE_APPLE_BLE_H

// LOVE
#include "ble/Ble.h"

namespace love
{
namespace ble
{
namespace apple
{

class Ble : public love::ble::Ble
{
public:
	Ble();
	~Ble() override;

	RadioState getState() override;

	void host(const std::string &roomName, int maxClients, Transport transport) override;
	void scan() override;
	void join(const std::string &roomId) override;
	void leave() override;

	void broadcast(const std::string &msgType, const std::vector<uint8_t> &payload) override;
	void send(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload) override;

	std::string getLocalId() override;
	bool isHost() override;
	std::vector<PeerInfo> getPeers() override;
	std::string getAddress() override;

private:
	// Opaque pointer to Objective-C implementation (added in Phase 1)
	void *impl;

}; // Ble

} // apple
} // ble
} // love

#endif // LOVE_BLE_APPLE_BLE_H

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

#ifndef LOVE_BLE_H
#define LOVE_BLE_H

// LOVE
#include "common/Module.h"
#include "common/StringMap.h"
#include "common/Variant.h"
#include "thread/threads.h"

// C++
#include <string>
#include <vector>
#include <deque>
#include <map>

namespace love
{
namespace ble
{

class Ble : public Module
{
public:

	enum RadioState
	{
		RADIO_ON,
		RADIO_OFF,
		RADIO_UNAUTHORIZED,
		RADIO_UNSUPPORTED,
		RADIO_MAX_ENUM
	};

	enum Transport
	{
		TRANSPORT_RELIABLE,
		TRANSPORT_RESILIENT,
		TRANSPORT_MAX_ENUM
	};

	struct PeerInfo
	{
		std::string peerId;
		bool isHost;
		std::string status; // "connected" or "reconnecting"
	};

	struct BleEvent
	{
		std::string type;
		std::map<std::string, Variant> fields;
	};

	virtual ~Ble() {}

	// Radio state
	virtual RadioState getState() = 0;

	// Session management
	virtual void host(const std::string &roomName, int maxClients, Transport transport) = 0;
	virtual void scan() = 0;
	virtual void join(const std::string &roomId) = 0;
	virtual void leave() = 0;

	// Messaging
	virtual void broadcast(const std::string &msgType, const std::vector<uint8_t> &payload) = 0;
	virtual void send(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload) = 0;

	// State queries
	virtual std::string getLocalId() = 0;
	virtual bool isHost() = 0;
	virtual std::vector<PeerInfo> getPeers() = 0;
	virtual std::string getAddress() = 0;

	// Event queue
	void pushEvent(const BleEvent &event);
	bool pollEvent(BleEvent &event);
	void clearEvents();

	STRINGMAP_CLASS_DECLARE(RadioState);
	STRINGMAP_CLASS_DECLARE(Transport);

protected:

	Ble(const char *name);

	love::thread::MutexRef mutex;
	std::deque<BleEvent> eventQueue;

}; // Ble

} // ble
} // love

#endif // LOVE_BLE_H

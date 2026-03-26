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

#include "common/Module.h"
#include "common/StringMap.h"
#include "common/Variant.h"

#include <deque>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

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
		bool isHost = false;
	};

	struct Event
	{
		std::string type;
		std::vector<std::pair<std::string, Variant>> fields;
	};

	struct ReliabilityConfig
	{
		double heartbeatInterval;
		int fragmentSpacingMs;
		int dedupWindow;

		ReliabilityConfig()
			: heartbeatInterval(2.0)
			, fragmentSpacingMs(15)
			, dedupWindow(64)
		{}
	};

	Ble(const char *name);
	virtual ~Ble() {}

	virtual RadioState getRadioState() const = 0;
	virtual void host(const std::string &room, int maxClients, Transport transport, const ReliabilityConfig &reliability = ReliabilityConfig()) = 0;
	virtual void scan() = 0;
	virtual void join(const std::string &roomId, const ReliabilityConfig &reliability = ReliabilityConfig()) = 0;
	virtual void leave() = 0;
	virtual void broadcast(const std::string &msgType, const Variant &payload) = 0;
	virtual void send(const std::string &peerId, const std::string &msgType, const Variant &payload) = 0;
	virtual std::string getLocalId() const = 0;
	virtual std::string getDeviceAddress() const = 0;
	virtual bool isHost() const = 0;
	virtual std::vector<PeerInfo> getPeers() const = 0;
	virtual std::string getDebugState() const = 0;

	bool poll(Event &event);
	STRINGMAP_CLASS_DECLARE(RadioState);
	STRINGMAP_CLASS_DECLARE(Transport);

protected:

	void pushEvent(const Event &event);
	void pushError(const std::string &code, const std::string &detail);
	void pushDiagnostic(const std::string &platform, const std::string &message);

private:

	mutable std::mutex eventMutex;
	std::deque<Event> events;
};

} // ble
} // love

#endif // LOVE_BLE_H

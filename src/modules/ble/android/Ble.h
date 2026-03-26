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

#ifndef LOVE_BLE_ANDROID_H
#define LOVE_BLE_ANDROID_H

#include "../Ble.h"

#include <cstdint>
#include <mutex>

namespace love
{
namespace ble
{
namespace android
{

class Ble : public love::ble::Ble
{
public:

	Ble();
	virtual ~Ble() override;

	RadioState getRadioState() const override;
	void host(const std::string &room, int maxClients, Transport transport, const ReliabilityConfig &reliability = ReliabilityConfig()) override;
	void scan() override;
	void join(const std::string &roomId, const ReliabilityConfig &reliability = ReliabilityConfig()) override;
	void leave() override;
	void broadcast(const std::string &msgType, const Variant &payload) override;
	void send(const std::string &peerId, const std::string &msgType, const Variant &payload) override;
	std::string getLocalId() const override;
	std::string getDeviceAddress() const override;
	bool isHost() const override;
	std::vector<PeerInfo> getPeers() const override;
	std::string getDebugState() const override;

	void onRoomFound(const std::string &roomId, const std::string &sessionId, const std::string &name, const std::string &transport, int peerCount, int maxClients, int rssi);
	void onRoomLost(const std::string &roomId);
	void onHosted(const std::string &sessionId, const std::string &peerId, const std::string &transport);
	void onJoined(const std::string &sessionId, const std::string &roomId, const std::string &peerId, const std::string &hostId, const std::string &transport);
	void onJoinFailed(const std::string &reason, const std::string &roomId);
	void onPeerJoined(const std::string &peerId);
	void onPeerLeft(const std::string &peerId, const std::string &reason);
	void onMessage(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload);
	void onSessionMigrating(const std::string &oldHostId, const std::string &newHostId);
	void onSessionResumed(const std::string &sessionId, const std::string &newHostId);
	void onSessionEnded(const std::string &reason);
	void onError(const std::string &code, const std::string &detail);
	void onPeerStatus(const std::string &peerId, const std::string &status);
	void onDiagnostic(const std::string &message);

private:

	bool ensureAvailable(const char *errorCode, const char *verb);
	void clearSessionLocked();
	Transport parseTransport(const std::string &transport) const;

	mutable std::mutex stateMutex;
	std::string localId;
	std::string roomId;
	std::string sessionId;
	Transport activeTransport = TRANSPORT_RELIABLE;
	bool hostActive = false;
	std::vector<PeerInfo> peers;
};

} // android
} // ble
} // love

#endif // LOVE_BLE_ANDROID_H

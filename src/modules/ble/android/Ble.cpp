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

#include "../Codec.h"
#include "common/android.h"

#include <algorithm>
#include <jni.h>

namespace love
{
namespace ble
{
namespace android
{

static Ble *instance = nullptr;

static std::string fromJString(JNIEnv *env, jstring value)
{
	if (value == nullptr)
		return "";

	const char *chars = env->GetStringUTFChars(value, nullptr);
	std::string result(chars != nullptr ? chars : "");
	if (chars != nullptr)
		env->ReleaseStringUTFChars(value, chars);
	return result;
}

static Variant makeStringVariant(const std::string &value)
{
	return Variant(value);
}

static std::vector<uint8_t> fromJByteArray(JNIEnv *env, jbyteArray value)
{
	std::vector<uint8_t> result;

	if (value == nullptr)
		return result;

	jsize len = env->GetArrayLength(value);
	if (len <= 0)
		return result;

	result.resize((size_t) len);
	env->GetByteArrayRegion(value, 0, len, reinterpret_cast<jbyte *>(result.data()));
	return result;
}

static Variant makePeersVariant(const std::vector<love::ble::Ble::PeerInfo> &peers, const std::string &localId)
{
	Variant::SharedTable *list = new Variant::SharedTable();
	int index = 1;

	for (const auto &peer : peers)
	{
		if (peer.peerId == localId)
			continue;

		Variant::SharedTable *peerTable = new Variant::SharedTable();
		peerTable->pairs.emplace_back(Variant("peer_id"), Variant(peer.peerId));
		peerTable->pairs.emplace_back(Variant("is_host"), Variant(peer.isHost));

		list->pairs.emplace_back(Variant((double) index++), Variant(peerTable));
	}

	return Variant(list);
}

Ble::Ble()
	: love::ble::Ble("love.ble.android")
{
	instance = this;
}

Ble::~Ble()
{
	if (instance == this)
		instance = nullptr;
}

Ble::RadioState Ble::getRadioState() const
{
	std::string state = love::android::getBluetoothRadioState();
	RadioState out = RADIO_UNSUPPORTED;

	if (love::ble::Ble::getConstant(state.c_str(), out))
		return out;

	return RADIO_UNSUPPORTED;
}

void Ble::clearSessionLocked()
{
	localId.clear();
	roomId.clear();
	sessionId.clear();
	hostActive = false;
	peers.clear();
}

bool Ble::ensureAvailable(const char *errorCode, const char *verb)
{
	if (!love::android::hasBluetoothLE())
	{
		pushError("transport_unavailable", "Bluetooth LE is not available on this device.");
		return false;
	}

	if (!love::android::hasBluetoothPermission())
	{
		love::android::requestBluetoothPermission();
		if (!love::android::hasBluetoothPermission())
		{
			pushError(errorCode, std::string("Bluetooth permission is required to ") + verb + ".");
			return false;
		}
	}

	if (getRadioState() != RADIO_ON)
	{
		pushError(errorCode, std::string("Bluetooth radio must be on to ") + verb + ".");
		return false;
	}

	return true;
}

Ble::Transport Ble::parseTransport(const std::string &transport) const
{
	Transport value = TRANSPORT_RELIABLE;
	if (love::ble::Ble::getConstant(transport.c_str(), value))
		return value;

	return TRANSPORT_RELIABLE;
}

void Ble::host(const std::string &room, int maxClients, Transport transport, const ReliabilityConfig &reliability)
{
	if (!ensureAvailable("host_failed", "host a BLE room"))
		return;

	const char *transportName = nullptr;
	if (!love::ble::Ble::getConstant(transport, transportName))
	{
		pushError("host_failed", "Unknown BLE transport.");
		return;
	}

	love::android::bleApplyReliabilityConfig(reliability.heartbeatInterval, reliability.fragmentSpacingMs, reliability.dedupWindow);
	if (!love::android::bleHost(room, maxClients, transportName))
		return;
}

void Ble::scan()
{
	if (!ensureAvailable("scan_failed", "scan for BLE rooms"))
		return;

	if (!love::android::bleScan())
		return;
}

void Ble::join(const std::string &roomIdValue, const ReliabilityConfig &reliability)
{
	if (!ensureAvailable("join_failed", "join a BLE room"))
		return;

	love::android::bleApplyReliabilityConfig(reliability.heartbeatInterval, reliability.fragmentSpacingMs, reliability.dedupWindow);
	if (!love::android::bleJoin(roomIdValue))
		return;
}

void Ble::leave()
{
	love::android::bleLeave();
}

void Ble::broadcast(const std::string &msgType, const Variant &payload)
{
	if (!ensureAvailable("send_failed", "broadcast BLE messages"))
		return;

	std::vector<uint8_t> bytes;
	std::string error;
	if (!love::ble::codec::encode(payload, bytes, error))
	{
		pushError("invalid_payload", error);
		return;
	}

	if (!love::android::bleBroadcast(msgType, bytes))
		return;
}

void Ble::send(const std::string &peerId, const std::string &msgType, const Variant &payload)
{
	if (!ensureAvailable("send_failed", "send BLE messages"))
		return;

	std::vector<uint8_t> bytes;
	std::string error;
	if (!love::ble::codec::encode(payload, bytes, error))
	{
		pushError("invalid_payload", error);
		return;
	}

	if (!love::android::bleSend(peerId, msgType, bytes))
		return;
}

std::string Ble::getLocalId() const
{
	std::lock_guard<std::mutex> lock(stateMutex);
	return localId;
}

std::string Ble::getDeviceAddress() const
{
	return love::android::getBluetoothAdapterAddress();
}

bool Ble::isHost() const
{
	std::lock_guard<std::mutex> lock(stateMutex);
	return hostActive;
}

std::vector<love::ble::Ble::PeerInfo> Ble::getPeers() const
{
	std::lock_guard<std::mutex> lock(stateMutex);
	return peers;
}

std::string Ble::getDebugState() const
{
	std::string result = love::android::bleDebugState();
	return result;
}

void Ble::onRoomFound(const std::string &roomIdValue, const std::string &sessionIdValue, const std::string &name, const std::string &transport, int peerCount, int maxClients, int rssi)
{
	Event event;
	event.type = "room_found";
	event.fields.emplace_back("room_id", makeStringVariant(roomIdValue));
	event.fields.emplace_back("session_id", makeStringVariant(sessionIdValue));
	event.fields.emplace_back("name", makeStringVariant(name));
	event.fields.emplace_back("transport", makeStringVariant(transport));
	event.fields.emplace_back("peer_count", Variant((double) peerCount));
	event.fields.emplace_back("max", Variant((double) maxClients));
	event.fields.emplace_back("rssi", Variant((double) rssi));
	pushEvent(event);
}

void Ble::onRoomLost(const std::string &roomIdValue)
{
	Event event;
	event.type = "room_lost";
	event.fields.emplace_back("room_id", makeStringVariant(roomIdValue));
	pushEvent(event);
}

void Ble::onHosted(const std::string &sessionIdValue, const std::string &peerId, const std::string &transport)
{
	Event event;
	event.type = "hosted";

	{
		std::lock_guard<std::mutex> lock(stateMutex);
		clearSessionLocked();
		sessionId = sessionIdValue;
		localId = peerId;
		hostActive = true;
		activeTransport = parseTransport(transport);
	}

	event.fields.emplace_back("session_id", makeStringVariant(sessionIdValue));
	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("transport", makeStringVariant(transport));
	event.fields.emplace_back("peers", makePeersVariant({}, peerId));
	pushEvent(event);
}

void Ble::onJoined(const std::string &sessionIdValue, const std::string &roomIdValue, const std::string &peerId, const std::string &hostId, const std::string &transport)
{
	Event event;
	event.type = "joined";

	std::vector<PeerInfo> snapshot;

	{
		std::lock_guard<std::mutex> lock(stateMutex);
		clearSessionLocked();
		sessionId = sessionIdValue;
		roomId = roomIdValue;
		localId = peerId;
		hostActive = false;
		activeTransport = parseTransport(transport);
		peers.push_back({hostId, true});
		snapshot = peers;
	}

	event.fields.emplace_back("session_id", makeStringVariant(sessionIdValue));
	event.fields.emplace_back("room_id", makeStringVariant(roomIdValue));
	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("host_id", makeStringVariant(hostId));
	event.fields.emplace_back("transport", makeStringVariant(transport));
	event.fields.emplace_back("peers", makePeersVariant(snapshot, peerId));
	pushEvent(event);
}

void Ble::onJoinFailed(const std::string &reason, const std::string &roomIdValue)
{
	{
		std::lock_guard<std::mutex> lock(stateMutex);
		clearSessionLocked();
	}

	Event event;
	event.type = "join_failed";
	event.fields.emplace_back("reason", makeStringVariant(reason));
	event.fields.emplace_back("room_id", makeStringVariant(roomIdValue));
	pushEvent(event);
}

void Ble::onPeerJoined(const std::string &peerId)
{
	Event event;
	event.type = "peer_joined";

	std::vector<PeerInfo> snapshot;
	std::string localIdSnapshot;

	{
		std::lock_guard<std::mutex> lock(stateMutex);
		auto it = std::find_if(peers.begin(), peers.end(), [&](const PeerInfo &peer) { return peer.peerId == peerId; });
		if (it == peers.end())
			peers.push_back({peerId, false});

		snapshot = peers;
		localIdSnapshot = localId;
	}

	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("peers", makePeersVariant(snapshot, localIdSnapshot));
	pushEvent(event);
}

void Ble::onPeerLeft(const std::string &peerId, const std::string &reason)
{
	Event event;
	event.type = "peer_left";

	std::vector<PeerInfo> snapshot;
	std::string localIdSnapshot;

	{
		std::lock_guard<std::mutex> lock(stateMutex);
		peers.erase(std::remove_if(peers.begin(), peers.end(), [&](const PeerInfo &peer) {
			return peer.peerId == peerId;
		}), peers.end());
		snapshot = peers;
		localIdSnapshot = localId;
	}

	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("reason", makeStringVariant(reason));
	event.fields.emplace_back("peers", makePeersVariant(snapshot, localIdSnapshot));
	pushEvent(event);
}

void Ble::onMessage(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload)
{
	Variant decodedPayload;
	std::string error;
	if (!love::ble::codec::decode(payload, decodedPayload, error))
	{
		pushError("invalid_payload", error);
		return;
	}

	Event event;
	event.type = "message";
	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("msg_type", makeStringVariant(msgType));
	event.fields.emplace_back("payload", decodedPayload);
	pushEvent(event);
}

void Ble::onSessionMigrating(const std::string &oldHostId, const std::string &newHostId)
{
	Event event;
	event.type = "session_migrating";
	event.fields.emplace_back("old_host_id", makeStringVariant(oldHostId));
	event.fields.emplace_back("new_host_id", makeStringVariant(newHostId));
	pushEvent(event);
}

void Ble::onSessionResumed(const std::string &sessionIdValue, const std::string &newHostId)
{
	Event event;
	event.type = "session_resumed";

	std::vector<PeerInfo> snapshot;
	std::string localIdSnapshot;

	{
		std::lock_guard<std::mutex> lock(stateMutex);
		sessionId = sessionIdValue;
		hostActive = localId == newHostId;

		for (auto it = peers.begin(); it != peers.end();)
		{
			if (it->isHost && it->peerId != newHostId)
				it = peers.erase(it);
			else
				++it;
		}

		if (localId != newHostId)
		{
			auto host = std::find_if(peers.begin(), peers.end(), [&](const PeerInfo &peer) { return peer.peerId == newHostId; });
			if (host == peers.end())
				peers.push_back({newHostId, true});
			else
				host->isHost = true;
		}

		snapshot = peers;
		localIdSnapshot = localId;
	}

	event.fields.emplace_back("new_host_id", makeStringVariant(newHostId));
	event.fields.emplace_back("session_id", makeStringVariant(sessionIdValue));
	event.fields.emplace_back("peers", makePeersVariant(snapshot, localIdSnapshot));
	pushEvent(event);
}

void Ble::onPeerStatus(const std::string &peerId, const std::string &status)
{
	Event event;
	event.type = "peer_status";
	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("status", makeStringVariant(status));
	pushEvent(event);
}

void Ble::onSessionEnded(const std::string &reason)
{
	{
		std::lock_guard<std::mutex> lock(stateMutex);
		clearSessionLocked();
	}

	Event event;
	event.type = "session_ended";
	event.fields.emplace_back("reason", makeStringVariant(reason));
	pushEvent(event);
}

void Ble::onError(const std::string &code, const std::string &detail)
{
	pushError(code, detail);
}

void Ble::onDiagnostic(const std::string &message)
{
	pushDiagnostic("android", message);
}

} // android
} // ble
} // love

extern "C"
{

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnRoomFound(JNIEnv *env, jclass, jstring roomId, jstring sessionId, jstring name, jstring transport, jint peerCount, jint maxClients, jint rssi)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onRoomFound(love::ble::android::fromJString(env, roomId), love::ble::android::fromJString(env, sessionId), love::ble::android::fromJString(env, name), love::ble::android::fromJString(env, transport), (int) peerCount, (int) maxClients, (int) rssi);
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnRoomLost(JNIEnv *env, jclass, jstring roomId)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onRoomLost(love::ble::android::fromJString(env, roomId));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnHosted(JNIEnv *env, jclass, jstring sessionId, jstring peerId, jstring transport)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onHosted(love::ble::android::fromJString(env, sessionId), love::ble::android::fromJString(env, peerId), love::ble::android::fromJString(env, transport));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnJoined(JNIEnv *env, jclass, jstring sessionId, jstring roomId, jstring peerId, jstring hostId, jstring transport)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onJoined(love::ble::android::fromJString(env, sessionId), love::ble::android::fromJString(env, roomId), love::ble::android::fromJString(env, peerId), love::ble::android::fromJString(env, hostId), love::ble::android::fromJString(env, transport));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnJoinFailed(JNIEnv *env, jclass, jstring reason, jstring roomId)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onJoinFailed(love::ble::android::fromJString(env, reason), love::ble::android::fromJString(env, roomId));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnPeerJoined(JNIEnv *env, jclass, jstring peerId)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onPeerJoined(love::ble::android::fromJString(env, peerId));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnPeerLeft(JNIEnv *env, jclass, jstring peerId, jstring reason)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onPeerLeft(love::ble::android::fromJString(env, peerId), love::ble::android::fromJString(env, reason));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnMessage(JNIEnv *env, jclass, jstring peerId, jstring msgType, jbyteArray payload)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onMessage(love::ble::android::fromJString(env, peerId), love::ble::android::fromJString(env, msgType), love::ble::android::fromJByteArray(env, payload));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnSessionMigrating(JNIEnv *env, jclass, jstring oldHostId, jstring newHostId)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onSessionMigrating(love::ble::android::fromJString(env, oldHostId), love::ble::android::fromJString(env, newHostId));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnSessionResumed(JNIEnv *env, jclass, jstring sessionId, jstring newHostId)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onSessionResumed(love::ble::android::fromJString(env, sessionId), love::ble::android::fromJString(env, newHostId));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnPeerStatus(JNIEnv *env, jclass, jstring peerId, jstring status)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onPeerStatus(love::ble::android::fromJString(env, peerId), love::ble::android::fromJString(env, status));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnSessionEnded(JNIEnv *env, jclass, jstring reason)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onSessionEnded(love::ble::android::fromJString(env, reason));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnError(JNIEnv *env, jclass, jstring code, jstring detail)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onError(love::ble::android::fromJString(env, code), love::ble::android::fromJString(env, detail));
}

JNIEXPORT void JNICALL Java_org_love2d_android_ble_BleManager_nativeOnDiagnostic(JNIEnv *env, jclass, jstring message)
{
	if (love::ble::android::instance != nullptr)
		love::ble::android::instance->onDiagnostic(love::ble::android::fromJString(env, message));
}

}

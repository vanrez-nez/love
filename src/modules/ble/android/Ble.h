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

#ifndef LOVE_BLE_ANDROID_BLE_H
#define LOVE_BLE_ANDROID_BLE_H

// LOVE
#include "ble/Ble.h"

// JNI
#include <jni.h>

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

	// JNI native callbacks (called from Java BleManager)
	static void JNICALL jniOnHosted(JNIEnv *env, jobject thiz, jstring sessionId, jstring peerId, jstring transport);
	static void JNICALL jniOnJoined(JNIEnv *env, jobject thiz, jstring sessionId, jstring roomId, jstring peerId, jstring hostId, jstring transport);
	static void JNICALL jniOnJoinFailed(JNIEnv *env, jobject thiz, jstring reason, jstring roomId);
	static void JNICALL jniOnRoomFound(JNIEnv *env, jobject thiz, jstring roomId, jstring sessionId, jstring name, jstring transport, jint peerCount, jint max, jint rssi);
	static void JNICALL jniOnRoomLost(JNIEnv *env, jobject thiz, jstring roomId);
	static void JNICALL jniOnPeerJoined(JNIEnv *env, jobject thiz, jstring peerId);
	static void JNICALL jniOnPeerLeft(JNIEnv *env, jobject thiz, jstring peerId, jstring reason);
	static void JNICALL jniOnPeerStatus(JNIEnv *env, jobject thiz, jstring peerId, jstring status);
	static void JNICALL jniOnMessage(JNIEnv *env, jobject thiz, jstring peerId, jstring msgType, jbyteArray payload);
	static void JNICALL jniOnSessionMigrating(JNIEnv *env, jobject thiz, jstring oldHostId, jstring newHostId);
	static void JNICALL jniOnSessionResumed(JNIEnv *env, jobject thiz, jstring sessionId, jstring newHostId);
	static void JNICALL jniOnSessionEnded(JNIEnv *env, jobject thiz, jstring reason);
	static void JNICALL jniOnError(JNIEnv *env, jobject thiz, jstring code, jstring detail);
	static void JNICALL jniOnDiagnostic(JNIEnv *env, jobject thiz, jstring message);

}; // Ble

} // android
} // ble
} // love

#endif // LOVE_BLE_ANDROID_BLE_H

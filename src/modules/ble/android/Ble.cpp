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

#include <jni.h>
#include <SDL3/SDL.h>
#include <string>
#include <vector>
#include <cstring>

// JNI helper: jstring -> std::string (null-safe)
static std::string jstringToStd(JNIEnv *env, jstring jstr)
{
	if (!jstr) return "";
	const char *chars = env->GetStringUTFChars(jstr, nullptr);
	std::string result(chars);
	env->ReleaseStringUTFChars(jstr, chars);
	return result;
}

// Global instance pointer for JNI callbacks
static love::ble::android::Ble *g_bleInstance = nullptr;

namespace love
{
namespace ble
{
namespace android
{

// Cached JNI references
static jclass g_bleManagerClass = nullptr;
static jobject g_bleManagerObj = nullptr;

// Method IDs
static jmethodID g_getRadioState = nullptr;
static jmethodID g_host = nullptr;
static jmethodID g_scan = nullptr;
static jmethodID g_join = nullptr;
static jmethodID g_leave = nullptr;
static jmethodID g_broadcast = nullptr;
static jmethodID g_send = nullptr;
static jmethodID g_getLocalId = nullptr;
static jmethodID g_isHosting = nullptr;
static jmethodID g_getAddress = nullptr;
static jmethodID g_getPeersString = nullptr;

static bool initJNI()
{
	if (g_bleManagerObj != nullptr)
		return true;

	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env) return false;

	jobject activity = (jobject)SDL_GetAndroidActivity();
	if (!activity) return false;

	// Find BleManager class
	jclass cls = env->FindClass("org/love2d/android/ble/BleManager");
	if (!cls) { env->ExceptionClear(); return false; }
	g_bleManagerClass = (jclass)env->NewGlobalRef(cls);
	env->DeleteLocalRef(cls);

	// Construct BleManager(Context)
	jmethodID ctor = env->GetMethodID(g_bleManagerClass, "<init>", "(Landroid/content/Context;)V");
	if (!ctor) { env->ExceptionClear(); return false; }

	jobject obj = env->NewObject(g_bleManagerClass, ctor, activity);
	if (!obj) { env->ExceptionClear(); return false; }
	g_bleManagerObj = env->NewGlobalRef(obj);
	env->DeleteLocalRef(obj);

	// Cache method IDs
	g_getRadioState = env->GetMethodID(g_bleManagerClass, "getRadioState", "()Ljava/lang/String;");
	g_host = env->GetMethodID(g_bleManagerClass, "host", "(Ljava/lang/String;ILjava/lang/String;)V");
	g_scan = env->GetMethodID(g_bleManagerClass, "scan", "()V");
	g_join = env->GetMethodID(g_bleManagerClass, "join", "(Ljava/lang/String;)V");
	g_leave = env->GetMethodID(g_bleManagerClass, "leave", "()V");
	g_broadcast = env->GetMethodID(g_bleManagerClass, "broadcast", "(Ljava/lang/String;[B)V");
	g_send = env->GetMethodID(g_bleManagerClass, "send", "(Ljava/lang/String;Ljava/lang/String;[B)V");
	g_getLocalId = env->GetMethodID(g_bleManagerClass, "getLocalId", "()Ljava/lang/String;");
	g_isHosting = env->GetMethodID(g_bleManagerClass, "isHosting", "()Z");
	g_getAddress = env->GetMethodID(g_bleManagerClass, "getAddress", "()Ljava/lang/String;");
	g_getPeersString = env->GetMethodID(g_bleManagerClass, "getPeersString", "()Ljava/lang/String;");

	// Register native callbacks
	JNINativeMethod nativeMethods[] = {
		{"nativeOnHosted", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnHosted},
		{"nativeOnJoined", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnJoined},
		{"nativeOnJoinFailed", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnJoinFailed},
		{"nativeOnRoomFound", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;III)V",
			(void *)&Ble::jniOnRoomFound},
		{"nativeOnRoomLost", "(Ljava/lang/String;)V",
			(void *)&Ble::jniOnRoomLost},
		{"nativeOnPeerJoined", "(Ljava/lang/String;)V",
			(void *)&Ble::jniOnPeerJoined},
		{"nativeOnPeerLeft", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnPeerLeft},
		{"nativeOnPeerStatus", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnPeerStatus},
		{"nativeOnMessage", "(Ljava/lang/String;Ljava/lang/String;[B)V",
			(void *)&Ble::jniOnMessage},
		{"nativeOnSessionMigrating", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnSessionMigrating},
		{"nativeOnSessionResumed", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnSessionResumed},
		{"nativeOnSessionEnded", "(Ljava/lang/String;)V",
			(void *)&Ble::jniOnSessionEnded},
		{"nativeOnError", "(Ljava/lang/String;Ljava/lang/String;)V",
			(void *)&Ble::jniOnError},
		{"nativeOnDiagnostic", "(Ljava/lang/String;)V",
			(void *)&Ble::jniOnDiagnostic},
	};
	env->RegisterNatives(g_bleManagerClass, nativeMethods,
		sizeof(nativeMethods) / sizeof(nativeMethods[0]));

	return true;
}

Ble::Ble()
	: love::ble::Ble("love.ble.android")
{
	g_bleInstance = this;
	initJNI();
}

Ble::~Ble()
{
	g_bleInstance = nullptr;

	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (env)
	{
		if (g_bleManagerObj)
		{
			env->DeleteGlobalRef(g_bleManagerObj);
			g_bleManagerObj = nullptr;
		}
		if (g_bleManagerClass)
		{
			env->DeleteGlobalRef(g_bleManagerClass);
			g_bleManagerClass = nullptr;
		}
	}
}

Ble::RadioState Ble::getState()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_getRadioState)
		return RADIO_UNSUPPORTED;

	jstring jstate = (jstring)env->CallObjectMethod(g_bleManagerObj, g_getRadioState);
	std::string state = jstringToStd(env, jstate);
	if (jstate) env->DeleteLocalRef(jstate);

	RadioState rs;
	if (getConstant(state.c_str(), rs))
		return rs;
	return RADIO_UNSUPPORTED;
}

void Ble::host(const std::string &roomName, int maxClients, Transport transport)
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_host) return;

	const char *transportStr = (transport == TRANSPORT_RESILIENT) ? "resilient" : "reliable";
	jstring jroom = env->NewStringUTF(roomName.c_str());
	jstring jtransport = env->NewStringUTF(transportStr);
	env->CallVoidMethod(g_bleManagerObj, g_host, jroom, maxClients, jtransport);
	env->DeleteLocalRef(jroom);
	env->DeleteLocalRef(jtransport);
}

void Ble::scan()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_scan) return;
	env->CallVoidMethod(g_bleManagerObj, g_scan);
}

void Ble::join(const std::string &roomId)
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_join) return;

	jstring jroomId = env->NewStringUTF(roomId.c_str());
	env->CallVoidMethod(g_bleManagerObj, g_join, jroomId);
	env->DeleteLocalRef(jroomId);
}

void Ble::leave()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_leave) return;
	env->CallVoidMethod(g_bleManagerObj, g_leave);
}

void Ble::broadcast(const std::string &msgType, const std::vector<uint8_t> &payload)
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_broadcast) return;

	jstring jmsgType = env->NewStringUTF(msgType.c_str());
	jbyteArray jpayload = env->NewByteArray((jsize)payload.size());
	env->SetByteArrayRegion(jpayload, 0, (jsize)payload.size(), (const jbyte *)payload.data());
	env->CallVoidMethod(g_bleManagerObj, g_broadcast, jmsgType, jpayload);
	env->DeleteLocalRef(jmsgType);
	env->DeleteLocalRef(jpayload);
}

void Ble::send(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload)
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_send) return;

	jstring jpeerId = env->NewStringUTF(peerId.c_str());
	jstring jmsgType = env->NewStringUTF(msgType.c_str());
	jbyteArray jpayload = env->NewByteArray((jsize)payload.size());
	env->SetByteArrayRegion(jpayload, 0, (jsize)payload.size(), (const jbyte *)payload.data());
	env->CallVoidMethod(g_bleManagerObj, g_send, jpeerId, jmsgType, jpayload);
	env->DeleteLocalRef(jpeerId);
	env->DeleteLocalRef(jmsgType);
	env->DeleteLocalRef(jpayload);
}

std::string Ble::getLocalId()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_getLocalId) return "";
	jstring jid = (jstring)env->CallObjectMethod(g_bleManagerObj, g_getLocalId);
	std::string id = jstringToStd(env, jid);
	if (jid) env->DeleteLocalRef(jid);
	return id;
}

bool Ble::isHost()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_isHosting) return false;
	return env->CallBooleanMethod(g_bleManagerObj, g_isHosting);
}

std::vector<Ble::PeerInfo> Ble::getPeers()
{
	std::vector<PeerInfo> peers;

	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_getPeersString)
		return peers;

	// getPeersString returns pipe-delimited: "peerId:isHost:status|peerId:isHost:status|..."
	jstring jpeers = (jstring)env->CallObjectMethod(g_bleManagerObj, g_getPeersString);
	std::string peersStr = jstringToStd(env, jpeers);
	if (jpeers) env->DeleteLocalRef(jpeers);

	if (peersStr.empty())
		return peers;

	// Parse pipe-delimited entries
	size_t pos = 0;
	while (pos < peersStr.size())
	{
		size_t pipePos = peersStr.find('|', pos);
		std::string entry = (pipePos == std::string::npos)
			? peersStr.substr(pos)
			: peersStr.substr(pos, pipePos - pos);
		pos = (pipePos == std::string::npos) ? peersStr.size() : pipePos + 1;

		if (entry.empty()) continue;

		// Parse "peerId:isHost:status"
		size_t c1 = entry.find(':');
		size_t c2 = (c1 != std::string::npos) ? entry.find(':', c1 + 1) : std::string::npos;
		if (c1 == std::string::npos || c2 == std::string::npos) continue;

		PeerInfo info;
		info.peerId = entry.substr(0, c1);
		info.isHost = (entry.substr(c1 + 1, c2 - c1 - 1) == "1");
		info.status = entry.substr(c2 + 1);
		peers.push_back(info);
	}

	return peers;
}

std::string Ble::getAddress()
{
	JNIEnv *env = (JNIEnv *)SDL_GetAndroidJNIEnv();
	if (!env || !g_bleManagerObj || !g_getAddress) return "";
	jstring jaddr = (jstring)env->CallObjectMethod(g_bleManagerObj, g_getAddress);
	std::string addr = jstringToStd(env, jaddr);
	if (jaddr) env->DeleteLocalRef(jaddr);
	return addr;
}

// ── JNI native callbacks ──

void JNICALL Ble::jniOnHosted(JNIEnv *env, jobject thiz,
	jstring jsessionId, jstring jpeerId, jstring jtransport)
{
	if (!g_bleInstance) return;
	std::string sessionId = jstringToStd(env, jsessionId);
	std::string peerId = jstringToStd(env, jpeerId);
	std::string transport = jstringToStd(env, jtransport);

	BleEvent event;
	event.type = "hosted";
	event.fields["session_id"] = Variant(sessionId.c_str(), sessionId.size());
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	event.fields["transport"] = Variant(transport.c_str(), transport.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnJoined(JNIEnv *env, jobject thiz,
	jstring jsessionId, jstring jroomId, jstring jpeerId, jstring jhostId, jstring jtransport)
{
	if (!g_bleInstance) return;
	std::string sessionId = jstringToStd(env, jsessionId);
	std::string roomId = jstringToStd(env, jroomId);
	std::string peerId = jstringToStd(env, jpeerId);
	std::string hostId = jstringToStd(env, jhostId);
	std::string transport = jstringToStd(env, jtransport);

	BleEvent event;
	event.type = "joined";
	event.fields["session_id"] = Variant(sessionId.c_str(), sessionId.size());
	event.fields["room_id"] = Variant(roomId.c_str(), roomId.size());
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	event.fields["host_id"] = Variant(hostId.c_str(), hostId.size());
	event.fields["transport"] = Variant(transport.c_str(), transport.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnJoinFailed(JNIEnv *env, jobject thiz,
	jstring jreason, jstring jroomId)
{
	if (!g_bleInstance) return;
	std::string reason = jstringToStd(env, jreason);
	std::string roomId = jstringToStd(env, jroomId);

	BleEvent event;
	event.type = "join_failed";
	event.fields["reason"] = Variant(reason.c_str(), reason.size());
	event.fields["room_id"] = Variant(roomId.c_str(), roomId.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnRoomFound(JNIEnv *env, jobject thiz,
	jstring jroomId, jstring jsessionId, jstring jname,
	jstring jtransport, jint peerCount, jint max, jint rssi)
{
	if (!g_bleInstance) return;
	std::string roomId = jstringToStd(env, jroomId);
	std::string sessionId = jstringToStd(env, jsessionId);
	std::string name = jstringToStd(env, jname);
	std::string transport = jstringToStd(env, jtransport);

	BleEvent event;
	event.type = "room_found";
	event.fields["room_id"] = Variant(roomId.c_str(), roomId.size());
	event.fields["session_id"] = Variant(sessionId.c_str(), sessionId.size());
	event.fields["name"] = Variant(name.c_str(), name.size());
	event.fields["transport"] = Variant(transport.c_str(), transport.size());
	event.fields["peer_count"] = Variant((double)peerCount);
	event.fields["max"] = Variant((double)max);
	event.fields["rssi"] = Variant((double)rssi);
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnRoomLost(JNIEnv *env, jobject thiz, jstring jroomId)
{
	if (!g_bleInstance) return;
	std::string roomId = jstringToStd(env, jroomId);

	BleEvent event;
	event.type = "room_lost";
	event.fields["room_id"] = Variant(roomId.c_str(), roomId.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnPeerJoined(JNIEnv *env, jobject thiz, jstring jpeerId)
{
	if (!g_bleInstance) return;
	std::string peerId = jstringToStd(env, jpeerId);

	BleEvent event;
	event.type = "peer_joined";
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnPeerLeft(JNIEnv *env, jobject thiz,
	jstring jpeerId, jstring jreason)
{
	if (!g_bleInstance) return;
	std::string peerId = jstringToStd(env, jpeerId);
	std::string reason = jstringToStd(env, jreason);

	BleEvent event;
	event.type = "peer_left";
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	event.fields["reason"] = Variant(reason.c_str(), reason.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnPeerStatus(JNIEnv *env, jobject thiz,
	jstring jpeerId, jstring jstatus)
{
	if (!g_bleInstance) return;
	std::string peerId = jstringToStd(env, jpeerId);
	std::string status = jstringToStd(env, jstatus);

	BleEvent event;
	event.type = "peer_status";
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	event.fields["status"] = Variant(status.c_str(), status.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnMessage(JNIEnv *env, jobject thiz,
	jstring jpeerId, jstring jmsgType, jbyteArray jpayload)
{
	if (!g_bleInstance) return;
	std::string peerId = jstringToStd(env, jpeerId);
	std::string msgType = jstringToStd(env, jmsgType);

	// Extract payload bytes
	jsize len = env->GetArrayLength(jpayload);
	std::vector<uint8_t> payload(len);
	env->GetByteArrayRegion(jpayload, 0, len, (jbyte *)payload.data());

	BleEvent event;
	event.type = "message";
	event.fields["peer_id"] = Variant(peerId.c_str(), peerId.size());
	event.fields["msg_type"] = Variant(msgType.c_str(), msgType.size());
	// Store payload as raw string bytes — wrap_Ble.cpp will decode via Codec
	event.fields["payload"] = Variant((const char *)payload.data(), payload.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnSessionMigrating(JNIEnv *env, jobject thiz,
	jstring joldHostId, jstring jnewHostId)
{
	if (!g_bleInstance) return;
	std::string oldHostId = jstringToStd(env, joldHostId);
	std::string newHostId = jstringToStd(env, jnewHostId);

	BleEvent event;
	event.type = "session_migrating";
	event.fields["old_host_id"] = Variant(oldHostId.c_str(), oldHostId.size());
	event.fields["new_host_id"] = Variant(newHostId.c_str(), newHostId.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnSessionResumed(JNIEnv *env, jobject thiz,
	jstring jsessionId, jstring jnewHostId)
{
	if (!g_bleInstance) return;
	std::string sessionId = jstringToStd(env, jsessionId);
	std::string newHostId = jstringToStd(env, jnewHostId);

	BleEvent event;
	event.type = "session_resumed";
	event.fields["session_id"] = Variant(sessionId.c_str(), sessionId.size());
	event.fields["new_host_id"] = Variant(newHostId.c_str(), newHostId.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnSessionEnded(JNIEnv *env, jobject thiz, jstring jreason)
{
	if (!g_bleInstance) return;
	std::string reason = jstringToStd(env, jreason);

	BleEvent event;
	event.type = "session_ended";
	event.fields["reason"] = Variant(reason.c_str(), reason.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnError(JNIEnv *env, jobject thiz,
	jstring jcode, jstring jdetail)
{
	if (!g_bleInstance) return;
	std::string code = jstringToStd(env, jcode);
	std::string detail = jstringToStd(env, jdetail);

	BleEvent event;
	event.type = "error";
	event.fields["code"] = Variant(code.c_str(), code.size());
	event.fields["detail"] = Variant(detail.c_str(), detail.size());
	g_bleInstance->pushEvent(event);
}

void JNICALL Ble::jniOnDiagnostic(JNIEnv *env, jobject thiz, jstring jmessage)
{
	if (!g_bleInstance) return;
	std::string message = jstringToStd(env, jmessage);

	BleEvent event;
	event.type = "diagnostic";
	event.fields["platform"] = Variant("android", 7);
	event.fields["message"] = Variant(message.c_str(), message.size());
	g_bleInstance->pushEvent(event);
}

} // android
} // ble
} // love

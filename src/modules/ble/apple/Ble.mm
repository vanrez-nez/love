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

#import <CoreBluetooth/CoreBluetooth.h>
#import <Foundation/Foundation.h>
#import <UIKit/UIKit.h>

#include <algorithm>
#include <cstring>
#include <vector>
#include <zlib.h>

namespace love
{
namespace ble
{
namespace apple
{

namespace
{

static Ble *instance = nullptr;

static const uint8_t PACKET_VERSION = 1;
static const uint8_t FRAGMENT_VERSION = 1;
static const NSUInteger DEFAULT_FRAGMENT_PAYLOAD_LIMIT = 20;
static const NSUInteger FRAGMENT_HEADER_SIZE = 5;
static const NSUInteger MAX_FRAGMENT_COUNT = 255;
static const NSUInteger MAX_REASSEMBLED_PACKET_SIZE = 64 * 1024;
static const NSTimeInterval ROOM_EXPIRY_SECONDS = 4.0;
static const NSTimeInterval ASSEMBLY_EXPIRY_SECONDS = 15.0;
static NSString *const ROOM_DISCOVERY_PREFIX = @"LB1";
static NSString *const CONTROL_SESSION_MIGRATING = @"session_migrating";
static const NSUInteger ROOM_ID_LENGTH = 6;
static const uint16_t MANUFACTURER_DATA_ID = 0xFFFF;
static const NSTimeInterval MIGRATION_TIMEOUT_SECONDS = 3.0;
static const NSTimeInterval RECONNECT_TIMEOUT_SECONDS = 10.0;

static void bleLog(NSString *message)
{
	NSLog(@"[love-ble-ios] %@", message ?: @"");

	if (instance != nullptr && message != nil)
		instance->onDiagnostic(message.UTF8String != nullptr ? message.UTF8String : "");
}

static NSString *hexPreview(NSData *data, NSUInteger maxBytes = 24)
{
	if (data == nil || data.length == 0)
		return @"<empty>";

	const uint8_t *bytes = (const uint8_t *) data.bytes;
	NSUInteger count = MIN(data.length, maxBytes);
	NSMutableArray<NSString *> *parts = [NSMutableArray arrayWithCapacity:count + 1];
	for (NSUInteger i = 0; i < count; i++)
		[parts addObject:[NSString stringWithFormat:@"%02X", bytes[i]]];

	if (data.length > maxBytes)
		[parts addObject:@"..."];

	return [parts componentsJoinedByString:@" "];
}

struct Packet
{
	std::string kind;
	std::string fromPeerId;
	std::string toPeerId;
	std::string msgType;
	std::vector<uint8_t> payload;
	int messageId = 0;
};

static NSString *toNSString(const std::string &value);
static NSData *toNSData(const std::vector<uint8_t> &value);

static NSString *packetSummary(const std::string &kind, const std::string &fromPeerId, const std::string &toPeerId, const std::string &msgType, const std::vector<uint8_t> &payload)
{
	return [NSString stringWithFormat:@"kind=%@ from=%@ to=%@ type=%@ payload_len=%lu payload_hex=%@",
		toNSString(kind),
		toNSString(fromPeerId),
		toNSString(toPeerId),
		toNSString(msgType),
		(unsigned long) payload.size(),
		hexPreview(toNSData(payload))];
}

static NSString *packetSummary(const Packet &packet)
{
	return packetSummary(packet.kind, packet.fromPeerId, packet.toPeerId, packet.msgType, packet.payload);
}

static NSString *toNSString(const std::string &value)
{
	return [[NSString alloc] initWithBytes:value.data() length:value.size() encoding:NSUTF8StringEncoding];
}

static std::string fromNSString(NSString *value)
{
	if (value == nil)
		return "";

	const char *chars = value.UTF8String;
	return chars != nullptr ? chars : "";
}

static NSData *toNSData(const std::vector<uint8_t> &value)
{
	if (value.empty())
		return [NSData data];

	return [NSData dataWithBytes:value.data() length:value.size()];
}

static std::vector<uint8_t> fromNSData(NSData *value)
{
	std::vector<uint8_t> bytes;
	if (value == nil || value.length == 0)
		return bytes;

	bytes.resize(value.length);
	[value getBytes:bytes.data() length:value.length];
	return bytes;
}

static CBUUID *serviceUUID()
{
	static CBUUID *uuid = [CBUUID UUIDWithString:@"4BDF6B6D-6B77-4B3F-9F4A-5A2D1499D641"];
	return uuid;
}

static CBUUID *messageUUID()
{
	static CBUUID *uuid = [CBUUID UUIDWithString:@"9E153F71-C2D0-4EE1-8B8D-090421BEA607"];
	return uuid;
}

static NSString *transportName(love::ble::Ble::Transport transport)
{
	return transport == love::ble::Ble::TRANSPORT_RESILIENT ? @"resilient" : @"reliable";
}

static NSString *safeString(NSString *value)
{
	return value != nil ? value : @"";
}

static NSString *peripheralKey(CBPeripheral *peripheral)
{
	return peripheral.identifier.UUIDString ?: @"";
}

static NSString *centralKey(CBCentral *central)
{
	return central.identifier.UUIDString ?: @"";
}

static void appendU8(NSMutableData *data, uint8_t value)
{
	[data appendBytes:&value length:1];
}

static void appendU16(NSMutableData *data, uint16_t value)
{
	uint16_t big = CFSwapInt16HostToBig(value);
	[data appendBytes:&big length:sizeof(big)];
}

static void appendU32(NSMutableData *data, uint32_t value)
{
	uint32_t big = CFSwapInt32HostToBig(value);
	[data appendBytes:&big length:sizeof(big)];
}

static void appendString(NSMutableData *data, const std::string &value)
{
	appendU32(data, (uint32_t) value.size());
	if (!value.empty())
		[data appendBytes:value.data() length:value.size()];
}

static void appendBytes(NSMutableData *data, const std::vector<uint8_t> &value)
{
	appendU32(data, (uint32_t) value.size());
	if (!value.empty())
		[data appendBytes:value.data() length:value.size()];
}

static bool readU8(const uint8_t *bytes, size_t length, size_t &offset, uint8_t &out)
{
	if (offset + 1 > length)
		return false;

	out = bytes[offset++];
	return true;
}

static bool readU16(const uint8_t *bytes, size_t length, size_t &offset, uint16_t &out)
{
	if (offset + 2 > length)
		return false;

	uint16_t big = 0;
	memcpy(&big, bytes + offset, sizeof(big));
	out = CFSwapInt16BigToHost(big);
	offset += 2;
	return true;
}

static bool readU32(const uint8_t *bytes, size_t length, size_t &offset, uint32_t &out)
{
	if (offset + 4 > length)
		return false;

	uint32_t big = 0;
	memcpy(&big, bytes + offset, sizeof(big));
	out = CFSwapInt32BigToHost(big);
	offset += 4;
	return true;
}

static bool readString(const uint8_t *bytes, size_t length, size_t &offset, std::string &out)
{
	uint32_t size = 0;
	if (!readU32(bytes, length, offset, size))
		return false;

	if (size > 4096 || offset + size > length)
		return false;

	out.assign((const char *) bytes + offset, size);
	offset += size;
	return true;
}

static bool readBytes(const uint8_t *bytes, size_t length, size_t &offset, std::vector<uint8_t> &out)
{
	uint32_t size = 0;
	if (!readU32(bytes, length, offset, size))
		return false;

	if (size > 65536 || offset + size > length)
		return false;

	out.resize(size);
	if (size > 0)
		memcpy(out.data(), bytes + offset, size);
	offset += size;
	return true;
}

static NSData *encodePacketData(const std::string &kind, const std::string &fromPeerId, const std::string &toPeerId, const std::string &msgType, const std::vector<uint8_t> &payload, int messageId)
{
	NSMutableData *data = [NSMutableData data];
	appendU8(data, PACKET_VERSION);
	appendU16(data, (uint16_t) messageId);
	appendString(data, kind);
	appendString(data, fromPeerId);
	appendString(data, toPeerId);
	appendString(data, msgType);
	appendBytes(data, payload);
	bleLog([NSString stringWithFormat:@"encodePacket raw_len=%lu msgId=%d %@", (unsigned long) data.length, messageId, packetSummary(kind, fromPeerId, toPeerId, msgType, payload)]);
	return data;
}

static bool decodePacketData(NSData *data, Packet &packet)
{
	if (data == nil || data.length == 0)
		return false;

	const uint8_t *bytes = (const uint8_t *) data.bytes;
	size_t length = data.length;
	size_t offset = 0;
	uint8_t version = 0;

	if (!readU8(bytes, length, offset, version) || version != PACKET_VERSION)
		return false;

	uint16_t msgId = 0;
	if (!readU16(bytes, length, offset, msgId))
		return false;
	packet.messageId = (int) msgId;

	BOOL ok = readString(bytes, length, offset, packet.kind)
		&& readString(bytes, length, offset, packet.fromPeerId)
		&& readString(bytes, length, offset, packet.toPeerId)
		&& readString(bytes, length, offset, packet.msgType)
		&& readBytes(bytes, length, offset, packet.payload)
		&& offset == length;
	if (ok)
		bleLog([NSString stringWithFormat:@"decodePacket raw_len=%lu msgId=%d %@", (unsigned long) data.length, packet.messageId, packetSummary(packet)]);
	return ok;
}

static Variant makeStringVariant(const std::string &value)
{
	return Variant(value);
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

} // namespace

} // namespace apple
} // namespace ble
} // namespace love

using namespace love::ble::apple;

@interface LoveBleRoom : NSObject
@property (nonatomic, copy) NSString *roomId;
@property (nonatomic, copy) NSString *sessionId;
@property (nonatomic, copy) NSString *hostPeerId;
@property (nonatomic, copy) NSString *name;
@property (nonatomic, copy) NSString *transport;
@property (nonatomic, assign) NSInteger peerCount;
@property (nonatomic, assign) NSInteger maxClients;
@property (nonatomic, assign) NSInteger rssi;
@property (nonatomic, strong) CBPeripheral *peripheral;
@property (nonatomic, assign) NSTimeInterval lastSeenAt;
@end

@implementation LoveBleRoom
@end

@interface LoveBleAssembly : NSObject
@property (nonatomic, assign) NSInteger fragmentCount;
@property (nonatomic, strong) NSMutableArray *fragments;
@property (nonatomic, assign) NSInteger receivedCount;
@property (nonatomic, assign) NSInteger totalBytes;
@property (nonatomic, assign) NSTimeInterval updatedAt;
@end

@implementation LoveBleAssembly
@end

@interface LoveBleMigrationInfo : NSObject
@property (nonatomic, copy) NSString *oldHostId;
@property (nonatomic, copy) NSString *successorPeerId;
@property (nonatomic, copy) NSString *sessionId;
@property (nonatomic, copy) NSString *roomName;
@property (nonatomic, copy) NSString *transport;
@property (nonatomic, assign) NSInteger maxClients;
@property (nonatomic, assign) NSInteger membershipEpoch;
@property (nonatomic, assign) BOOL becomingHost;
@property (nonatomic, strong) NSMutableSet<NSString *> *excludedSuccessors;
@end

@implementation LoveBleMigrationInfo
- (instancetype)init {
	if ((self = [super init]))
		_excludedSuccessors = [NSMutableSet set];
	return self;
}
@end

@interface LoveBleManager : NSObject <CBCentralManagerDelegate, CBPeripheralDelegate, CBPeripheralManagerDelegate>
- (instancetype)initWithOwner:(love::ble::apple::Ble *)owner;
- (love::ble::Ble::RadioState)radioState;
- (NSString *)generateShortId;
- (void)hostRoom:(const std::string &)room maxClients:(int)maxClients transport:(love::ble::Ble::Transport)transport;
- (void)scanRooms;
- (void)joinRoomId:(const std::string &)roomId;
- (void)leave;
- (BOOL)broadcastMessageType:(const std::string &)msgType payload:(const std::vector<uint8_t> &)payload;
- (BOOL)sendPeerId:(const std::string &)peerId messageType:(const std::string &)msgType payload:(const std::vector<uint8_t> &)payload;
- (void)shutdown;
- (BOOL)beginHostingSession:(NSString *)roomName maxClients:(NSInteger)maxClients transport:(NSString *)transport sessionId:(NSString *)sessionId;
- (void)connectToRoom:(LoveBleRoom *)room migrationJoin:(BOOL)migrationJoin;
- (void)finishLeave:(NSString *)remoteReason;
- (BOOL)beginGracefulMigration;
- (NSString *)selectMigrationSuccessor;
- (NSData *)encodeMigrationPayload:(LoveBleMigrationInfo *)info;
- (LoveBleMigrationInfo *)decodeMigrationPayloadFromHost:(NSString *)oldHostId payload:(NSData *)payload;
- (void)discardAssemblyForKey:(NSString *)assemblyKey sourceKey:(NSString *)sourceKey nonce:(uint16_t)nonce reason:(NSString *)reason emitError:(BOOL)emitError detail:(NSString *)detail;
- (void)startMigration:(LoveBleMigrationInfo *)info;
- (BOOL)hasActiveMigration;
- (BOOL)matchesMigrationRoom:(LoveBleRoom *)room;
- (void)scheduleMigrationTimeout;
- (void)cancelMigrationTimeout;
- (void)cancelPendingMigrationDeparture;
- (void)failMigration;
- (void)beginMigrationReconnect;
- (void)completeMigrationResume;
- (void)completeLocalJoin;
- (void)handleJoinFailure:(NSString *)detail;
- (void)addSessionPeerId:(NSString *)peerId;
- (void)removeSessionPeerId:(NSString *)peerId;
- (void)resetSessionPeerIdsWithHostId:(NSString *)hostId;
- (NSString *)selectRecoverySuccessorExcludingHostId:(NSString *)oldHostId;
- (BOOL)beginUnexpectedHostRecovery;
- (void)migrationTimeoutFired:(NSTimer *)timer;
- (void)migrationDepartureFired:(NSTimer *)timer;
- (BOOL)validateInboundPacketShape:(const Packet &)packet context:(NSString *)context;
- (BOOL)validateInboundPacketFromCentral:(const Packet &)packet centralKey:(NSString *)sourceKey;
- (BOOL)validateInboundPacketFromHost:(const Packet &)packet;
- (BOOL)validateControlPacketPayload:(const Packet &)packet context:(NSString *)context;
- (void)applyReliabilityConfig:(const love::ble::Ble::ReliabilityConfig &)config;
- (void)startHeartbeatTimer;
- (void)stopHeartbeatTimer;
- (void)heartbeatFired:(NSTimer *)timer;
- (BOOL)isDuplicateMessageFrom:(NSString *)fromPeerId msgType:(const std::string &)msgType messageId:(int)messageId;
- (NSData *)processIncomingFragment:(NSString *)sourceKey data:(NSData *)fragmentData outNonce:(uint16_t *)outNonce;
- (NSString *)debugStateString;
@end

struct love::ble::apple::Ble::Impl
{
	LoveBleManager *manager = nil;
};

@implementation LoveBleManager
{
	love::ble::apple::Ble *_owner;
	CBCentralManager *_centralManager;
	CBPeripheralManager *_peripheralManager;
	NSMutableDictionary<NSString *, LoveBleRoom *> *_rooms;
	NSMutableDictionary<NSString *, CBCentral *> *_connectedClients;
	NSMutableDictionary<NSString *, CBCentral *> *_pendingClients;
	NSMutableDictionary<NSString *, CBCentral *> *_centralsByKey;
	NSMutableDictionary<NSString *, NSString *> *_centralPeerIds;
	NSMutableDictionary<NSString *, NSNumber *> *_centralPayloadLimits;
	NSMutableDictionary<NSString *, NSMutableArray<NSData *> *> *_notificationQueues;
	NSMutableDictionary<NSString *, LoveBleAssembly *> *_inboundAssemblies;
	NSMutableArray<NSData *> *_clientWriteQueue;
	NSMutableSet<NSString *> *_sessionPeerIds;
	NSTimer *_roomExpiryTimer;
	CBMutableCharacteristic *_hostCharacteristic;
	CBPeripheral *_clientPeripheral;
	CBCharacteristic *_clientCharacteristic;
	NSString *_sessionId;
	NSString *_roomName;
	NSString *_transport;
	NSString *_localPeerId;
	NSString *_joinedRoomId;
	NSString *_joinedSessionId;
	NSString *_joinedRoomName;
	NSString *_hostPeerId;
	NSInteger _maxClients;
	NSInteger _joinedMaxClients;
	BOOL _hosting;
	BOOL _scanning;
	BOOL _clientLeaving;
	BOOL _clientJoined;
	BOOL _hostAnnounced;
	BOOL _hostServiceReady;
	BOOL _clientWriteInFlight;
	BOOL _clientPendingHelloAck;
	NSUInteger _clientPayloadLimit;
	uint16_t _nextMessageNonce;
	uint16_t _nextMessageId;
	LoveBleMigrationInfo *_migration;
	NSTimer *_migrationTimeoutTimer;
	NSTimer *_migrationDepartureTimer;
	BOOL _migrationJoinInProgress;
	BOOL _migrationDepartureInProgress;
	NSTimeInterval _reliabilityHeartbeatInterval;
	NSInteger _reliabilityFragmentSpacingMs;
	NSInteger _reliabilityDedupWindow;
	NSTimer *_heartbeatTimer;
	NSData *_lastBroadcastPacketData;
	NSMutableArray *_dedupEntries;
	NSMutableSet<NSString *> *_dedupLookup;
	BOOL _fragmentPacingInFlight;

	// Roster state
	NSMutableDictionary<NSString *, NSString *> *_rosterStatus; // peerId -> "connected"/"reconnecting"
	NSInteger _membershipEpoch;
	NSInteger _clientLocalEpoch;
	NSTimeInterval _lastRosterRequestTime;

	// Pending client timestamps
	NSMutableDictionary<NSString *, NSDate *> *_pendingClientTimestamps;

	// Client-side reconnect state
	BOOL _reconnectScanInProgress;
	BOOL _reconnectJoinInProgress;
	NSString *_reconnectSessionId;
	NSString *_reconnectHostPeerId;
	NSTimer *_reconnectTimer;

	// Host-side reconnect grace state
	NSMutableDictionary<NSString *, NSTimer *> *_peerReconnectTimers;

	// Metrics counters
	NSInteger _metricMsgOut;
	NSInteger _metricMsgIn;
	NSInteger _metricCtrlOut;
	NSInteger _metricCtrlIn;
	NSInteger _metricHeartbeatTx;
	NSInteger _metricHeartbeatRx;
	NSInteger _metricDedupHit;
	NSInteger _metricFragmentTx;
	NSInteger _metricFragmentRx;
	NSInteger _metricFragmentDrop;
	NSInteger _metricAssemblyTimeout;
	NSInteger _metricWriteFail;
	NSInteger _metricReconnectAttempt;
	NSInteger _metricReconnectSuccess;
	NSInteger _metricReconnectFail;
	NSInteger _metricGraceStart;
	NSInteger _metricGraceExpire;
	NSInteger _metricGraceResume;
	NSInteger _metricRosterRequest;
	NSInteger _metricJoinReject;
}

- (instancetype)initWithOwner:(love::ble::apple::Ble *)owner
{
	if ((self = [super init]))
	{
		_owner = owner;
		_centralManager = [[CBCentralManager alloc] initWithDelegate:self queue:dispatch_get_main_queue()];
		_peripheralManager = [[CBPeripheralManager alloc] initWithDelegate:self queue:dispatch_get_main_queue()];
		_rooms = [NSMutableDictionary dictionary];
		_connectedClients = [NSMutableDictionary dictionary];
		_pendingClients = [NSMutableDictionary dictionary];
		_centralsByKey = [NSMutableDictionary dictionary];
		_centralPeerIds = [NSMutableDictionary dictionary];
		_centralPayloadLimits = [NSMutableDictionary dictionary];
		_notificationQueues = [NSMutableDictionary dictionary];
		_inboundAssemblies = [NSMutableDictionary dictionary];
		_clientWriteQueue = [NSMutableArray array];
		_sessionPeerIds = [NSMutableSet set];
		_transport = @"reliable";
		_localPeerId = [[[NSUUID UUID] UUIDString] stringByReplacingOccurrencesOfString:@"-" withString:@""];
		if (_localPeerId.length > 6)
			_localPeerId = [_localPeerId substringToIndex:6];
		_clientPayloadLimit = DEFAULT_FRAGMENT_PAYLOAD_LIMIT;
		_nextMessageNonce = 1;
		_nextMessageId = 1;
		_dedupEntries = [NSMutableArray array];
		_dedupLookup = [NSMutableSet set];
		_reliabilityHeartbeatInterval = 2.0;
		_reliabilityFragmentSpacingMs = 15;
		_reliabilityDedupWindow = 64;
		_rosterStatus = [NSMutableDictionary dictionary];
		_pendingClientTimestamps = [NSMutableDictionary dictionary];
		_reconnectSessionId = @"";
		_reconnectHostPeerId = @"";
		_peerReconnectTimers = [NSMutableDictionary dictionary];
	}

	return self;
}

- (void)addSessionPeerId:(NSString *)peerId
{
	NSString *safePeerId = safeString(peerId);
	if (safePeerId.length > 0)
		[_sessionPeerIds addObject:safePeerId];
}

- (void)removeSessionPeerId:(NSString *)peerId
{
	NSString *safePeerId = safeString(peerId);
	if (safePeerId.length > 0)
		[_sessionPeerIds removeObject:safePeerId];
}

- (void)resetSessionPeerIdsWithHostId:(NSString *)hostId
{
	[_sessionPeerIds removeAllObjects];
	[self addSessionPeerId:_localPeerId];
	[self addSessionPeerId:hostId];
}

- (NSString *)selectRecoverySuccessorExcludingHostId:(NSString *)oldHostId
{
	return [self selectRecoverySuccessorExcludingHostId:oldHostId excluded:[NSSet set]];
}

- (NSString *)selectRecoverySuccessorExcludingHostId:(NSString *)oldHostId excluded:(NSSet<NSString *> *)excluded
{
	NSMutableArray<NSString *> *peerIds = [NSMutableArray array];
	for (NSString *peerId in _sessionPeerIds)
	{
		NSString *safePeerId = safeString(peerId);
		if (safePeerId.length > 0
			&& ![safePeerId isEqualToString:safeString(oldHostId)]
			&& ![self isPeerInReconnectGrace:safePeerId]
			&& ![excluded containsObject:safePeerId])
			[peerIds addObject:safePeerId];
	}

	[peerIds sortUsingSelector:@selector(compare:)];
	return peerIds.count > 0 ? peerIds.firstObject : @"";
}

- (void)shutdown
{
	[self stopHeartbeatTimer];
	[self cancelReconnectTimeout];
	[self cancelAllPeerReconnectGraces];
	[self leave];
	_owner = nullptr;
	_centralManager.delegate = nil;
	_peripheralManager.delegate = nil;
}

- (love::ble::Ble::RadioState)radioState
{
	if (@available(iOS 13.1, *))
	{
		CBManagerAuthorization auth = [CBCentralManager authorization];
		if (auth == CBManagerAuthorizationDenied || auth == CBManagerAuthorizationRestricted)
			return love::ble::Ble::RADIO_UNAUTHORIZED;
	}

	switch (_centralManager.state)
	{
	case CBManagerStatePoweredOn:
		return love::ble::Ble::RADIO_ON;
	case CBManagerStateUnsupported:
		return love::ble::Ble::RADIO_UNSUPPORTED;
	case CBManagerStateUnauthorized:
		return love::ble::Ble::RADIO_UNAUTHORIZED;
	case CBManagerStatePoweredOff:
	case CBManagerStateResetting:
	case CBManagerStateUnknown:
	default:
		return love::ble::Ble::RADIO_OFF;
	}
}

- (NSString *)generateShortId
{
	NSString *value = [[[NSUUID UUID] UUIDString] stringByReplacingOccurrencesOfString:@"-" withString:@""];
	if (value.length > 6)
		return [value substringToIndex:6];

	return value;
}

- (void)startRoomExpiryTimer
{
	if (_roomExpiryTimer != nil)
		return;

	_roomExpiryTimer = [NSTimer scheduledTimerWithTimeInterval:1.0
		target:self
		selector:@selector(pruneRoomsTimer:)
		userInfo:nil
		repeats:YES];
}

- (void)stopRoomExpiryTimer
{
	[_roomExpiryTimer invalidate];
	_roomExpiryTimer = nil;
}

- (void)pruneRoomsTimer:(NSTimer *)timer
{
	#pragma unused(timer)
	NSTimeInterval now = [NSDate timeIntervalSinceReferenceDate];
	NSMutableArray<NSString *> *expired = [NSMutableArray array];

	for (NSString *roomId in _rooms)
	{
		LoveBleRoom *room = _rooms[roomId];
		if (room.lastSeenAt + ROOM_EXPIRY_SECONDS < now)
			[expired addObject:roomId];
	}

	for (NSString *roomId in expired)
	{
		[_rooms removeObjectForKey:roomId];
		if (_owner != nullptr)
			_owner->onRoomLost(fromNSString(roomId));
	}
}

- (NSString *)encodeRoomLocalName
{
	NSString *payload = [NSString stringWithFormat:@"%@%@%@%@%ld%lu%@",
		ROOM_DISCOVERY_PREFIX,
		safeString(_sessionId),
		safeString(_localPeerId),
		[_transport isEqualToString:@"resilient"] ? @"s" : @"r",
		(long) _maxClients,
		(unsigned long) _connectedClients.count,
		safeString(_roomName)];
	bleLog([NSString stringWithFormat:@"encodeRoom payload=%@", payload]);
	return payload;
}

- (LoveBleRoom *)decodeRoomFromLocalName:(NSString *)localName peripheral:(CBPeripheral *)peripheral RSSI:(NSNumber *)RSSI
{
	if (localName == nil || ![localName hasPrefix:ROOM_DISCOVERY_PREFIX])
		return nil;

	NSUInteger minimumLength = ROOM_DISCOVERY_PREFIX.length + ROOM_ID_LENGTH + ROOM_ID_LENGTH + 3;
	if (localName.length < minimumLength)
		return nil;

	NSUInteger offset = ROOM_DISCOVERY_PREFIX.length;
	NSString *sessionId = [localName substringWithRange:NSMakeRange(offset, ROOM_ID_LENGTH)];
	offset += ROOM_ID_LENGTH;

	NSString *hostPeerId = [localName substringWithRange:NSMakeRange(offset, ROOM_ID_LENGTH)];
	offset += ROOM_ID_LENGTH;

	NSString *transportCode = [localName substringWithRange:NSMakeRange(offset, 1)];
	offset += 1;

	NSString *maxString = [localName substringWithRange:NSMakeRange(offset, 1)];
	offset += 1;

	NSString *peerCountString = [localName substringWithRange:NSMakeRange(offset, 1)];
	offset += 1;

	NSString *roomName = offset < localName.length ? [localName substringFromIndex:offset] : @"";

	LoveBleRoom *room = [LoveBleRoom new];
	room.roomId = peripheralKey(peripheral);
	room.sessionId = safeString(sessionId);
	room.hostPeerId = safeString(hostPeerId);
	room.transport = [transportCode isEqualToString:@"s"] ? @"resilient" : @"reliable";
	room.maxClients = MAX(1, MIN(7, safeString(maxString).integerValue));
	room.peerCount = MAX(0, safeString(peerCountString).integerValue);
	room.name = safeString(roomName);
	room.rssi = RSSI.integerValue;
	room.peripheral = peripheral;
	room.lastSeenAt = [NSDate timeIntervalSinceReferenceDate];
	bleLog([NSString stringWithFormat:@"decodeRoom local_name room=%@ session=%@ host=%@ transport=%@ peers=%ld/%ld rssi=%ld name=%@",
		room.roomId,
		room.sessionId,
		room.hostPeerId,
		room.transport,
		(long) room.peerCount,
		(long) room.maxClients,
		(long) room.rssi,
		room.name]);
	return room;
}

- (LoveBleRoom *)decodeRoomFromPeripheral:(CBPeripheral *)peripheral advertisementData:(NSDictionary<NSString *, id> *)advertisementData RSSI:(NSNumber *)RSSI
{
	NSData *manufacturerData = advertisementData[CBAdvertisementDataManufacturerDataKey];
	if (manufacturerData.length > 2)
	{
		const uint8_t *bytes = (const uint8_t *) manufacturerData.bytes;
		uint16_t companyId = (uint16_t) bytes[0] | ((uint16_t) bytes[1] << 8);
		if (companyId == MANUFACTURER_DATA_ID)
		{
			NSData *payloadData = [manufacturerData subdataWithRange:NSMakeRange(2, manufacturerData.length - 2)];
			NSString *payload = [[NSString alloc] initWithData:payloadData encoding:NSUTF8StringEncoding];
			bleLog([NSString stringWithFormat:@"scan manufacturer company=0x%04X payload=%@ hex=%@", companyId, safeString(payload), hexPreview(payloadData)]);
			if ([payload hasPrefix:ROOM_DISCOVERY_PREFIX])
				return [self decodeRoomFromLocalName:payload peripheral:peripheral RSSI:RSSI];
		}
	}

	NSDictionary<CBUUID *, NSData *> *serviceData = advertisementData[CBAdvertisementDataServiceDataKey];
	NSData *payloadData = serviceData[serviceUUID()];
	if (payloadData.length > 0)
		bleLog([NSString stringWithFormat:@"scan service_data hex=%@", hexPreview(payloadData)]);
	if (payloadData == nil || payloadData.length == 0)
	{
		NSString *localName = advertisementData[CBAdvertisementDataLocalNameKey];
		bleLog([NSString stringWithFormat:@"scan local_name payload=%@", safeString(localName)]);
		return [self decodeRoomFromLocalName:localName peripheral:peripheral RSSI:RSSI];
	}

	NSString *payload = [[NSString alloc] initWithData:payloadData encoding:NSUTF8StringEncoding];
	NSArray<NSString *> *parts = [payload componentsSeparatedByString:@"|"];
	if (parts.count < 6)
		return nil;

	LoveBleRoom *room = [LoveBleRoom new];
	room.roomId = peripheralKey(peripheral);
	room.sessionId = safeString(parts[0]);
	room.hostPeerId = safeString(parts[1]);
	room.transport = [parts[2] isEqualToString:@"s"] ? @"resilient" : @"reliable";
	room.maxClients = MAX(1, MIN(7, safeString(parts[3]).integerValue));
	room.peerCount = MAX(0, safeString(parts[4]).integerValue);
	room.name = safeString(parts[5]);
	room.rssi = RSSI.integerValue;
	room.peripheral = peripheral;
	room.lastSeenAt = [NSDate timeIntervalSinceReferenceDate];
	bleLog([NSString stringWithFormat:@"decodeRoom compact room=%@ session=%@ host=%@ transport=%@ peers=%ld/%ld rssi=%ld name=%@",
		room.roomId,
		room.sessionId,
		room.hostPeerId,
		room.transport,
		(long) room.peerCount,
		(long) room.maxClients,
		(long) room.rssi,
		room.name]);
	return room;
}

- (NSUInteger)nextNonce
{
	uint16_t nonce = _nextMessageNonce++;
	if (_nextMessageNonce == 0)
		_nextMessageNonce = 1;
	return nonce;
}

- (int)nextMessageId
{
	uint16_t mid = _nextMessageId++;
	if (_nextMessageId == 0)
		_nextMessageId = 1;
	return (int) mid;
}

- (void)pruneAssemblies
{
	NSTimeInterval now = [NSDate timeIntervalSinceReferenceDate];
	NSMutableArray<NSString *> *expired = [NSMutableArray array];
	for (NSString *key in _inboundAssemblies)
	{
		LoveBleAssembly *assembly = _inboundAssemblies[key];
		if (assembly.updatedAt + ASSEMBLY_EXPIRY_SECONDS < now)
			[expired addObject:key];
	}

	for (NSString *key in expired)
	{
		_metricAssemblyTimeout++;
		NSArray<NSString *> *parts = [key componentsSeparatedByString:@":"];
		NSString *sourceKey = parts.count > 0 ? parts[0] : @"";
		uint16_t nonce = 0;
		if (parts.count > 1)
			nonce = (uint16_t) parts[1].intValue;
		[self discardAssemblyForKey:key
			sourceKey:sourceKey
			nonce:nonce
			reason:@"stale_timeout"
			emitError:NO
			detail:nil];
	}
}

- (NSArray<NSData *> *)fragmentPacketData:(NSData *)packetData payloadLimit:(NSUInteger)payloadLimit
{
	NSUInteger chunkSize = payloadLimit > FRAGMENT_HEADER_SIZE ? payloadLimit - FRAGMENT_HEADER_SIZE : 0;
	if (chunkSize == 0)
	{
		if (_owner != nullptr)
			_owner->onError("send_failed", "BLE payload limit is too small for transport framing.");
		return nil;
	}

	NSUInteger totalLength = packetData.length;
	NSUInteger fragmentCount = totalLength == 0 ? 1 : (totalLength + chunkSize - 1) / chunkSize;
	if (fragmentCount > MAX_FRAGMENT_COUNT)
	{
		if (_owner != nullptr)
			_owner->onError("payload_too_large", "BLE payload exceeds the current transport limit.");
		return nil;
	}

	uint16_t nonce = (uint16_t) [self nextNonce];
	NSMutableArray<NSData *> *fragments = [NSMutableArray arrayWithCapacity:fragmentCount];
	const uint8_t *packetBytes = (const uint8_t *) packetData.bytes;

	for (NSUInteger index = 0; index < fragmentCount; index++)
	{
		NSUInteger start = index * chunkSize;
		NSUInteger len = totalLength > start ? MIN(chunkSize, totalLength - start) : 0;
		NSMutableData *fragment = [NSMutableData dataWithCapacity:FRAGMENT_HEADER_SIZE + len];
		uint8_t header[FRAGMENT_HEADER_SIZE] = {
			FRAGMENT_VERSION,
			(uint8_t) ((nonce >> 8) & 0xFF),
			(uint8_t) (nonce & 0xFF),
			(uint8_t) index,
			(uint8_t) fragmentCount,
		};
		[fragment appendBytes:header length:sizeof(header)];
		if (len > 0)
			[fragment appendBytes:packetBytes + start length:len];
		[fragments addObject:fragment];
	}

	_metricFragmentTx += fragmentCount;
	bleLog([NSString stringWithFormat:@"fragmentPacket payload_limit=%lu raw_len=%lu nonce=%u fragments=%lu first_fragment=%@",
		(unsigned long) payloadLimit,
		(unsigned long) packetData.length,
		(unsigned int) nonce,
		(unsigned long) fragmentCount,
		fragments.count > 0 ? hexPreview(fragments.firstObject) : @"<none>"]);
	return fragments;
}

- (NSData *)processIncomingFragment:(NSString *)sourceKey data:(NSData *)fragmentData outNonce:(uint16_t *)outNonce
{
	if (outNonce) *outNonce = 0;
	if (fragmentData == nil || fragmentData.length < FRAGMENT_HEADER_SIZE)
		return nil;

	[self pruneAssemblies];
	_metricFragmentRx++;

	const uint8_t *bytes = (const uint8_t *) fragmentData.bytes;
	uint8_t version = bytes[0];
	uint16_t nonce = (uint16_t) (((uint16_t) bytes[1] << 8) | bytes[2]);
	NSUInteger index = bytes[3];
	NSUInteger count = bytes[4];
	bleLog([NSString stringWithFormat:@"incomingFragment source=%@ nonce=%u index=%lu/%lu raw_len=%lu hex=%@",
		safeString(sourceKey),
		(unsigned int) nonce,
		(unsigned long) index,
		(unsigned long) count,
		(unsigned long) fragmentData.length,
		hexPreview(fragmentData)]);

	if (version != FRAGMENT_VERSION)
	{
		_metricFragmentDrop++;
		return nil;
	}

	if (count == 0 || index >= count)
	{
		_metricFragmentDrop++;
		return nil;
	}

	NSData *chunk = [fragmentData subdataWithRange:NSMakeRange(FRAGMENT_HEADER_SIZE, fragmentData.length - FRAGMENT_HEADER_SIZE)];
	if (count == 1)
	{
		NSString *singleKey = [NSString stringWithFormat:@"%@:%hu", safeString(sourceKey), nonce];
		if (_inboundAssemblies[singleKey] != nil)
			[self discardAssemblyForKey:singleKey sourceKey:sourceKey nonce:nonce reason:@"single_fragment_replaced_partial" emitError:NO detail:nil];
		if (outNonce) *outNonce = nonce;
		return chunk;
	}

	NSString *assemblyKey = [NSString stringWithFormat:@"%@:%hu", safeString(sourceKey), nonce];
	LoveBleAssembly *assembly = _inboundAssemblies[assemblyKey];
	if (assembly == nil)
	{
		// Enforce per-source assembly limit of 32
		NSString *sourcePrefix = [NSString stringWithFormat:@"%@:", safeString(sourceKey)];
		NSInteger sourceCount = 0;
		NSString *oldestKey = nil;
		NSTimeInterval oldestTime = DBL_MAX;
		for (NSString *key in _inboundAssemblies)
		{
			if ([key hasPrefix:sourcePrefix])
			{
				sourceCount++;
				NSTimeInterval t = _inboundAssemblies[key].updatedAt;
				if (t < oldestTime)
				{
					oldestTime = t;
					oldestKey = key;
				}
			}
		}
		if (sourceCount >= 32 && oldestKey != nil)
			[self discardAssemblyForKey:oldestKey sourceKey:sourceKey nonce:0 reason:@"max_assemblies_exceeded" emitError:NO detail:nil];

		assembly = [LoveBleAssembly new];
		assembly.fragmentCount = count;
		assembly.fragments = [NSMutableArray arrayWithCapacity:count];
		for (NSUInteger i = 0; i < count; i++)
			[assembly.fragments addObject:[NSNull null]];
		_inboundAssemblies[assemblyKey] = assembly;
	}
	else if (assembly.fragmentCount != (NSInteger) count)
	{
		[self discardAssemblyForKey:assemblyKey
			sourceKey:sourceKey
			nonce:nonce
			reason:@"fragment_count_mismatch"
			emitError:YES
			detail:@"Received BLE transport fragments with mismatched counts."];
		return nil;
	}

	assembly.updatedAt = [NSDate timeIntervalSinceReferenceDate];
	if (assembly.fragments[index] == (id) [NSNull null])
	{
		assembly.fragments[index] = chunk;
		assembly.receivedCount += 1;
		assembly.totalBytes += chunk.length;
	}
	else
	{
		NSData *existing = assembly.fragments[index];
		if (![existing isEqualToData:chunk])
		{
			[self discardAssemblyForKey:assemblyKey
				sourceKey:sourceKey
				nonce:nonce
				reason:@"conflicting_duplicate_fragment"
				emitError:YES
				detail:@"Received BLE transport fragments with conflicting duplicate data."];
			return nil;
		}

		bleLog([NSString stringWithFormat:@"duplicateFragment source=%@ nonce=%u index=%lu/%lu action=ignored",
			safeString(sourceKey),
			(unsigned int) nonce,
			(unsigned long) index,
			(unsigned long) count]);
	}

	if ((NSUInteger) assembly.totalBytes > MAX_REASSEMBLED_PACKET_SIZE)
	{
		[self discardAssemblyForKey:assemblyKey
			sourceKey:sourceKey
			nonce:nonce
			reason:@"assembly_overflow"
			emitError:YES
			detail:@"Received BLE payload exceeds the supported transport limit."];
		return nil;
	}

	if (assembly.receivedCount < assembly.fragmentCount)
		return nil;

	NSMutableData *packetData = [NSMutableData dataWithCapacity:assembly.totalBytes];
	for (NSUInteger i = 0; i < (NSUInteger) assembly.fragmentCount; i++)
	{
		NSData *part = assembly.fragments[i];
		if (![part isKindOfClass:[NSData class]])
		{
			[self discardAssemblyForKey:assemblyKey
				sourceKey:sourceKey
				nonce:nonce
				reason:@"incomplete_reassembly"
				emitError:YES
				detail:@"Received incomplete BLE transport payload."];
			return nil;
		}

		[packetData appendData:part];
	}

	[_inboundAssemblies removeObjectForKey:assemblyKey];
	bleLog([NSString stringWithFormat:@"reassembledPacket source=%@ nonce=%u raw_len=%lu hex=%@",
		safeString(sourceKey),
		(unsigned int) nonce,
		(unsigned long) packetData.length,
		hexPreview(packetData)]);
	if (outNonce) *outNonce = nonce;
	return packetData;
}

- (void)discardAssemblyForKey:(NSString *)assemblyKey sourceKey:(NSString *)sourceKey nonce:(uint16_t)nonce reason:(NSString *)reason emitError:(BOOL)emitError detail:(NSString *)detail
{
	LoveBleAssembly *assembly = _inboundAssemblies[assemblyKey];
	if (assembly == nil)
		return;

	bleLog([NSString stringWithFormat:@"resetAssembly source=%@ nonce=%u reason=%@ received=%ld/%ld bytes=%ld",
		safeString(sourceKey),
		(unsigned int) nonce,
		safeString(reason),
		(long) assembly.receivedCount,
		(long) assembly.fragmentCount,
		(long) assembly.totalBytes]);
	[_inboundAssemblies removeObjectForKey:assemblyKey];

	if (!emitError || _owner == nullptr)
		return;

	std::string errorCode = [safeString(detail) containsString:@"payload exceeds"] ? "payload_too_large" : "invalid_payload";
	_owner->onError(errorCode, fromNSString(safeString(detail)));
}

- (BOOL)pumpClientWriteQueue
{
	if (_clientWriteInFlight)
		return YES;

	if (_clientPeripheral == nil || _clientCharacteristic == nil)
		return NO;

	NSData *fragment = _clientWriteQueue.firstObject;
	if (fragment == nil)
		return YES;

	[_clientPeripheral writeValue:fragment forCharacteristic:_clientCharacteristic type:CBCharacteristicWriteWithResponse];
	_clientWriteInFlight = YES;
	return YES;
}

- (BOOL)validateInboundPacketShape:(const Packet &)packet context:(NSString *)context
{
	NSString *kind = safeString(toNSString(packet.kind));
	NSString *msgType = safeString(toNSString(packet.msgType));
	NSString *fromPeerId = safeString(toNSString(packet.fromPeerId));

	if (kind.length == 0)
	{
		bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=missing_kind", safeString(context)]);
		return NO;
	}

	if (msgType.length == 0)
	{
		bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=missing_type kind=%@", safeString(context), kind]);
		return NO;
	}

	if ([kind isEqualToString:@"data"] && fromPeerId.length == 0)
	{
		bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=missing_sender kind=%@ type=%@", safeString(context), kind, msgType]);
		return NO;
	}

	return YES;
}

- (BOOL)validateControlPacketPayload:(const Packet &)packet context:(NSString *)context
{
	NSString *msgType = safeString(toNSString(packet.msgType));

	if ([msgType isEqualToString:@"peer_joined"] || [msgType isEqualToString:@"hello_ack"])
	{
		if (!packet.payload.empty())
		{
			bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=unexpected_payload type=%@", safeString(context), msgType]);
			return NO;
		}
	}
	else if ([msgType isEqualToString:CONTROL_SESSION_MIGRATING])
	{
		if (packet.payload.empty())
		{
			bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=missing_payload type=%@", safeString(context), msgType]);
			return NO;
		}

		LoveBleMigrationInfo *info = [self decodeMigrationPayloadFromHost:toNSString(packet.fromPeerId) payload:toNSData(packet.payload)];
		if (info == nil || info.sessionId.length == 0 || info.successorPeerId.length == 0)
		{
			bleLog([NSString stringWithFormat:@"reject packet context=%@ reason=invalid_migration_payload type=%@", safeString(context), msgType]);
			return NO;
		}
	}

	return YES;
}

- (BOOL)validateInboundPacketFromCentral:(const Packet &)packet centralKey:(NSString *)sourceKey
{
	if (![self validateInboundPacketShape:packet context:[NSString stringWithFormat:@"central:%@", safeString(sourceKey)]])
		return NO;

	NSString *kind = safeString(toNSString(packet.kind));
	NSString *msgType = safeString(toNSString(packet.msgType));
	NSString *fromPeerId = safeString(toNSString(packet.fromPeerId));
	NSString *toPeerId = safeString(toNSString(packet.toPeerId));
	NSString *boundPeerId = _centralPeerIds[sourceKey];

	if ([kind isEqualToString:@"control"] && [msgType isEqualToString:@"hello"])
	{
		if (![self validateControlPacketPayload:packet context:[NSString stringWithFormat:@"central:%@", safeString(sourceKey)]])
			return NO;

		if (fromPeerId.length == 0)
		{
			bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=hello_missing_peer", safeString(sourceKey)]);
			return NO;
		}

		if (toPeerId.length > 0 && ![toPeerId isEqualToString:safeString(_localPeerId)])
		{
			bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=hello_wrong_target target=%@ local=%@",
				safeString(sourceKey),
				toPeerId,
				safeString(_localPeerId)]);
			return NO;
		}

		if (boundPeerId != nil && ![boundPeerId isEqualToString:fromPeerId])
		{
			bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=sender_spoof claimed=%@ bound=%@",
				safeString(sourceKey),
				fromPeerId,
				boundPeerId]);
			return NO;
		}

		CBCentral *existingCentral = _connectedClients[fromPeerId];
		if (existingCentral != nil && ![centralKey(existingCentral) isEqualToString:safeString(sourceKey)])
		{
			bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=peer_already_bound peer=%@ existing=%@",
				safeString(sourceKey),
				fromPeerId,
				centralKey(existingCentral)]);
			return NO;
		}

		return YES;
	}

	if (boundPeerId == nil)
	{
		bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=packet_before_hello type=%@",
			safeString(sourceKey),
			msgType]);
		return NO;
	}

	if (fromPeerId.length == 0 || ![boundPeerId isEqualToString:fromPeerId])
	{
		bleLog([NSString stringWithFormat:@"reject packet context=central:%@ reason=sender_spoof claimed=%@ bound=%@",
			safeString(sourceKey),
			fromPeerId,
			boundPeerId]);
		return NO;
	}

	return YES;
}

- (BOOL)validateInboundPacketFromHost:(const Packet &)packet
{
	if (![self validateInboundPacketShape:packet context:@"host"])
		return NO;

	NSString *kind = safeString(toNSString(packet.kind));
	NSString *msgType = safeString(toNSString(packet.msgType));
	NSString *fromPeerId = safeString(toNSString(packet.fromPeerId));

	if ([kind isEqualToString:@"control"] && ![self validateControlPacketPayload:packet context:@"host"])
		return NO;

	if ([kind isEqualToString:@"control"]
		&& ([msgType isEqualToString:CONTROL_SESSION_MIGRATING] || [msgType isEqualToString:@"session_ended"])
		&& _hostPeerId.length > 0
		&& ![fromPeerId isEqualToString:safeString(_hostPeerId)])
	{
		bleLog([NSString stringWithFormat:@"reject packet context=host reason=host_control_sender_mismatch type=%@ claimed=%@ host=%@",
			msgType,
			fromPeerId,
			safeString(_hostPeerId)]);
		return NO;
	}

	return YES;
}

- (BOOL)enqueueClientPacketData:(NSData *)packetData
{
	Packet packet;
	if (decodePacketData(packetData, packet))
		bleLog([NSString stringWithFormat:@"client enqueuePacket %@", packetSummary(packet)]);
	NSArray<NSData *> *fragments = [self fragmentPacketData:packetData payloadLimit:MAX(_clientPayloadLimit, DEFAULT_FRAGMENT_PAYLOAD_LIMIT)];
	if (fragments == nil)
		return NO;

	[_clientWriteQueue addObjectsFromArray:fragments];
	return [self pumpClientWriteQueue];
}

- (BOOL)pumpNotificationQueueForCentral:(CBCentral *)central
{
	if (central == nil || _peripheralManager == nil || _hostCharacteristic == nil)
		return NO;

	NSString *key = centralKey(central);
	NSMutableArray<NSData *> *queue = _notificationQueues[key];
	if (queue == nil || queue.count == 0)
		return YES;

	if (![_peripheralManager updateValue:queue.firstObject forCharacteristic:_hostCharacteristic onSubscribedCentrals:@[central]])
		return NO;

	[queue removeObjectAtIndex:0];

	if (queue.count == 0)
	{
		[_notificationQueues removeObjectForKey:key];
		return YES;
	}

	if (_reliabilityFragmentSpacingMs > 0)
	{
		dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t) (_reliabilityFragmentSpacingMs * NSEC_PER_MSEC)), dispatch_get_main_queue(), ^{
			[self pumpNotificationQueueForCentral:central];
		});
	}
	else
	{
		[self pumpNotificationQueueForCentral:central];
	}

	return YES;
}

- (BOOL)enqueueNotificationPacketData:(NSData *)packetData toCentral:(CBCentral *)central
{
	if (central == nil)
		return NO;

	NSString *key = centralKey(central);
	NSUInteger payloadLimit = DEFAULT_FRAGMENT_PAYLOAD_LIMIT;
	NSNumber *stored = _centralPayloadLimits[key];
	if (stored != nil)
		payloadLimit = MAX(payloadLimit, (NSUInteger) stored.unsignedIntegerValue);

	NSArray<NSData *> *fragments = [self fragmentPacketData:packetData payloadLimit:payloadLimit];
	if (fragments == nil)
		return NO;

	Packet packet;
	if (decodePacketData(packetData, packet))
		bleLog([NSString stringWithFormat:@"host enqueueNotification central=%@ %@", centralKey(central), packetSummary(packet)]);

	NSMutableArray<NSData *> *queue = _notificationQueues[key];
	if (queue == nil)
	{
		queue = [NSMutableArray array];
		_notificationQueues[key] = queue;
	}

	BOOL wasIdle = queue.count == 0;
	[queue addObjectsFromArray:fragments];
	return !wasIdle || [self pumpNotificationQueueForCentral:central];
}

- (BOOL)notifyClientsPacketData:(NSData *)packetData skipPeerId:(NSString *)skipPeerId
{
	BOOL notified = NO;

	for (NSString *peerId in [_connectedClients allKeys])
	{
		if (skipPeerId != nil && [skipPeerId isEqualToString:peerId])
			continue;

		CBCentral *central = _connectedClients[peerId];
		if (central != nil)
			notified |= [self enqueueNotificationPacketData:packetData toCentral:central];
	}

	return notified;
}

- (void)notifyClientsSessionEnded:(NSString *)reason
{
	if (!_hosting)
		return;

	_metricCtrlOut++;
	NSData *packetData = encodePacketData("control", fromNSString(_hostPeerId), "", "session_ended", fromNSData([safeString(reason) dataUsingEncoding:NSUTF8StringEncoding]), 0);
	[self notifyClientsPacketData:packetData skipPeerId:nil];
}

- (void)notifyPeerJoined:(NSString *)peerId
{
	_metricCtrlOut++;
	NSData *packetData = encodePacketData("control", fromNSString(peerId), "", "peer_joined", std::vector<uint8_t>(), 0);
	[self notifyClientsPacketData:packetData skipPeerId:peerId];
}

- (void)notifyPeerLeft:(NSString *)peerId reason:(NSString *)reason
{
	_metricCtrlOut++;
	NSData *packetData = encodePacketData("control", fromNSString(peerId), "", "peer_left", fromNSData([safeString(reason) dataUsingEncoding:NSUTF8StringEncoding]), 0);
	[self notifyClientsPacketData:packetData skipPeerId:peerId];
}

- (void)sendRosterToCentral:(CBCentral *)central peerId:(NSString *)peerId
{
	for (NSString *existingPeerId in _connectedClients)
	{
		if ([existingPeerId isEqualToString:peerId])
			continue;

		_metricCtrlOut++;
		NSData *packetData = encodePacketData("control", fromNSString(existingPeerId), fromNSString(peerId), "peer_joined", std::vector<uint8_t>(), 0);
		[self enqueueNotificationPacketData:packetData toCentral:central];
	}
}

- (void)handleHelloFromCentral:(CBCentral *)central centralKey:(NSString *)sourceKey packet:(const Packet &)packet
{
	NSString *peerId = safeString(toNSString(packet.fromPeerId));
	if (peerId.length == 0)
		return;

	// Parse HELLO payload: sessionId|joinIntent
	NSString *helloPayload = [[NSString alloc] initWithData:toNSData(packet.payload) encoding:NSUTF8StringEncoding] ?: @"";
	NSArray<NSString *> *parts = [helloPayload componentsSeparatedByString:@"|"];
	NSString *helloSessionId = parts.count > 0 ? safeString(parts[0]) : @"";
	NSString *joinIntent = parts.count > 1 ? safeString(parts[1]) : @"fresh";

	bleLog([NSString stringWithFormat:@"received HELLO from peer=%@ central=%@ session=%@ intent=%@", peerId, sourceKey, helloSessionId, joinIntent]);

	// Validate admission
	if ((NSInteger)_connectedClients.count >= _maxClients && ![self isPeerInReconnectGrace:peerId])
	{
		[self sendJoinRejected:central peerId:peerId reason:@"room_full"];
		return;
	}
	if (_connectedClients[peerId] != nil)
	{
		[self sendJoinRejected:central peerId:peerId reason:@"duplicate_peer_id"];
		return;
	}
	if (helloSessionId.length > 0 && ![helloSessionId isEqualToString:safeString(_sessionId)])
	{
		[self sendJoinRejected:central peerId:peerId reason:@"stale_session"];
		return;
	}
	NSString *toPeer = safeString(toNSString(packet.toPeerId));
	if (toPeer.length > 0 && ![toPeer isEqualToString:safeString(_localPeerId)])
	{
		[self sendJoinRejected:central peerId:peerId reason:@"wrong_target"];
		return;
	}
	if ([joinIntent isEqualToString:@"migration_resume"] && !_migrationDepartureInProgress)
	{
		[self sendJoinRejected:central peerId:peerId reason:@"migration_mismatch"];
		return;
	}

	// Admission granted
	[_pendingClients removeObjectForKey:sourceKey];
	[_pendingClientTimestamps removeObjectForKey:sourceKey];
	_centralsByKey[sourceKey] = central;
	_centralPeerIds[sourceKey] = peerId;
	_connectedClients[peerId] = central;

	// Send hello_ack
	_metricCtrlOut++;
	[self enqueueNotificationPacketData:encodePacketData("control", fromNSString(_localPeerId), fromNSString(peerId), "hello_ack", std::vector<uint8_t>(), 0) toCentral:central];

	if ([self isPeerInReconnectGrace:peerId])
	{
		_metricGraceResume++;
		[self cancelPeerReconnectGrace:peerId];
		[self addSessionPeerId:peerId];
		_rosterStatus[peerId] = @"connected";
		_membershipEpoch++;
		bleLog([NSString stringWithFormat:@"peer reconnected peer=%@ epoch=%ld", peerId, (long)_membershipEpoch]);
		if (_owner != nullptr)
			_owner->onPeerStatus(fromNSString(peerId), "connected");
		[self broadcastRosterSnapshot];
	}
	else
	{
		[self addSessionPeerId:peerId];
		_rosterStatus[peerId] = @"connected";
		_membershipEpoch++;
		bleLog([NSString stringWithFormat:@"peer admitted peer=%@ epoch=%ld", peerId, (long)_membershipEpoch]);
		if (_owner != nullptr)
			_owner->onPeerJoined(fromNSString(peerId));
		[self notifyPeerJoined:peerId];
		[self broadcastRosterSnapshot];
	}
}

- (void)sendJoinRejected:(CBCentral *)central peerId:(NSString *)peerId reason:(NSString *)reason
{
	_metricJoinReject++;
	bleLog([NSString stringWithFormat:@"join_rejected peer=%@ reason=%@", peerId, reason]);
	std::vector<uint8_t> reasonBytes = fromNSData([safeString(reason) dataUsingEncoding:NSUTF8StringEncoding]);
	_metricCtrlOut++;
	[self enqueueNotificationPacketData:encodePacketData("control", fromNSString(_localPeerId), fromNSString(peerId), "join_rejected", reasonBytes, 0) toCentral:central];
}

- (void)handleRosterRequestFromCentral:(CBCentral *)central packet:(const Packet &)packet
{
	NSString *peerId = safeString(toNSString(packet.fromPeerId));
	CBCentral *clientCentral = _connectedClients[peerId];
	if (clientCentral != nil)
	{
		NSData *payload = [self encodeRosterSnapshotPayload];
		_metricCtrlOut++;
		[self enqueueNotificationPacketData:encodePacketData("control", fromNSString(_localPeerId), fromNSString(peerId), "roster_snapshot", fromNSData(payload), 0) toCentral:clientCentral];
	}
}

- (NSData *)encodeRosterSnapshotPayload
{
	NSMutableString *sb = [NSMutableString string];
	[sb appendFormat:@"%@|%@|%ld", safeString(_sessionId), safeString(_localPeerId), (long)_membershipEpoch];
	for (NSString *peerId in _rosterStatus)
	{
		[sb appendFormat:@"|%@:%@", safeString(peerId), safeString(_rosterStatus[peerId])];
	}
	return [sb dataUsingEncoding:NSUTF8StringEncoding];
}

- (void)broadcastRosterSnapshot
{
	NSData *payload = [self encodeRosterSnapshotPayload];
	_metricCtrlOut++;
	[self notifyClientsPacketData:encodePacketData("control", fromNSString(_localPeerId), "", "roster_snapshot", fromNSData(payload), 0) skipPeerId:nil];
}

- (void)handleRosterSnapshot:(NSData *)payloadData
{
	NSString *payload = [[NSString alloc] initWithData:payloadData encoding:NSUTF8StringEncoding] ?: @"";
	NSArray<NSString *> *parts = [payload componentsSeparatedByString:@"|"];
	if (parts.count < 3)
		return;

	NSInteger snapshotEpoch = [parts[2] integerValue];
	_clientLocalEpoch = snapshotEpoch;
	[_rosterStatus removeAllObjects];

	NSMutableSet<NSString *> *newPeers = [NSMutableSet set];
	NSString *snapshotHostId = safeString(parts[1]);

	for (NSUInteger i = 3; i < parts.count; i++)
	{
		NSArray<NSString *> *peerParts = [parts[i] componentsSeparatedByString:@":"];
		if (peerParts.count == 2)
		{
			NSString *pid = safeString(peerParts[0]);
			NSString *status = safeString(peerParts[1]);
			if (pid.length > 0)
			{
				_rosterStatus[pid] = status;
				[newPeers addObject:pid];
			}
		}
	}

	// Detect joins and leaves
	NSMutableSet<NSString *> *currentPeers = [_sessionPeerIds mutableCopy];
	[currentPeers removeObject:safeString(_localPeerId)];
	[currentPeers removeObject:snapshotHostId];

	[self addSessionPeerId:snapshotHostId];
	[self addSessionPeerId:_localPeerId];

	for (NSString *pid in newPeers)
	{
		if (![currentPeers containsObject:pid] && ![pid isEqualToString:safeString(_localPeerId)] && ![pid isEqualToString:snapshotHostId])
		{
			[self addSessionPeerId:pid];
			if (_owner != nullptr)
				_owner->onPeerJoined(fromNSString(pid));
		}
	}

	for (NSString *pid in currentPeers)
	{
		if (![newPeers containsObject:pid] && ![pid isEqualToString:snapshotHostId])
		{
			[self removeSessionPeerId:pid];
			if (_owner != nullptr)
				_owner->onPeerLeft(fromNSString(pid), "roster_update");
		}
	}

	for (NSString *pid in _rosterStatus)
	{
		if ([_rosterStatus[pid] isEqualToString:@"reconnecting"] && _owner != nullptr)
			_owner->onPeerStatus(fromNSString(pid), "reconnecting");
	}

	bleLog([NSString stringWithFormat:@"handleRosterSnapshot host=%@ epoch=%ld peers=%lu", snapshotHostId, (long)snapshotEpoch, (unsigned long)newPeers.count]);
}

- (uint32_t)computeRosterFingerprint
{
	NSMutableArray<NSString *> *entries = [NSMutableArray array];
	for (NSString *pid in _rosterStatus)
	{
		NSString *status = [_rosterStatus[pid] isEqualToString:@"connected"] ? @"c" : @"r";
		[entries addObject:[NSString stringWithFormat:@"%@:%@", safeString(pid), status]];
	}
	[entries sortUsingSelector:@selector(compare:)];
	NSString *joined = [entries componentsJoinedByString:@"|"];
	NSData *data = [joined dataUsingEncoding:NSUTF8StringEncoding];

	// CRC32
	uLong crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, (const Bytef *)data.bytes, (uInt)data.length);
	return (uint32_t)crc;
}

- (void)handleHeartbeatFingerprint:(const std::vector<uint8_t> &)payload
{
	_metricHeartbeatRx++;
	if (payload.size() < 4)
		return;

	uint32_t remoteFingerprint = ((uint32_t)payload[0] << 24) | ((uint32_t)payload[1] << 16) | ((uint32_t)payload[2] << 8) | (uint32_t)payload[3];
	uint32_t localFingerprint = [self computeRosterFingerprint];

	if (remoteFingerprint != localFingerprint)
	{
		NSTimeInterval now = [[NSDate date] timeIntervalSince1970];
		NSTimeInterval interval = _reliabilityHeartbeatInterval;
		if (now - _lastRosterRequestTime >= interval)
		{
			_lastRosterRequestTime = now;
			_metricRosterRequest++;
			_metricCtrlOut++;
			[self enqueueClientPacketData:encodePacketData("control", fromNSString(_localPeerId), fromNSString(_hostPeerId), "roster_request", std::vector<uint8_t>(), 0)];
		}
	}
}

- (void)stopHostOnly
{
	[_connectedClients removeAllObjects];
	[_pendingClients removeAllObjects];
	[_pendingClientTimestamps removeAllObjects];
	[_centralsByKey removeAllObjects];
	[_centralPeerIds removeAllObjects];
	[_centralPayloadLimits removeAllObjects];
	[_notificationQueues removeAllObjects];
	[_inboundAssemblies removeAllObjects];
	_hostServiceReady = NO;
	_hostCharacteristic = nil;

	[_peripheralManager stopAdvertising];
	[_peripheralManager removeAllServices];
}

- (void)stopClientOnly
{
	_clientJoined = NO;
	_clientWriteInFlight = NO;
	_clientPayloadLimit = DEFAULT_FRAGMENT_PAYLOAD_LIMIT;
	[_clientWriteQueue removeAllObjects];
	[_inboundAssemblies removeAllObjects];

	if (_clientPeripheral != nil)
	{
		_clientPeripheral.delegate = nil;
		if (_clientPeripheral.state != CBPeripheralStateDisconnected)
			[_centralManager cancelPeripheralConnection:_clientPeripheral];
	}

	_clientPeripheral = nil;
	_clientCharacteristic = nil;
}

- (BOOL)beginHostingSession:(NSString *)roomName maxClients:(NSInteger)maxClients transport:(NSString *)transport sessionId:(NSString *)sessionId
{
	bleLog([NSString stringWithFormat:@"beginHostingSession room=%@ session=%@ host=%@ transport=%@ max=%ld",
		safeString(roomName),
		safeString(sessionId),
		safeString(_localPeerId),
		safeString(transport),
		(long) maxClients]);
	_roomName = safeString(roomName);
	_maxClients = MAX(1, MIN(7, maxClients));
	_transport = safeString(transport);
	_sessionId = safeString(sessionId);
	_hostPeerId = _localPeerId;
	_joinedSessionId = _sessionId;
	_joinedRoomId = @"";
	_joinedRoomName = _roomName;
	_joinedMaxClients = _maxClients;
	_clientLeaving = NO;
	_hosting = YES;
	_hostAnnounced = NO;
	_hostServiceReady = NO;
	_membershipEpoch = 0;
	[_rosterStatus removeAllObjects];
	[self resetSessionPeerIdsWithHostId:_localPeerId];

	CBMutableCharacteristic *characteristic = [[CBMutableCharacteristic alloc] initWithType:messageUUID()
		properties:(CBCharacteristicPropertyNotify | CBCharacteristicPropertyWrite | CBCharacteristicPropertyWriteWithoutResponse | CBCharacteristicPropertyRead)
		value:nil
		permissions:(CBAttributePermissionsReadable | CBAttributePermissionsWriteable)];

	CBMutableService *service = [[CBMutableService alloc] initWithType:serviceUUID() primary:YES];
	service.characteristics = @[characteristic];

	_hostCharacteristic = characteristic;
	[_peripheralManager removeAllServices];
	[_peripheralManager addService:service];
	[self startHeartbeatTimer];
	return YES;
}

- (void)connectToRoom:(LoveBleRoom *)room migrationJoin:(BOOL)migrationJoin
{
	bleLog([NSString stringWithFormat:@"connectToRoom room=%@ session=%@ host=%@ transport=%@ migration=%@",
		room.roomId ?: @"",
		room.sessionId ?: @"",
		room.hostPeerId ?: @"",
		room.transport ?: @"",
		migrationJoin ? @"YES" : @"NO"]);
	[_centralManager stopScan];
	_scanning = NO;
	[self stopRoomExpiryTimer];
	[self stopClientOnly];

	_joinedRoomId = room.roomId;
	_joinedSessionId = room.sessionId;
	_joinedRoomName = room.name;
	_joinedMaxClients = room.maxClients;
	_hostPeerId = room.hostPeerId;
	_transport = room.transport;
	_clientLeaving = NO;
	_clientJoined = NO;
	_migrationJoinInProgress = migrationJoin;
	if (!migrationJoin && !_reconnectJoinInProgress)
		[self resetSessionPeerIdsWithHostId:room.hostPeerId];
	_clientPeripheral = room.peripheral;
	_clientPeripheral.delegate = self;
	[_centralManager connectPeripheral:_clientPeripheral options:nil];
}

- (void)finishLeave:(NSString *)remoteReason
{
	[self cancelMigrationTimeout];
	[self cancelPendingMigrationDeparture];
	[self cancelReconnectTimeout];
	[self cancelAllPeerReconnectGraces];
	_reconnectSessionId = @"";
	_reconnectHostPeerId = @"";
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;
	[self stopHeartbeatTimer];
	[_dedupEntries removeAllObjects];
	[_dedupLookup removeAllObjects];
	_metricMsgOut = 0; _metricMsgIn = 0; _metricCtrlOut = 0; _metricCtrlIn = 0;
	_metricHeartbeatTx = 0; _metricHeartbeatRx = 0; _metricDedupHit = 0;
	_metricFragmentTx = 0; _metricFragmentRx = 0; _metricFragmentDrop = 0; _metricAssemblyTimeout = 0;
	_metricWriteFail = 0; _metricReconnectAttempt = 0; _metricReconnectSuccess = 0; _metricReconnectFail = 0;
	_metricGraceStart = 0; _metricGraceExpire = 0; _metricGraceResume = 0;
	_metricRosterRequest = 0; _metricJoinReject = 0;

	if (remoteReason != nil)
		[self notifyClientsSessionEnded:remoteReason];

	[_centralManager stopScan];
	_scanning = NO;
	[self stopRoomExpiryTimer];
	_hosting = NO;
	_clientLeaving = YES;
	[self stopHostOnly];
	[self stopClientOnly];
	[_rooms removeAllObjects];
	_joinedRoomId = @"";
	_joinedSessionId = @"";
	_joinedRoomName = @"";
	_hostPeerId = @"";
	_joinedMaxClients = 4;
	_clientPendingHelloAck = NO;
	_hostAnnounced = NO;
	_hostServiceReady = NO;
	_migration = nil;
	_migrationJoinInProgress = NO;
	_migrationDepartureInProgress = NO;
	_membershipEpoch = 0;
	[_rosterStatus removeAllObjects];
	_clientLocalEpoch = 0;
	_lastRosterRequestTime = 0;
	[_pendingClientTimestamps removeAllObjects];
	[_sessionPeerIds removeAllObjects];
}

- (NSString *)selectMigrationSuccessor
{
	if (_connectedClients.count == 0)
		return @"";

	// Exclude peers in reconnect grace
	NSMutableArray<NSString *> *peerIds = [NSMutableArray array];
	for (NSString *peerId in _connectedClients)
	{
		if (![self isPeerInReconnectGrace:peerId])
			[peerIds addObject:peerId];
	}
	[peerIds sortUsingSelector:@selector(compare:)];
	return peerIds.count > 0 ? peerIds.firstObject : @"";
}

- (NSData *)encodeMigrationPayload:(LoveBleMigrationInfo *)info
{
	if (info == nil)
		return nil;

	NSString *payload = [NSString stringWithFormat:@"%@|%@|%ld|%@|%ld",
		safeString(info.sessionId),
		safeString(info.successorPeerId),
		(long) MAX(1, MIN(7, info.maxClients)),
		safeString(info.roomName),
		(long) _membershipEpoch];
	return [payload dataUsingEncoding:NSUTF8StringEncoding];
}

- (LoveBleMigrationInfo *)decodeMigrationPayloadFromHost:(NSString *)oldHostId payload:(NSData *)payload
{
	NSString *payloadString = [[NSString alloc] initWithData:payload encoding:NSUTF8StringEncoding];
	NSArray<NSString *> *parts = [safeString(payloadString) componentsSeparatedByString:@"|"];
	if (parts.count < 4)
		return nil;

	LoveBleMigrationInfo *info = [LoveBleMigrationInfo new];
	info.oldHostId = safeString(oldHostId);
	info.sessionId = safeString(parts[0]);
	info.successorPeerId = safeString(parts[1]);
	info.maxClients = MAX(1, MIN(7, safeString(parts[2]).integerValue));
	info.roomName = safeString(parts[3]);
	info.membershipEpoch = parts.count > 4 ? safeString(parts[4]).integerValue : 0;
	info.transport = safeString(_transport);
	info.becomingHost = [info.successorPeerId isEqualToString:_localPeerId];
	return info;
}

- (BOOL)beginGracefulMigration
{
	// Cancel all reconnect grace; treat grace peers as departed
	for (NSString *gracePeerId in [_peerReconnectTimers allKeys])
	{
		[self cancelPeerReconnectGrace:gracePeerId];
		[self removeSessionPeerId:gracePeerId];
		[_rosterStatus removeObjectForKey:gracePeerId];
	}
	if (_peerReconnectTimers.count > 0)
		_membershipEpoch++;

	NSString *successor = [self selectMigrationSuccessor];
	if (successor.length == 0)
		return NO;

	LoveBleMigrationInfo *info = [LoveBleMigrationInfo new];
	info.oldHostId = _localPeerId;
	info.successorPeerId = successor;
	info.sessionId = _sessionId;
	info.roomName = _roomName;
	info.transport = _transport;
	info.maxClients = _maxClients;

	NSData *payload = [self encodeMigrationPayload:info];
	if (payload == nil)
	{
		[self finishLeave:@"migration_failed"];
		return YES;
	}

	_metricCtrlOut++;
	NSData *packetData = encodePacketData("control", fromNSString(_localPeerId), "", "session_migrating", fromNSData(payload), 0);
	if (![self notifyClientsPacketData:packetData skipPeerId:nil])
	{
		[self finishLeave:@"migration_failed"];
		return YES;
	}

	_migrationDepartureInProgress = YES;
	[self cancelPendingMigrationDeparture];
	_migrationDepartureTimer = [NSTimer scheduledTimerWithTimeInterval:0.4
		target:self
		selector:@selector(migrationDepartureFired:)
		userInfo:nil
		repeats:NO];
	return YES;
}

- (void)startMigration:(LoveBleMigrationInfo *)info
{
	if (info == nil || info.successorPeerId.length == 0 || info.sessionId.length == 0)
	{
		[self finishLeave:nil];
		if (_owner != nullptr)
			_owner->onSessionEnded("migration_failed");
		return;
	}

	_migration = info;
	_migrationJoinInProgress = NO;
	[self removeSessionPeerId:info.oldHostId];
	[self addSessionPeerId:_localPeerId];
	[self addSessionPeerId:info.successorPeerId];
	if (_owner != nullptr)
		_owner->onSessionMigrating(fromNSString(info.oldHostId), fromNSString(info.successorPeerId));
	[self scheduleMigrationTimeout];
}

- (BOOL)beginUnexpectedHostRecovery
{
	if (![_transport isEqualToString:@"resilient"])
		return NO;

	NSString *oldHostId = safeString(_hostPeerId);
	if (oldHostId.length == 0 || safeString(_joinedSessionId).length == 0)
		return NO;

	[self removeSessionPeerId:oldHostId];
	[self addSessionPeerId:_localPeerId];

	NSString *successor = [self selectRecoverySuccessorExcludingHostId:oldHostId];
	if (successor.length == 0)
		return NO;

	LoveBleMigrationInfo *info = [LoveBleMigrationInfo new];
	info.oldHostId = oldHostId;
	info.successorPeerId = successor;
	info.sessionId = safeString(_joinedSessionId);
	info.roomName = safeString(_joinedRoomName.length > 0 ? _joinedRoomName : _roomName);
	info.transport = safeString(_transport);
	info.maxClients = MAX(1, MIN(7, _joinedMaxClients));
	info.becomingHost = [successor isEqualToString:_localPeerId];

	bleLog([NSString stringWithFormat:@"unexpected host loss recovery old_host=%@ new_host=%@ session=%@ local=%@",
		oldHostId,
		successor,
		safeString(_joinedSessionId),
		safeString(_localPeerId)]);

	[self startMigration:info];
	[self beginMigrationReconnect];
	return YES;
}

- (BOOL)hasActiveMigration
{
	return _migration != nil;
}

- (BOOL)matchesMigrationRoom:(LoveBleRoom *)room
{
	return _migration != nil
		&& room != nil
		&& [_migration.sessionId isEqualToString:room.sessionId]
		&& [_migration.successorPeerId isEqualToString:room.hostPeerId];
}

- (void)scheduleMigrationTimeout
{
	[self cancelMigrationTimeout];
	_migrationTimeoutTimer = [NSTimer scheduledTimerWithTimeInterval:MIGRATION_TIMEOUT_SECONDS
		target:self
		selector:@selector(migrationTimeoutFired:)
		userInfo:nil
		repeats:NO];
}

- (void)cancelMigrationTimeout
{
	[_migrationTimeoutTimer invalidate];
	_migrationTimeoutTimer = nil;
}

- (void)cancelPendingMigrationDeparture
{
	[_migrationDepartureTimer invalidate];
	_migrationDepartureTimer = nil;
}

- (void)failMigration
{
	if (![self hasActiveMigration])
		return;

	[self finishLeave:nil];
	if (_owner != nullptr)
		_owner->onSessionEnded("migration_failed");
}

- (void)beginMigrationReconnect
{
	if (![self hasActiveMigration])
		return;

	if (_migration.becomingHost)
	{
		if (![self beginHostingSession:_migration.roomName maxClients:_migration.maxClients transport:_migration.transport sessionId:_migration.sessionId])
			[self failMigration];
		return;
	}

	[self scanRooms];
}

- (void)completeMigrationResume
{
	if (![self hasActiveMigration])
		return;

	_joinedSessionId = _migration.sessionId;
	_joinedRoomName = _migration.roomName;
	_joinedMaxClients = _migration.maxClients;
	_hostPeerId = _migration.successorPeerId;
	_membershipEpoch = _migration.membershipEpoch;
	_clientLocalEpoch = _migration.membershipEpoch;
	if (_migration.becomingHost)
		_joinedRoomId = @"";

	[self addSessionPeerId:_localPeerId];
	[self addSessionPeerId:_migration.successorPeerId];

	if (_owner != nullptr)
		_owner->onSessionResumed(fromNSString(_migration.sessionId), fromNSString(_migration.successorPeerId));

	[self cancelMigrationTimeout];
	_migration = nil;
	_migrationJoinInProgress = NO;
	_migrationDepartureInProgress = NO;
}

- (void)completeLocalJoin
{
	[self addSessionPeerId:_localPeerId];
	[self addSessionPeerId:_hostPeerId];
	_clientPendingHelloAck = YES;

	NSString *joinIntent;
	if (_reconnectJoinInProgress && [self hasActiveReconnect])
		joinIntent = @"reconnect";
	else if (_migrationJoinInProgress && [self hasActiveMigration])
		joinIntent = @"migration_resume";
	else
		joinIntent = @"fresh";

	NSString *helloSessionId = [joinIntent isEqualToString:@"fresh"] && safeString(_joinedSessionId).length == 0 ? @"" : safeString(_joinedSessionId);
	NSString *helloPayload = [NSString stringWithFormat:@"%@|%@", helloSessionId, joinIntent];

	bleLog([NSString stringWithFormat:@"completeLocalJoin room=%@ session=%@ local=%@ host=%@ transport=%@ intent=%@",
		safeString(_joinedRoomId),
		safeString(_joinedSessionId),
		safeString(_localPeerId),
		safeString(_hostPeerId),
		safeString(_transport),
		joinIntent]);

	std::vector<uint8_t> payloadBytes = fromNSData([helloPayload dataUsingEncoding:NSUTF8StringEncoding]);
	_metricCtrlOut++;
	[self enqueueClientPacketData:encodePacketData("control", fromNSString(_localPeerId), fromNSString(_hostPeerId), "hello", payloadBytes, 0)];
}

- (void)handleJoinFailure:(NSString *)detail
{
	bleLog([NSString stringWithFormat:@"handleJoinFailure detail=%@", safeString(detail)]);
	_clientLeaving = YES;
	[self stopClientOnly];

	if (_reconnectJoinInProgress || [self hasActiveReconnect])
	{
		_reconnectJoinInProgress = NO;
		_reconnectScanInProgress = NO;
		[self scanRooms];
		_reconnectScanInProgress = YES;
	}
	else if (_migrationJoinInProgress || [self hasActiveMigration])
		[self failMigration];
	else if (_owner != nullptr)
		_owner->onError("join_failed", fromNSString(safeString(detail)));
}

- (void)migrationTimeoutFired:(NSTimer *)timer
{
	if (_migrationTimeoutTimer != timer)
		return;

	_migrationTimeoutTimer = nil;

	if (_migration == nil)
	{
		[self failMigration];
		return;
	}

	// Convergence fallback: exclude failed successor and re-elect
	[_migration.excludedSuccessors addObject:_migration.successorPeerId];
	NSString *nextSuccessor = [self selectRecoverySuccessorExcludingHostId:_migration.oldHostId excluded:_migration.excludedSuccessors];
	if (nextSuccessor.length == 0)
	{
		[self failMigration];
	}
	else
	{
		_migration.successorPeerId = nextSuccessor;
		_migration.becomingHost = [nextSuccessor isEqualToString:_localPeerId];
		bleLog([NSString stringWithFormat:@"convergence fallback new_host=%@", nextSuccessor]);
		if (_owner != nullptr)
			_owner->onSessionMigrating(fromNSString(_migration.oldHostId), fromNSString(nextSuccessor));
		[self beginMigrationReconnect];
		[self scheduleMigrationTimeout];
	}
}

- (void)migrationDepartureFired:(NSTimer *)timer
{
	if (_migrationDepartureTimer != timer)
		return;

	_migrationDepartureTimer = nil;
	[self finishLeave:nil];
}

- (void)hostRoom:(const std::string &)room maxClients:(int)maxClients transport:(love::ble::Ble::Transport)transport
{
	NSString *roomName = safeString(toNSString(room));
	roomName = [[roomName stringByReplacingOccurrencesOfString:@"|" withString:@" "] stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]];
	if (roomName.length == 0)
		roomName = @"Room";
	if (roomName.length > 8)
		roomName = [roomName substringToIndex:8];

	[self leave];
	[self beginHostingSession:roomName maxClients:maxClients transport:transportName(transport) sessionId:[self generateShortId]];
}

- (void)scanRooms
{
	bleLog(@"scanRooms");
	[_rooms removeAllObjects];
	[_centralManager stopScan];
	[_centralManager scanForPeripheralsWithServices:nil options:@{CBCentralManagerScanOptionAllowDuplicatesKey: @YES}];
	bleLog(@"scan started filter=none");
	_scanning = YES;
	[self startRoomExpiryTimer];
}

- (void)joinRoomId:(const std::string &)roomId
{
	NSString *roomKey = safeString(toNSString(roomId));
	bleLog([NSString stringWithFormat:@"joinRoomId room=%@", roomKey]);
	LoveBleRoom *room = _rooms[roomKey];
	if (room == nil || room.peripheral == nil)
	{
		if (_owner != nullptr)
		_owner->onError("room_gone", "Selected room is no longer available.");
		return;
	}

	if (_clientPeripheral != nil && !_clientJoined && [_joinedRoomId isEqualToString:room.roomId])
	{
		bleLog([NSString stringWithFormat:@"ignoring duplicate join attempt for room=%@", room.roomId ?: @""]);
		return;
	}

	[self connectToRoom:room migrationJoin:NO];
}

- (void)leave
{
	if (_hosting && [_transport isEqualToString:@"resilient"] && _connectedClients.count > 0 && [self beginGracefulMigration])
		return;

	[self finishLeave:@"host_left"];
}

- (BOOL)broadcastMessageType:(const std::string &)msgType payload:(const std::vector<uint8_t> &)payload
{
	_metricMsgOut++;
	NSData *packetData = encodePacketData("data", fromNSString(_localPeerId), "", msgType, payload, [self nextMessageId]);
	if (_hosting)
	{
		_lastBroadcastPacketData = packetData;
		return _connectedClients.count == 0 || [self notifyClientsPacketData:packetData skipPeerId:nil];
	}

	return [self enqueueClientPacketData:packetData];
}

- (BOOL)sendPeerId:(const std::string &)peerId messageType:(const std::string &)msgType payload:(const std::vector<uint8_t> &)payload
{
	_metricMsgOut++;
	NSData *packetData = encodePacketData("data", fromNSString(_localPeerId), peerId, msgType, payload, [self nextMessageId]);
	if (_hosting)
	{
		CBCentral *central = _connectedClients[safeString(toNSString(peerId))];
		if (central == nil)
		{
			if (_owner != nullptr)
				_owner->onError("send_failed", "Target peer is not connected.");
			return NO;
		}

		return [self enqueueNotificationPacketData:packetData toCentral:central];
	}

	return [self enqueueClientPacketData:packetData];
}

- (void)centralManagerDidUpdateState:(CBCentralManager *)central
{
	#pragma unused(central)
}

- (void)peripheralManagerDidUpdateState:(CBPeripheralManager *)peripheral
{
	#pragma unused(peripheral)
}

- (void)centralManager:(CBCentralManager *)central didDiscoverPeripheral:(CBPeripheral *)peripheral advertisementData:(NSDictionary<NSString *, id> *)advertisementData RSSI:(NSNumber *)RSSI
{
	#pragma unused(central)
	bleLog([NSString stringWithFormat:@"didDiscoverPeripheral id=%@ rssi=%@ adv_keys=%@", peripheral.identifier.UUIDString ?: @"", RSSI, [[advertisementData allKeys] componentsJoinedByString:@","]]);
	LoveBleRoom *room = [self decodeRoomFromPeripheral:peripheral advertisementData:advertisementData RSSI:RSSI];
	if (room == nil)
		return;

	_rooms[room.roomId] = room;
	if ([self hasActiveMigration] && [self matchesMigrationRoom:room] && !_migration.becomingHost && _clientPeripheral == nil)
	{
		[self connectToRoom:room migrationJoin:YES];
		return;
	}

	if ([self hasActiveMigration])
		return;

	if ([self hasActiveReconnect] && _reconnectScanInProgress && _clientPeripheral == nil)
	{
		if ([self matchesReconnectRoom:room])
		{
			_reconnectScanInProgress = NO;
			_reconnectJoinInProgress = YES;
			[self connectToRoom:room migrationJoin:NO];
		}
		else if (room.hostPeerId.length > 0
				 && [safeString(room.hostPeerId) isEqualToString:_reconnectHostPeerId]
				 && room.sessionId.length > 0
				 && ![safeString(room.sessionId) isEqualToString:_reconnectSessionId])
		{
			[self failReconnect];
		}
		return;
	}

	if (_owner != nullptr)
		_owner->onRoomFound(fromNSString(room.roomId), fromNSString(room.sessionId), fromNSString(room.name), fromNSString(room.transport), (int) room.peerCount, (int) room.maxClients, (int) room.rssi);
}

- (void)centralManager:(CBCentralManager *)central didConnectPeripheral:(CBPeripheral *)peripheral
{
	#pragma unused(central)
	if (_clientPeripheral != peripheral)
		return;

	bleLog([NSString stringWithFormat:@"didConnectPeripheral id=%@", peripheral.identifier.UUIDString ?: @""]);
	_clientPayloadLimit = MAX(DEFAULT_FRAGMENT_PAYLOAD_LIMIT, [peripheral maximumWriteValueLengthForType:CBCharacteristicWriteWithResponse]);
	[peripheral discoverServices:@[serviceUUID()]];
}

- (void)centralManager:(CBCentralManager *)central didFailToConnectPeripheral:(CBPeripheral *)peripheral error:(NSError *)error
{
	#pragma unused(central)
	#pragma unused(peripheral)
	bleLog([NSString stringWithFormat:@"didFailToConnectPeripheral id=%@ error=%@", peripheral.identifier.UUIDString ?: @"", error.localizedDescription ?: @""]);
	if (_migrationJoinInProgress || [self hasActiveMigration])
		[self failMigration];
	else
		[self handleJoinFailure:error.localizedDescription ?: @"Could not connect to BLE host."];
}

- (void)centralManager:(CBCentralManager *)central didDisconnectPeripheral:(CBPeripheral *)peripheral error:(NSError *)error
{
	#pragma unused(central)
	bleLog([NSString stringWithFormat:@"didDisconnectPeripheral id=%@ error=%@", peripheral.identifier.UUIDString ?: @"", error.localizedDescription ?: @""]);
	BOOL shouldEmit = !_clientLeaving && _clientPeripheral == peripheral;
	BOOL wasJoined = _clientJoined;
	[self stopClientOnly];

	if ([self hasActiveMigration])
	{
		[self beginMigrationReconnect];
		return;
	}

	if (shouldEmit && wasJoined && [self beginUnexpectedHostRecovery])
		return;

	if (shouldEmit && wasJoined && [self beginClientReconnect])
		return;

	// If GATT connect failed during an active reconnect, retry via handleJoinFailure
	if (!shouldEmit && (_reconnectJoinInProgress || [self hasActiveReconnect]))
	{
		bleLog(@"reconnect GATT connect failed, retrying scan");
		[self handleJoinFailure:error.localizedDescription ?: @"BLE reconnect connection failed."];
		return;
	}

	if (_owner == nullptr || !shouldEmit)
		return;

	[self finishLeave:nil];
	if (wasJoined)
		_owner->onSessionEnded("host_lost");
	else
		_owner->onError("join_failed", "BLE connection was lost before session join completed.");
}

- (void)peripheral:(CBPeripheral *)peripheral didDiscoverServices:(NSError *)error
{
	if (_clientPeripheral != peripheral)
		return;

	bleLog([NSString stringWithFormat:@"didDiscoverServices id=%@ error=%@ count=%lu",
		peripheral.identifier.UUIDString ?: @"",
		error.localizedDescription ?: @"",
		(unsigned long) peripheral.services.count]);
	if (error != nil)
	{
		[self handleJoinFailure:error.localizedDescription ?: @"BLE service discovery failed."];
		return;
	}

	for (CBService *service in peripheral.services)
	{
		if ([service.UUID isEqual:serviceUUID()])
		{
			[peripheral discoverCharacteristics:@[messageUUID()] forService:service];
			return;
		}
	}

	[self handleJoinFailure:@"BLE service not found on host."];
}

- (void)peripheral:(CBPeripheral *)peripheral didDiscoverCharacteristicsForService:(CBService *)service error:(NSError *)error
{
	if (_clientPeripheral != peripheral)
		return;

	bleLog([NSString stringWithFormat:@"didDiscoverCharacteristics id=%@ service=%@ error=%@ count=%lu",
		peripheral.identifier.UUIDString ?: @"",
		service.UUID.UUIDString ?: @"",
		error.localizedDescription ?: @"",
		(unsigned long) service.characteristics.count]);
	if (error != nil)
	{
		[self handleJoinFailure:error.localizedDescription ?: @"BLE characteristic discovery failed."];
		return;
	}

	for (CBCharacteristic *characteristic in service.characteristics)
	{
		if ([characteristic.UUID isEqual:messageUUID()])
		{
			_clientCharacteristic = characteristic;
			[peripheral setNotifyValue:YES forCharacteristic:characteristic];
			return;
		}
	}

	[self handleJoinFailure:@"BLE message characteristic not found."];
}

- (void)peripheral:(CBPeripheral *)peripheral didUpdateNotificationStateForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	if (_clientPeripheral != peripheral || ![characteristic.UUID isEqual:messageUUID()])
		return;

	bleLog([NSString stringWithFormat:@"didUpdateNotificationState id=%@ characteristic=%@ notifying=%@ error=%@",
		peripheral.identifier.UUIDString ?: @"",
		characteristic.UUID.UUIDString ?: @"",
		characteristic.isNotifying ? @"YES" : @"NO",
		error.localizedDescription ?: @""]);
	if (error != nil || !characteristic.isNotifying)
	{
		[self handleJoinFailure:error.localizedDescription ?: @"Could not enable BLE notifications."];
		return;
	}

	[self completeLocalJoin];
}

- (void)peripheral:(CBPeripheral *)peripheral didWriteValueForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	#pragma unused(characteristic)
	if (_clientPeripheral != peripheral)
		return;

	if (_clientWriteQueue.count > 0)
		[_clientWriteQueue removeObjectAtIndex:0];

	_clientWriteInFlight = NO;
	if (error != nil)
	{
		[_clientWriteQueue removeAllObjects];
		_metricWriteFail++;
		if (_owner != nullptr)
			_owner->onError("write_failed", error.localizedDescription.UTF8String ?: "BLE write to host failed.");
		return;
	}

	if (_reliabilityFragmentSpacingMs > 0 && _clientWriteQueue.count > 0)
	{
		dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t) (_reliabilityFragmentSpacingMs * NSEC_PER_MSEC)), dispatch_get_main_queue(), ^{
			[self pumpClientWriteQueue];
		});
	}
	else
	{
		[self pumpClientWriteQueue];
	}
}

- (void)peripheral:(CBPeripheral *)peripheral didUpdateValueForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	if (_clientPeripheral != peripheral || ![characteristic.UUID isEqual:messageUUID()])
		return;

	if (error != nil)
	{
		if (_owner != nullptr)
			_owner->onError("invalid_payload", error.localizedDescription.UTF8String ?: "BLE notification decode failed.");
		return;
	}

	uint16_t packetNonce = 0;
	NSData *packetData = [self processIncomingFragment:@"host" data:characteristic.value outNonce:&packetNonce];
	if (packetData == nil)
		return;

	Packet packet;
	if (!decodePacketData(packetData, packet))
	{
		if (_owner != nullptr)
			_owner->onError("invalid_payload", "Received malformed BLE packet.");
		return;
	}

	if (![self validateInboundPacketFromHost:packet])
		return;

	bleLog([NSString stringWithFormat:@"client received packet %@", packetSummary(packet)]);

	if (packet.kind == "control")
	{
		_metricCtrlIn++;
		if (!packet.toPeerId.empty() && packet.toPeerId != fromNSString(_localPeerId))
			return;

		if (packet.msgType == "hello_ack")
		{
			_clientPendingHelloAck = NO;
			_clientJoined = YES;
			if (_reconnectJoinInProgress && [self hasActiveReconnect])
				[self completeReconnectResume];
			else if (_migrationJoinInProgress && [self hasActiveMigration])
				[self completeMigrationResume];
			else if (_owner != nullptr)
				_owner->onJoined(fromNSString(_joinedSessionId), fromNSString(_joinedRoomId), fromNSString(_localPeerId), fromNSString(_hostPeerId), fromNSString(_transport));
		}
		else if (packet.msgType == "join_rejected")
		{
			std::string reason = fromNSString([[NSString alloc] initWithData:toNSData(packet.payload) encoding:NSUTF8StringEncoding]);
			_clientPendingHelloAck = NO;
			bleLog([NSString stringWithFormat:@"join rejected reason=%s", reason.c_str()]);
			NSString *savedRoomId = _joinedRoomId;
			_clientLeaving = YES;
			[self stopClientOnly];
			if (_owner != nullptr)
				_owner->onJoinFailed(reason, fromNSString(savedRoomId));
		}
		else if (packet.msgType == "roster_snapshot")
		{
			[self handleRosterSnapshot:toNSData(packet.payload)];
		}
		else if (packet.msgType == "peer_joined" && !packet.fromPeerId.empty() && packet.fromPeerId != fromNSString(_localPeerId))
		{
			[self addSessionPeerId:toNSString(packet.fromPeerId)];
			if (_owner != nullptr)
				_owner->onPeerJoined(packet.fromPeerId);
		}
		else if (packet.msgType == "peer_left" && !packet.fromPeerId.empty())
		{
			[self removeSessionPeerId:toNSString(packet.fromPeerId)];
			if (_owner != nullptr)
				_owner->onPeerLeft(packet.fromPeerId, fromNSString([[NSString alloc] initWithData:toNSData(packet.payload) encoding:NSUTF8StringEncoding]));
		}
		else if (packet.msgType == fromNSString(CONTROL_SESSION_MIGRATING))
		{
			[_clientWriteQueue removeAllObjects];
			[_inboundAssemblies removeAllObjects];
			[self startMigration:[self decodeMigrationPayloadFromHost:toNSString(packet.fromPeerId) payload:toNSData(packet.payload)]];
		}
		else if (packet.msgType == "session_ended")
		{
			[self finishLeave:nil];
			if (_owner != nullptr)
				_owner->onSessionEnded(fromNSString([[NSString alloc] initWithData:toNSData(packet.payload) encoding:NSUTF8StringEncoding]));
		}
		else if (packet.msgType == "heartbeat")
		{
			[self handleHeartbeatFingerprint:packet.payload];
		}

		return;
	}

	if (packet.kind == "data" && (packet.toPeerId.empty() || packet.toPeerId == fromNSString(_localPeerId)))
	{
		if (![self isDuplicateMessageFrom:safeString(toNSString(packet.fromPeerId)) msgType:packet.msgType messageId:packet.messageId])
		{
			_metricMsgIn++;
			_owner->onMessage(packet.fromPeerId, packet.msgType, packet.payload);
		}
	}
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral didAddService:(CBService *)service error:(NSError *)error
{
	#pragma unused(peripheral)
	if (![service.UUID isEqual:serviceUUID()] || !_hosting)
		return;

	if (error != nil)
	{
		[self stopHostOnly];
		_hosting = NO;
		if ([self hasActiveMigration] && _migration.becomingHost)
			[self failMigration];
		else if (_owner != nullptr)
			_owner->onError("host_failed", error.localizedDescription.UTF8String ?: "Bluetooth GATT service registration failed.");
		return;
	}

	_hostServiceReady = YES;
	bleLog([NSString stringWithFormat:@"didAddService service=%@ error=%@", service.UUID.UUIDString ?: @"", error.localizedDescription ?: @""]);
	NSString *localName = [self encodeRoomLocalName];
	NSData *localNameData = [localName dataUsingEncoding:NSUTF8StringEncoding];
	bleLog([NSString stringWithFormat:@"advertise adv service_uuid=omitted local_name=%@ local_name_len=%lu local_name_hex=%@",
		safeString(localName),
		(unsigned long) localName.length,
		hexPreview(localNameData)]);
	NSDictionary *advertisement = @{
		CBAdvertisementDataLocalNameKey: localName,
	};
	[_peripheralManager startAdvertising:advertisement];
}

- (void)peripheralManagerDidStartAdvertising:(CBPeripheralManager *)peripheral error:(NSError *)error
{
	#pragma unused(peripheral)
	bleLog([NSString stringWithFormat:@"didStartAdvertising error=%@ isAdvertising=%d state=%ld",
		error.localizedDescription ?: @"",
		_peripheralManager.isAdvertising ? 1 : 0,
		(long) _peripheralManager.state]);
	if (!_hosting)
		return;

	if (error != nil)
	{
		[self stopHostOnly];
		_hosting = NO;
		if ([self hasActiveMigration] && _migration.becomingHost)
			[self failMigration];
		else if (_owner != nullptr)
			_owner->onError("host_failed", error.localizedDescription.UTF8String ?: "Advertising failed.");
		return;
	}

	if (!_hostAnnounced)
	{
		_hostAnnounced = YES;
		if ([self hasActiveMigration] && _migration.becomingHost)
			[self completeMigrationResume];
		else if (_owner != nullptr)
			_owner->onHosted(fromNSString(_sessionId), fromNSString(_localPeerId), fromNSString(_transport));
	}

	dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t) (1500 * NSEC_PER_MSEC)), dispatch_get_main_queue(), ^{
		if (!self->_hosting)
			return;

		bleLog([NSString stringWithFormat:@"advertise watchdog after_ms=1500 isAdvertising=%d state=%ld hostServiceReady=%d connected=%lu pending=%lu",
			self->_peripheralManager.isAdvertising ? 1 : 0,
			(long) self->_peripheralManager.state,
			self->_hostServiceReady ? 1 : 0,
			(unsigned long) self->_connectedClients.count,
			(unsigned long) self->_pendingClients.count]);
	});
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral central:(CBCentral *)central didSubscribeToCharacteristic:(CBCharacteristic *)characteristic
{
	#pragma unused(peripheral)
	if (![characteristic.UUID isEqual:messageUUID()])
		return;

	bleLog([NSString stringWithFormat:@"didSubscribe central=%@ mtu_payload=%lu characteristic=%@",
		centralKey(central),
		(unsigned long) central.maximumUpdateValueLength,
		characteristic.UUID.UUIDString ?: @""]);
	NSString *key = centralKey(central);
	_pendingClients[key] = central;
	_pendingClientTimestamps[key] = [NSDate date];
	_centralsByKey[key] = central;
	_centralPayloadLimits[key] = @(MAX((NSUInteger) central.maximumUpdateValueLength, DEFAULT_FRAGMENT_PAYLOAD_LIMIT));
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral central:(CBCentral *)central didUnsubscribeFromCharacteristic:(CBCharacteristic *)characteristic
{
	#pragma unused(peripheral)
	if (![characteristic.UUID isEqual:messageUUID()])
		return;

	bleLog([NSString stringWithFormat:@"didUnsubscribe central=%@ characteristic=%@", centralKey(central), characteristic.UUID.UUIDString ?: @""]);
	NSString *key = centralKey(central);
	[_pendingClients removeObjectForKey:key];
	[_centralsByKey removeObjectForKey:key];
	[_centralPayloadLimits removeObjectForKey:key];
	[_notificationQueues removeObjectForKey:key];

	NSString *peerId = _centralPeerIds[key];
	if (peerId != nil)
	{
		[_centralPeerIds removeObjectForKey:key];
		[_connectedClients removeObjectForKey:peerId];

		if (_hosting && !_migrationDepartureInProgress)
		{
			[self beginPeerReconnectGrace:peerId];
		}
		else
		{
			[self removeSessionPeerId:peerId];
			[_rosterStatus removeObjectForKey:peerId];
		}
	}
}

- (void)peripheralManagerIsReadyToUpdateSubscribers:(CBPeripheralManager *)peripheral
{
	#pragma unused(peripheral)
	for (NSString *key in [_notificationQueues allKeys])
	{
		CBCentral *central = _centralsByKey[key];
		if (central != nil)
			[self pumpNotificationQueueForCentral:central];
	}
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral didReceiveWriteRequests:(NSArray<CBATTRequest *> *)requests
{
	#pragma unused(peripheral)
	for (CBATTRequest *request in requests)
	{
		if (![request.characteristic.UUID isEqual:messageUUID()])
		{
			[_peripheralManager respondToRequest:request withResult:CBATTErrorRequestNotSupported];
			continue;
		}

		if (request.offset != 0 || request.value == nil)
		{
			[_peripheralManager respondToRequest:request withResult:CBATTErrorInvalidOffset];
			continue;
		}

		[_peripheralManager respondToRequest:request withResult:CBATTErrorSuccess];

		NSString *sourceKey = centralKey(request.central);
		uint16_t reqNonce = 0;
		NSData *packetData = [self processIncomingFragment:sourceKey data:request.value outNonce:&reqNonce];
		if (packetData == nil)
			continue;

		Packet packet;
		if (!decodePacketData(packetData, packet))
		{
			if (_owner != nullptr)
				_owner->onError("invalid_payload", "Received malformed BLE packet.");
			continue;
		}

		if (![self validateInboundPacketFromCentral:packet centralKey:sourceKey])
			continue;

		bleLog([NSString stringWithFormat:@"host received packet central=%@ %@", sourceKey, packetSummary(packet)]);

		if (packet.kind == "control" && packet.msgType == "hello")
		{
			_metricCtrlIn++;
			[self handleHelloFromCentral:request.central centralKey:sourceKey packet:packet];
			continue;
		}

		if (packet.kind == "control" && packet.msgType == "roster_request")
		{
			_metricCtrlIn++;
			[self handleRosterRequestFromCentral:request.central packet:packet];
			continue;
		}

		if (packet.kind != "data" || packet.fromPeerId.empty())
			continue;

		if ([self isDuplicateMessageFrom:safeString(toNSString(packet.fromPeerId)) msgType:packet.msgType messageId:packet.messageId])
			continue;

		if (packet.toPeerId.empty())
		{
			// Deliver to self only if sender is not the host
			if (_owner != nullptr && packet.fromPeerId != fromNSString(_localPeerId))
			{
				_metricMsgIn++;
				_owner->onMessage(packet.fromPeerId, packet.msgType, packet.payload);
			}
			[self notifyClientsPacketData:packetData skipPeerId:safeString(toNSString(packet.fromPeerId))];
		}
		else if (packet.toPeerId == fromNSString(_localPeerId))
		{
			if (_owner != nullptr)
			{
				_metricMsgIn++;
				_owner->onMessage(packet.fromPeerId, packet.msgType, packet.payload);
			}
		}
		else
		{
			CBCentral *target = _connectedClients[safeString(toNSString(packet.toPeerId))];
			if (target != nil)
				[self enqueueNotificationPacketData:packetData toCentral:target];
		}
	}
}

- (void)applyReliabilityConfig:(const love::ble::Ble::ReliabilityConfig &)config
{
	_reliabilityHeartbeatInterval = config.heartbeatInterval;
	_reliabilityFragmentSpacingMs = config.fragmentSpacingMs;
	_reliabilityDedupWindow = config.dedupWindow;
	bleLog([NSString stringWithFormat:@"reliability config heartbeat=%.1fs fragment_spacing=%ldms dedup=%ld",
		_reliabilityHeartbeatInterval,
		(long) _reliabilityFragmentSpacingMs,
		(long) _reliabilityDedupWindow]);
}

- (void)startHeartbeatTimer
{
	[self stopHeartbeatTimer];
	if (_reliabilityHeartbeatInterval <= 0)
		return;

	_heartbeatTimer = [NSTimer scheduledTimerWithTimeInterval:_reliabilityHeartbeatInterval
		target:self
		selector:@selector(heartbeatFired:)
		userInfo:nil
		repeats:YES];
}

- (void)stopHeartbeatTimer
{
	[_heartbeatTimer invalidate];
	_heartbeatTimer = nil;
	_lastBroadcastPacketData = nil;
}

- (void)heartbeatFired:(NSTimer *)timer
{
	#pragma unused(timer)
	if (!_hosting)
		return;

	// Disconnect stale pending clients (5-second timeout)
	NSDate *now = [NSDate date];
	NSMutableArray<NSString *> *staleKeys = [NSMutableArray array];
	for (NSString *key in _pendingClientTimestamps)
	{
		NSDate *ts = _pendingClientTimestamps[key];
		if ([now timeIntervalSinceDate:ts] > 5.0)
			[staleKeys addObject:key];
	}
	for (NSString *key in staleKeys)
	{
		[_pendingClients removeObjectForKey:key];
		[_pendingClientTimestamps removeObjectForKey:key];
		bleLog([NSString stringWithFormat:@"pending client timeout central=%@", key]);
	}

	// Send roster fingerprint to all connected clients
	if (_connectedClients.count > 0)
	{
		uint32_t fingerprint = [self computeRosterFingerprint];
		uint8_t fpBytes[4];
		fpBytes[0] = (uint8_t)((fingerprint >> 24) & 0xFF);
		fpBytes[1] = (uint8_t)((fingerprint >> 16) & 0xFF);
		fpBytes[2] = (uint8_t)((fingerprint >> 8) & 0xFF);
		fpBytes[3] = (uint8_t)(fingerprint & 0xFF);
		std::vector<uint8_t> fpVec(fpBytes, fpBytes + 4);
		_metricCtrlOut++;
		_metricHeartbeatTx++;
		[self notifyClientsPacketData:encodePacketData("control", fromNSString(_localPeerId), "", "heartbeat", fpVec, 0) skipPeerId:nil];
	}

	// Re-broadcast last data packet
	if (_connectedClients.count > 0 && _lastBroadcastPacketData != nil)
	{
		bleLog(@"heartbeat re-broadcast");
		[self notifyClientsPacketData:_lastBroadcastPacketData skipPeerId:nil];
	}
}

- (BOOL)isDuplicateMessageFrom:(NSString *)fromPeerId msgType:(const std::string &)msgType messageId:(int)messageId
{
	NSTimeInterval now = [[NSDate date] timeIntervalSince1970];
	NSTimeInterval expirySeconds = 5.0;

	while (_dedupEntries.count > 0)
	{
		NSDictionary *first = _dedupEntries.firstObject;
		NSTimeInterval ts = [first[@"t"] doubleValue];
		if (now - ts > expirySeconds)
		{
			[_dedupLookup removeObject:first[@"id"]];
			[_dedupEntries removeObjectAtIndex:0];
		}
		else
			break;
	}

	while ((NSInteger) _dedupEntries.count > _reliabilityDedupWindow)
	{
		NSDictionary *first = _dedupEntries.firstObject;
		[_dedupLookup removeObject:first[@"id"]];
		[_dedupEntries removeObjectAtIndex:0];
	}

	NSString *dedupId = [NSString stringWithFormat:@"%@:%s:%d", fromPeerId ?: @"", msgType.c_str(), messageId];
	if ([_dedupLookup containsObject:dedupId])
	{
		_metricDedupHit++;
		bleLog([NSString stringWithFormat:@"dedup: dropped %@", dedupId]);
		return YES;
	}

	[_dedupEntries addObject:@{@"id": dedupId, @"t": @(now)}];
	[_dedupLookup addObject:dedupId];
	return NO;
}

// --- Client-side reconnect ---

- (BOOL)hasActiveReconnect
{
	return _reconnectSessionId.length > 0;
}

- (BOOL)matchesReconnectRoom:(LoveBleRoom *)room
{
	return room != nil
		&& [safeString(_reconnectSessionId) isEqualToString:safeString(room.sessionId)]
		&& [safeString(_reconnectHostPeerId) isEqualToString:safeString(room.hostPeerId)];
}

- (BOOL)beginClientReconnect
{
	_metricReconnectAttempt++;
	if (safeString(_joinedSessionId).length == 0 || safeString(_hostPeerId).length == 0)
		return NO;

	_reconnectSessionId = safeString(_joinedSessionId);
	_reconnectHostPeerId = safeString(_hostPeerId);
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	bleLog([NSString stringWithFormat:@"beginClientReconnect session=%@ host=%@",
		_reconnectSessionId, _reconnectHostPeerId]);

	if (_owner != nullptr)
		_owner->onPeerStatus(fromNSString(_localPeerId), "reconnecting");

	[self scheduleReconnectTimeout];
	[self scanRooms];
	_reconnectScanInProgress = YES;
	return YES;
}

- (void)completeReconnectResume
{
	if (![self hasActiveReconnect])
		return;

	_metricReconnectSuccess++;
	bleLog([NSString stringWithFormat:@"completeReconnectResume session=%@", _reconnectSessionId]);

	[self cancelReconnectTimeout];
	_reconnectSessionId = @"";
	_reconnectHostPeerId = @"";
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	if (_owner != nullptr)
		_owner->onPeerStatus(fromNSString(_localPeerId), "connected");
}

- (void)failReconnect
{
	if (![self hasActiveReconnect])
		return;

	_metricReconnectFail++;
	bleLog([NSString stringWithFormat:@"failReconnect session=%@", _reconnectSessionId]);

	[self cancelReconnectTimeout];
	_reconnectSessionId = @"";
	_reconnectHostPeerId = @"";
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	[_centralManager stopScan];
	_scanning = NO;
	[self stopRoomExpiryTimer];
	[self finishLeave:nil];
	if (_owner != nullptr)
		_owner->onSessionEnded("host_lost");
}

- (void)scheduleReconnectTimeout
{
	[self cancelReconnectTimeout];
	_reconnectTimer = [NSTimer scheduledTimerWithTimeInterval:RECONNECT_TIMEOUT_SECONDS
		target:self
		selector:@selector(reconnectTimeoutFired:)
		userInfo:nil
		repeats:NO];
}

- (void)cancelReconnectTimeout
{
	if (_reconnectTimer != nil)
	{
		[_reconnectTimer invalidate];
		_reconnectTimer = nil;
	}
}

- (void)reconnectTimeoutFired:(NSTimer *)timer
{
	if (_reconnectTimer != timer)
		return;
	_reconnectTimer = nil;
	[self failReconnect];
}

// --- Host-side reconnect grace ---

- (void)beginPeerReconnectGrace:(NSString *)peerId
{
	_metricGraceStart++;
	NSString *safePeerId = safeString(peerId);
	bleLog([NSString stringWithFormat:@"beginPeerReconnectGrace peer=%@", safePeerId]);

	_rosterStatus[safePeerId] = @"reconnecting";
	_membershipEpoch++;
	if (_owner != nullptr)
		_owner->onPeerStatus(fromNSString(safePeerId), "reconnecting");
	[self broadcastRosterSnapshot];

	NSTimer *timer = [NSTimer scheduledTimerWithTimeInterval:RECONNECT_TIMEOUT_SECONDS
		target:self
		selector:@selector(peerReconnectGraceFired:)
		userInfo:@{@"peerId": safePeerId}
		repeats:NO];
	_peerReconnectTimers[safePeerId] = timer;
}

- (void)expirePeerReconnectGrace:(NSString *)peerId
{
	_metricGraceExpire++;
	NSString *safePeerId = safeString(peerId);
	bleLog([NSString stringWithFormat:@"expirePeerReconnectGrace peer=%@", safePeerId]);
	[self removeSessionPeerId:safePeerId];
	[_rosterStatus removeObjectForKey:safePeerId];
	_membershipEpoch++;
	if (_owner != nullptr)
	{
		_owner->onPeerLeft(fromNSString(safePeerId), "timeout");
		[self notifyPeerLeft:safePeerId reason:@"timeout"];
	}
	[self broadcastRosterSnapshot];
}

- (void)cancelPeerReconnectGrace:(NSString *)peerId
{
	NSTimer *timer = _peerReconnectTimers[safeString(peerId)];
	if (timer != nil)
	{
		[timer invalidate];
		[_peerReconnectTimers removeObjectForKey:safeString(peerId)];
	}
}

- (void)cancelAllPeerReconnectGraces
{
	for (NSTimer *timer in [_peerReconnectTimers allValues])
		[timer invalidate];
	[_peerReconnectTimers removeAllObjects];
}

- (BOOL)isPeerInReconnectGrace:(NSString *)peerId
{
	return _peerReconnectTimers[safeString(peerId)] != nil;
}

- (void)peerReconnectGraceFired:(NSTimer *)timer
{
	NSString *peerId = timer.userInfo[@"peerId"];
	if (peerId == nil)
		return;
	[_peerReconnectTimers removeObjectForKey:peerId];
	[self expirePeerReconnectGrace:peerId];
}

- (NSString *)debugStateString
{
	NSMutableString *s = [NSMutableString string];
	[s appendFormat:@"hosting=%d scanning=%d\n", _hosting ? 1 : 0, _scanning ? 1 : 0];
	[s appendFormat:@"local_id=%@\n", _localPeerId ?: @""];
	[s appendFormat:@"session=%@ transport=%@\n", _sessionId ?: @"", _transport ?: @""];
	[s appendFormat:@"clients=%lu pending=%lu\n", (unsigned long) _connectedClients.count, (unsigned long) _pendingClients.count];

	for (NSString *peerId in [_connectedClients allKeys])
		[s appendFormat:@"  client: %@\n", peerId];

	[s appendFormat:@"host_announced=%d service_ready=%d\n", _hostAnnounced ? 1 : 0, _hostServiceReady ? 1 : 0];
	[s appendFormat:@"client_joined=%d write_inflight=%d queue=%lu\n", _clientJoined ? 1 : 0, _clientWriteInFlight ? 1 : 0, (unsigned long) _clientWriteQueue.count];
	[s appendFormat:@"notify_queues=%lu\n", (unsigned long) _notificationQueues.count];

	for (NSString *key in [_notificationQueues allKeys])
	{
		NSMutableArray<NSData *> *q = _notificationQueues[key];
		[s appendFormat:@"  queue[%@]=%lu fragments\n", key, (unsigned long) q.count];
	}

	[s appendFormat:@"heartbeat=%@ last_broadcast=%lu bytes\n",
		_heartbeatTimer != nil ? @"active" : @"off",
		(unsigned long) (_lastBroadcastPacketData ? _lastBroadcastPacketData.length : 0)];

	[s appendFormat:@"metrics msg_out=%ld msg_in=%ld ctrl_out=%ld ctrl_in=%ld\n", (long)_metricMsgOut, (long)_metricMsgIn, (long)_metricCtrlOut, (long)_metricCtrlIn];
	[s appendFormat:@"metrics heartbeat_tx=%ld heartbeat_rx=%ld dedup_hit=%ld\n", (long)_metricHeartbeatTx, (long)_metricHeartbeatRx, (long)_metricDedupHit];
	[s appendFormat:@"metrics frag_tx=%ld frag_rx=%ld frag_drop=%ld asm_timeout=%ld\n", (long)_metricFragmentTx, (long)_metricFragmentRx, (long)_metricFragmentDrop, (long)_metricAssemblyTimeout];
	[s appendFormat:@"metrics write_fail=%ld reconn_try=%ld reconn_ok=%ld reconn_fail=%ld\n", (long)_metricWriteFail, (long)_metricReconnectAttempt, (long)_metricReconnectSuccess, (long)_metricReconnectFail];
	[s appendFormat:@"metrics grace_start=%ld grace_expire=%ld grace_resume=%ld\n", (long)_metricGraceStart, (long)_metricGraceExpire, (long)_metricGraceResume];
	[s appendFormat:@"metrics roster_req=%ld join_reject=%ld\n", (long)_metricRosterRequest, (long)_metricJoinReject];

	return s;
}

@end

namespace love
{
namespace ble
{
namespace apple
{

Ble::Ble()
	: love::ble::Ble("love.ble.apple")
{
	instance = this;
	impl = new Impl();
	impl->manager = [[LoveBleManager alloc] initWithOwner:this];
}

Ble::~Ble()
{
	if (instance == this)
		instance = nullptr;

	if (impl != nullptr)
	{
		[impl->manager shutdown];
		Impl *old = impl;
		impl = nullptr;
		delete old;
	}
}

Ble::RadioState Ble::getRadioState() const
{
	return impl != nullptr ? [impl->manager radioState] : RADIO_UNSUPPORTED;
}

bool Ble::ensureAvailable(const char *errorCode, const char *verb) const
{
	RadioState state = getRadioState();
	if (state == RADIO_UNSUPPORTED)
	{
		const_cast<Ble *>(this)->pushError("transport_unavailable", "Bluetooth LE is not available on this device.");
		return false;
	}

	if (state == RADIO_UNAUTHORIZED)
	{
		const_cast<Ble *>(this)->pushError(errorCode, std::string("Bluetooth permission is required to ") + verb + ".");
		return false;
	}

	if (state != RADIO_ON)
	{
		const_cast<Ble *>(this)->pushError(errorCode, std::string("Bluetooth radio must be on to ") + verb + ".");
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

void Ble::clearSessionLocked()
{
	localId.clear();
	roomId.clear();
	sessionId.clear();
	hostActive = false;
	peers.clear();
}

void Ble::host(const std::string &room, int maxClients, Transport transport, const ReliabilityConfig &reliability)
{
	if (!ensureAvailable("host_failed", "host a BLE room"))
		return;

	if (impl != nullptr)
	{
		[impl->manager applyReliabilityConfig:reliability];
		[impl->manager hostRoom:room maxClients:maxClients transport:transport];
	}
}

void Ble::scan()
{
	if (!ensureAvailable("scan_failed", "scan for BLE rooms"))
		return;

	if (impl != nullptr)
		[impl->manager scanRooms];
}

void Ble::join(const std::string &roomIdValue, const ReliabilityConfig &reliability)
{
	if (!ensureAvailable("join_failed", "join a BLE room"))
		return;

	if (impl != nullptr)
	{
		[impl->manager applyReliabilityConfig:reliability];
		[impl->manager joinRoomId:roomIdValue];
	}
}

void Ble::leave()
{
	if (impl != nullptr)
		[impl->manager leave];
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

	if (impl != nullptr)
		[impl->manager broadcastMessageType:msgType payload:bytes];
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

	if (impl != nullptr)
		[impl->manager sendPeerId:peerId messageType:msgType payload:bytes];
}

std::string Ble::getLocalId() const
{
	std::lock_guard<std::mutex> lock(stateMutex);
	return localId;
}

std::string Ble::getDeviceAddress() const
{
	@autoreleasepool
	{
		NSUUID *identifier = [UIDevice currentDevice].identifierForVendor;
		NSString *uuid = identifier.UUIDString;
		if (uuid != nil)
			return uuid.UTF8String != nullptr ? uuid.UTF8String : "";
	}

	return "";
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
	if (impl == nullptr || impl->manager == nil)
		return "not initialized";

	NSString *s = [impl->manager debugStateString];
	return s ? std::string([s UTF8String]) : "unavailable";
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

void Ble::onPeerStatus(const std::string &peerId, const std::string &status)
{
	Event event;
	event.type = "peer_status";
	event.fields.emplace_back("peer_id", makeStringVariant(peerId));
	event.fields.emplace_back("status", makeStringVariant(status));
	pushEvent(event);
}

void Ble::onError(const std::string &code, const std::string &detail)
{
	pushError(code, detail);
}

void Ble::onDiagnostic(const std::string &message)
{
	pushDiagnostic("ios", message);
}

} // apple
} // ble
} // love

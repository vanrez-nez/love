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

#import <CoreBluetooth/CoreBluetooth.h>
#import <Foundation/Foundation.h>

// ──────────────────────────────────────────────────
// Protocol constants (spec Section 2)
// ──────────────────────────────────────────────────

static NSString *const kServiceUUID = @"4bdf6b6d-6b77-4b3f-9f4a-5a2d1499d641";
static NSString *const kCharacteristicUUID = @"9e153f71-c2d0-4ee1-8b8d-090421bea607";
static NSString *const kAdvertPrefix = @"LB1";

// Timeouts and limits (spec Section 17)
static const NSTimeInterval kRoomExpirySeconds = 4.0;
static const NSTimeInterval kAssemblyTimeoutSeconds = 15.0;
static const NSTimeInterval kPendingClientTimeout = 5.0;
static const NSTimeInterval kHeartbeatInterval = 2.0;
static const NSTimeInterval kReconnectTimeoutSeconds = 10.0;
static const int kMaxConcurrentAssembliesPerSource = 32;
static const int kDedupExpiry = 5;
static const int kDedupWindow = 64;
static const uint8_t kProtocolVersion = 1;
static const int kDesiredMTU = 185;
static const int kDefaultMTU = 23;
static const int kATTOverhead = 3;
static const int kFragmentHeaderSize = 5;
static const int kMaxStringLength = 4096;
static const int kMaxPayloadLength = 65536;

// ──────────────────────────────────────────────────
// Room data extracted from advertisement (spec Section 3.1)
// ──────────────────────────────────────────────────

@interface LoveBleRoom : NSObject
@property (nonatomic, copy) NSString *roomId;
@property (nonatomic, copy) NSString *sessionId;
@property (nonatomic, copy) NSString *hostPeerId;
@property (nonatomic, assign) char transport;
@property (nonatomic, assign) int maxClients;
@property (nonatomic, assign) int peerCount;
@property (nonatomic, copy) NSString *roomName;
@property (nonatomic, assign) NSInteger rssi;
@property (nonatomic, strong) NSDate *lastSeenAt;
@property (nonatomic, strong) CBPeripheral *peripheral;
@end

@implementation LoveBleRoom
@end

// ──────────────────────────────────────────────────
// Fragment assembly tracking (spec Section 5.4)
// ──────────────────────────────────────────────────

@interface LoveBleAssembly : NSObject
@property (nonatomic, assign) uint8_t count;
@property (nonatomic, assign) uint8_t receivedCount;
@property (nonatomic, strong) NSMutableArray<NSData *> *slots;
@property (nonatomic, strong) NSDate *updatedAt;
@end

@implementation LoveBleAssembly
- (instancetype)initWithCount:(uint8_t)count
{
	self = [super init];
	if (self)
	{
		_count = count;
		_receivedCount = 0;
		_updatedAt = [NSDate date];
		_slots = [NSMutableArray arrayWithCapacity:count];
		for (uint8_t i = 0; i < count; i++)
			[_slots addObject:(NSData *)[NSNull null]];
	}
	return self;
}
@end

// ──────────────────────────────────────────────────
// Dedup entry (spec Section 10)
// ──────────────────────────────────────────────────

@interface LoveBleDedupEntry : NSObject
@property (nonatomic, copy) NSString *key;
@property (nonatomic, strong) NSDate *timestamp;
@end

@implementation LoveBleDedupEntry
@end

// ──────────────────────────────────────────────────
// Session peer entry for roster tracking
// ──────────────────────────────────────────────────

@interface LoveBleSessionPeer : NSObject
@property (nonatomic, copy) NSString *peerId;
@property (nonatomic, assign) BOOL isHost;
@property (nonatomic, copy) NSString *status; // "connected" or "reconnecting"
@end

@implementation LoveBleSessionPeer
@end

// ──────────────────────────────────────────────────
// Static helpers
// ──────────────────────────────────────────────────

// Generate a 6-character hex string from random bytes (spec: Peer ID / Session ID)
static NSString *generateShortID()
{
	uint8_t bytes[3];
	arc4random_buf(bytes, 3);
	return [NSString stringWithFormat:@"%02x%02x%02x", bytes[0], bytes[1], bytes[2]];
}

// Spec Section 3.2: NormalizeRoomName
static NSString *normalizeRoomName(NSString *name)
{
	if (name == nil || name.length == 0)
		return @"Room";

	NSMutableString *result = [name mutableCopy];
	[result replaceOccurrencesOfString:@"|" withString:@" " options:0 range:NSMakeRange(0, result.length)];
	[result replaceOccurrencesOfString:@"\n" withString:@" " options:0 range:NSMakeRange(0, result.length)];
	[result replaceOccurrencesOfString:@"\r" withString:@" " options:0 range:NSMakeRange(0, result.length)];

	NSString *trimmed = [result stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];

	if (trimmed.length == 0)
		return @"Room";

	if (trimmed.length > 8)
		trimmed = [trimmed substringToIndex:8];

	return trimmed;
}

// Spec Section 3.1: Encode room advertisement string
static NSString *encodeRoomAdvertisement(NSString *sessionId, NSString *hostPeerId,
                                          char transport, int maxClients, int peerCount,
                                          NSString *roomName)
{
	return [NSString stringWithFormat:@"LB1%@%@%c%d%d%@",
		sessionId, hostPeerId, transport,
		maxClients, peerCount, roomName];
}

// Spec Section 3.4: Decode room from advertisement string
static LoveBleRoom *decodeRoomFromString(NSString *str, NSString *roomId, NSInteger rssi)
{
	if (str == nil || str.length < 18)
		return nil;

	if (![str hasPrefix:@"LB1"])
		return nil;

	LoveBleRoom *room = [[LoveBleRoom alloc] init];
	room.roomId = roomId;
	room.sessionId = [str substringWithRange:NSMakeRange(3, 6)];
	room.hostPeerId = [str substringWithRange:NSMakeRange(9, 6)];
	room.transport = [str characterAtIndex:15];
	room.maxClients = [str characterAtIndex:16] - '0';
	room.peerCount = [str characterAtIndex:17] - '0';
	room.roomName = (str.length > 18) ? [str substringFromIndex:18] : @"";
	room.rssi = rssi;
	room.lastSeenAt = [NSDate date];

	if (room.transport != 'r' && room.transport != 's')
		return nil;

	if (room.maxClients < 1 || room.maxClients > 7)
		return nil;

	if (room.peerCount < 0 || room.peerCount > 9)
		return nil;

	return room;
}

// Spec Section 3.4: Attempt decode from advertisement data (priority order)
static LoveBleRoom *decodeRoomFromAdvertisement(NSDictionary *advertisementData,
                                                 NSString *roomId, NSInteger rssi)
{
	// 1. Manufacturer Data with company ID 0xFFFF
	NSData *mfrData = advertisementData[CBAdvertisementDataManufacturerDataKey];
	if (mfrData && mfrData.length >= 2)
	{
		const uint8_t *bytes = (const uint8_t *)mfrData.bytes;
		uint16_t companyId = bytes[0] | (bytes[1] << 8);
		if (companyId == 0xFFFF && mfrData.length > 2)
		{
			NSString *payload = [[NSString alloc] initWithBytes:bytes + 2
			                                            length:mfrData.length - 2
			                                          encoding:NSUTF8StringEncoding];
			LoveBleRoom *room = decodeRoomFromString(payload, roomId, rssi);
			if (room) return room;
		}
	}

	// 2. Service Data for our service UUID
	NSDictionary *serviceData = advertisementData[CBAdvertisementDataServiceDataKey];
	if (serviceData)
	{
		CBUUID *serviceUUID = [CBUUID UUIDWithString:kServiceUUID];
		NSData *data = serviceData[serviceUUID];
		if (data)
		{
			NSString *payload = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
			LoveBleRoom *room = decodeRoomFromString(payload, roomId, rssi);
			if (room) return room;
		}
	}

	// 3. Local Name
	NSString *localName = advertisementData[CBAdvertisementDataLocalNameKey];
	{
		LoveBleRoom *room = decodeRoomFromString(localName, roomId, rssi);
		if (room) return room;
	}

	return nil;
}

// ──────────────────────────────────────────────────
// Event push helpers
// ──────────────────────────────────────────────────

static love::Variant makeStringVariant(NSString *str)
{
	const char *utf8 = str.UTF8String;
	return love::Variant(utf8, strlen(utf8));
}

static void pushRoomFoundEvent(love::ble::Ble *owner, LoveBleRoom *room)
{
	love::ble::Ble::BleEvent event;
	event.type = "room_found";
	event.fields["room_id"] = makeStringVariant(room.roomId);
	event.fields["session_id"] = makeStringVariant(room.sessionId);
	event.fields["name"] = makeStringVariant(room.roomName);

	const char *transportStr = (room.transport == 's') ? "resilient" : "reliable";
	event.fields["transport"] = love::Variant(transportStr, strlen(transportStr));
	event.fields["peer_count"] = love::Variant((double)room.peerCount);
	event.fields["max"] = love::Variant((double)room.maxClients);
	event.fields["rssi"] = love::Variant((double)room.rssi);

	owner->pushEvent(event);
}

static void pushRoomLostEvent(love::ble::Ble *owner, NSString *roomId)
{
	love::ble::Ble::BleEvent event;
	event.type = "room_lost";
	event.fields["room_id"] = makeStringVariant(roomId);
	owner->pushEvent(event);
}

static void pushDiagnosticEvent(love::ble::Ble *owner, NSString *message)
{
	love::ble::Ble::BleEvent event;
	event.type = "diagnostic";
	event.fields["platform"] = love::Variant("ios", 3);
	event.fields["message"] = makeStringVariant(message);
	owner->pushEvent(event);
}

static void pushErrorEvent(love::ble::Ble *owner, NSString *code, NSString *detail)
{
	love::ble::Ble::BleEvent event;
	event.type = "error";
	event.fields["code"] = makeStringVariant(code);
	if (detail)
		event.fields["detail"] = makeStringVariant(detail);
	owner->pushEvent(event);
}

// ──────────────────────────────────────────────────
// Packet encoding (spec Section 4.1)
// ──────────────────────────────────────────────────

static void appendUint8(NSMutableData *data, uint8_t val)
{
	[data appendBytes:&val length:1];
}

static void appendUint16BE(NSMutableData *data, uint16_t val)
{
	uint8_t bytes[2] = { (uint8_t)(val >> 8), (uint8_t)(val & 0xFF) };
	[data appendBytes:bytes length:2];
}

static void appendUint32BE(NSMutableData *data, uint32_t val)
{
	uint8_t bytes[4] = {
		(uint8_t)((val >> 24) & 0xFF),
		(uint8_t)((val >> 16) & 0xFF),
		(uint8_t)((val >> 8) & 0xFF),
		(uint8_t)(val & 0xFF)
	};
	[data appendBytes:bytes length:4];
}

static void appendLengthPrefixedString(NSMutableData *data, NSString *str)
{
	NSData *strData = [str dataUsingEncoding:NSUTF8StringEncoding];
	appendUint32BE(data, (uint32_t)strData.length);
	[data appendData:strData];
}

static void appendLengthPrefixedBytes(NSMutableData *data, const uint8_t *bytes, uint32_t len)
{
	appendUint32BE(data, len);
	if (len > 0)
		[data appendBytes:bytes length:len];
}

// Build a complete packet per spec Section 4.1
static NSData *buildPacket(NSString *kind, NSString *fromPeerId, NSString *toPeerId,
                            NSString *msgType, uint16_t messageId,
                            const uint8_t *payload, uint32_t payloadLen)
{
	NSMutableData *data = [NSMutableData data];
	appendUint8(data, kProtocolVersion);                // Version
	appendUint16BE(data, messageId);                    // MessageID
	appendLengthPrefixedString(data, kind);             // Kind
	appendLengthPrefixedString(data, fromPeerId);       // FromPeerID
	appendLengthPrefixedString(data, toPeerId);         // ToPeerID
	appendLengthPrefixedString(data, msgType);          // MsgType
	appendLengthPrefixedBytes(data, payload, payloadLen); // Payload
	return data;
}

// ──────────────────────────────────────────────────
// Packet decoding (spec Section 4.1)
// ──────────────────────────────────────────────────

// Parsed packet structure
@interface LoveBlePacket : NSObject
@property (nonatomic, assign) uint8_t version;
@property (nonatomic, assign) uint16_t messageId;
@property (nonatomic, copy) NSString *kind;
@property (nonatomic, copy) NSString *fromPeerId;
@property (nonatomic, copy) NSString *toPeerId;
@property (nonatomic, copy) NSString *msgType;
@property (nonatomic, strong) NSData *payload;
@end

@implementation LoveBlePacket
@end

static uint16_t readUint16BE(const uint8_t *bytes)
{
	return ((uint16_t)bytes[0] << 8) | bytes[1];
}

static uint32_t readUint32BE(const uint8_t *bytes)
{
	return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
	       ((uint32_t)bytes[2] << 8) | bytes[3];
}

static LoveBlePacket *decodePacket(NSData *data)
{
	const uint8_t *bytes = (const uint8_t *)data.bytes;
	NSUInteger length = data.length;
	NSUInteger offset = 0;

	// Version (1 byte)
	if (offset + 1 > length) return nil;
	uint8_t version = bytes[offset++];
	if (version != kProtocolVersion) return nil; // Spec: version mismatch = silent drop

	// MessageID (2 bytes, big-endian)
	if (offset + 2 > length) return nil;
	uint16_t messageId = readUint16BE(bytes + offset);
	offset += 2;

	// Kind (length-prefixed string)
	if (offset + 4 > length) return nil;
	uint32_t kindLen = readUint32BE(bytes + offset);
	offset += 4;
	if (kindLen > kMaxStringLength || offset + kindLen > length) return nil;
	NSString *kind = [[NSString alloc] initWithBytes:bytes + offset length:kindLen encoding:NSUTF8StringEncoding];
	offset += kindLen;
	if (!kind) return nil;

	// FromPeerID (length-prefixed string)
	if (offset + 4 > length) return nil;
	uint32_t fromLen = readUint32BE(bytes + offset);
	offset += 4;
	if (fromLen > kMaxStringLength || offset + fromLen > length) return nil;
	NSString *fromPeerId = [[NSString alloc] initWithBytes:bytes + offset length:fromLen encoding:NSUTF8StringEncoding];
	offset += fromLen;
	if (!fromPeerId) fromPeerId = @"";

	// ToPeerID (length-prefixed string)
	if (offset + 4 > length) return nil;
	uint32_t toLen = readUint32BE(bytes + offset);
	offset += 4;
	if (toLen > kMaxStringLength || offset + toLen > length) return nil;
	NSString *toPeerId = [[NSString alloc] initWithBytes:bytes + offset length:toLen encoding:NSUTF8StringEncoding];
	offset += toLen;
	if (!toPeerId) toPeerId = @"";

	// MsgType (length-prefixed string)
	if (offset + 4 > length) return nil;
	uint32_t typeLen = readUint32BE(bytes + offset);
	offset += 4;
	if (typeLen > kMaxStringLength || offset + typeLen > length) return nil;
	NSString *msgType = [[NSString alloc] initWithBytes:bytes + offset length:typeLen encoding:NSUTF8StringEncoding];
	offset += typeLen;
	if (!msgType) return nil;

	// Payload (length-prefixed bytes)
	if (offset + 4 > length) return nil;
	uint32_t payloadLen = readUint32BE(bytes + offset);
	offset += 4;
	if (payloadLen > kMaxPayloadLength || offset + payloadLen > length) return nil;
	NSData *payload = [NSData dataWithBytes:bytes + offset length:payloadLen];
	offset += payloadLen;

	LoveBlePacket *packet = [[LoveBlePacket alloc] init];
	packet.version = version;
	packet.messageId = messageId;
	packet.kind = kind;
	packet.fromPeerId = fromPeerId;
	packet.toPeerId = toPeerId;
	packet.msgType = msgType;
	packet.payload = payload;
	return packet;
}

// ──────────────────────────────────────────────────
// Objective-C implementation
// ──────────────────────────────────────────────────

@interface LoveBleImpl : NSObject <CBCentralManagerDelegate, CBPeripheralManagerDelegate, CBPeripheralDelegate>

// Core managers
@property (nonatomic, strong) CBCentralManager *centralManager;
@property (nonatomic, strong) CBPeripheralManager *peripheralManager;
@property (nonatomic, assign) love::ble::Ble *owner;

// Radio state
@property (nonatomic, assign) love::ble::Ble::RadioState radioState;

// Local identity
@property (nonatomic, copy) NSString *localPeerId;

// ── Host state ──
@property (nonatomic, assign) BOOL hosting;
@property (nonatomic, copy) NSString *sessionId;
@property (nonatomic, copy) NSString *roomName;
@property (nonatomic, assign) int maxClients;
@property (nonatomic, assign) char transportChar;
@property (nonatomic, assign) int peerCount;
@property (nonatomic, assign) int membershipEpoch;
@property (nonatomic, strong) CBMutableService *gattService;
@property (nonatomic, strong) CBMutableCharacteristic *messageCharacteristic;
@property (nonatomic, assign) BOOL hostServiceReady;

// Host: connected clients map (peerId -> CBCentral)
@property (nonatomic, strong) NSMutableDictionary<NSString *, CBCentral *> *connectedClients;
// Host: device-peer map (device UUID string -> peerId)
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSString *> *devicePeerMap;
// Host: pending clients (device UUID string -> NSDate timestamp)
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSDate *> *pendingClients;
// Host: per-device notification queues (device UUID string -> NSMutableArray<NSData *>)
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSMutableArray<NSData *> *> *notificationQueues;
// Host: per-device MTU (device UUID string -> NSNumber)
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSNumber *> *deviceMTUs;
// Host: subscribed centrals for notifications
@property (nonatomic, strong) NSMutableSet<CBCentral *> *subscribedCentrals;

// ── Client state ──
@property (nonatomic, assign) BOOL clientJoined;
@property (nonatomic, assign) BOOL clientLeaving;
@property (nonatomic, copy) NSString *joinedRoomId;
@property (nonatomic, copy) NSString *joinedSessionId;
@property (nonatomic, copy) NSString *hostPeerId;
@property (nonatomic, strong) CBPeripheral *connectedPeripheral;
@property (nonatomic, strong) CBCharacteristic *remoteCharacteristic;
@property (nonatomic, assign) int negotiatedMTU;

// Client: write queue (spec Section 15.1)
@property (nonatomic, strong) NSMutableArray<NSData *> *clientWriteQueue;
@property (nonatomic, assign) BOOL writeInFlight;

// ── Scan state ──
@property (nonatomic, assign) BOOL scanning;
@property (nonatomic, strong) NSMutableDictionary<NSString *, LoveBleRoom *> *discoveredRooms;
@property (nonatomic, strong) NSTimer *roomExpiryTimer;

// ── Session peers roster ──
@property (nonatomic, strong) NSMutableArray<LoveBleSessionPeer *> *sessionPeers;

// ── Fragment assembler: sourceKey -> (assemblyKey -> LoveBleAssembly) ──
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSMutableDictionary<NSString *, LoveBleAssembly *> *> *assemblerBySource;

// ── Nonce and MessageID counters ──
@property (nonatomic, assign) uint16_t nonceCounter;
@property (nonatomic, assign) uint16_t messageIdCounter;

// ── Dedup state (spec Section 10) ──
@property (nonatomic, strong) NSMutableArray<LoveBleDedupEntry *> *dedupList;
@property (nonatomic, strong) NSMutableSet<NSString *> *dedupSet;

// ── Reconnect grace timers (host side, peerId -> NSTimer) ──
@property (nonatomic, strong) NSMutableDictionary<NSString *, NSTimer *> *graceTimers;

// ── Heartbeat (spec Section 9) ──
@property (nonatomic, strong) NSTimer *heartbeatTimer;
@property (nonatomic, strong) NSData *lastBroadcastPacket;
@property (nonatomic, assign) uint16_t lastBroadcastMessageId;
@property (nonatomic, assign) BOOL rosterRequestSentThisInterval;

// ── Migration state (spec Section 8) ──
@property (nonatomic, assign) BOOL migrationInProgress;
@property (nonatomic, strong) NSString *migrationSuccessorId;
@property (nonatomic, strong) NSString *migrationSessionId;
@property (nonatomic, assign) int migrationMaxClients;
@property (nonatomic, strong) NSString *migrationRoomName;
@property (nonatomic, assign) int migrationEpoch;
@property (nonatomic, assign) BOOL becomingHost;
@property (nonatomic, strong) NSTimer *migrationTimer;

// ── Reconnect state (spec Section 7.1) ──
@property (nonatomic, assign) BOOL reconnectInProgress;
@property (nonatomic, strong) NSString *reconnectSessionId;
@property (nonatomic, strong) NSString *reconnectHostPeerId;
@property (nonatomic, strong) NSTimer *reconnectTimer;
@property (nonatomic, assign) BOOL reconnectScanInProgress;
@property (nonatomic, assign) BOOL reconnectJoinInProgress;

@end

@implementation LoveBleImpl

- (instancetype)initWithOwner:(love::ble::Ble *)owner
{
	self = [super init];
	if (self)
	{
		_owner = owner;
		_radioState = love::ble::Ble::RADIO_OFF;
		_localPeerId = generateShortID();
		_hosting = NO;
		_scanning = NO;
		_peerCount = 0;
		_membershipEpoch = 0;
		_hostServiceReady = NO;
		_clientJoined = NO;
		_clientLeaving = NO;
		_writeInFlight = NO;
		_negotiatedMTU = kDefaultMTU;
		_nonceCounter = 0;
		_messageIdCounter = 0;
		_migrationInProgress = NO;
		_becomingHost = NO;
		_migrationEpoch = 0;
		_migrationMaxClients = 0;

		_reconnectInProgress = NO;
		_reconnectScanInProgress = NO;
		_reconnectJoinInProgress = NO;

		_discoveredRooms = [NSMutableDictionary dictionary];
		_connectedClients = [NSMutableDictionary dictionary];
		_devicePeerMap = [NSMutableDictionary dictionary];
		_pendingClients = [NSMutableDictionary dictionary];
		_notificationQueues = [NSMutableDictionary dictionary];
		_deviceMTUs = [NSMutableDictionary dictionary];
		_subscribedCentrals = [NSMutableSet set];
		_clientWriteQueue = [NSMutableArray array];
		_sessionPeers = [NSMutableArray array];
		_assemblerBySource = [NSMutableDictionary dictionary];
		_dedupList = [NSMutableArray array];
		_dedupSet = [NSMutableSet set];
		_graceTimers = [NSMutableDictionary dictionary];

		_centralManager = [[CBCentralManager alloc] initWithDelegate:self
		                                                       queue:nil
		                                                     options:@{CBCentralManagerOptionShowPowerAlertKey: @NO}];
		_peripheralManager = [[CBPeripheralManager alloc] initWithDelegate:self
		                                                             queue:nil
		                                                           options:nil];
	}
	return self;
}

- (void)dealloc
{
	[self stopRoomExpiryTimer];
	[self stopHeartbeat];
	[self cancelAllGraceTimers];
	[_migrationTimer invalidate];
	_migrationTimer = nil;
	[_reconnectTimer invalidate];
	_reconnectTimer = nil;
}

// ──────────────────────────────────────────────────
// Nonce and MessageID (spec Section 5.3 and 4.1)
// ──────────────────────────────────────────────────

// Spec Section 5.3: NextNonce — wraps 65535->1, 0 is reserved
- (uint16_t)nextNonce
{
	_nonceCounter++;
	if (_nonceCounter == 0)
		_nonceCounter = 1;
	return _nonceCounter;
}

// NextMessageID — same 16-bit wrapping pattern as nonce
- (uint16_t)nextMessageId
{
	_messageIdCounter++;
	if (_messageIdCounter == 0)
		_messageIdCounter = 1;
	return _messageIdCounter;
}

// ──────────────────────────────────────────────────
// Fragmentation (spec Section 5.2)
// ──────────────────────────────────────────────────

- (NSArray<NSData *> *)fragmentPacket:(NSData *)packetBytes payloadLimit:(int)payloadLimit
{
	int chunkSize = payloadLimit - kFragmentHeaderSize;
	if (chunkSize <= 0)
	{
		pushErrorEvent(_owner, @"send_failed", nil);
		return nil;
	}

	int packetLen = (int)packetBytes.length;
	int fragmentCount = (packetLen + chunkSize - 1) / chunkSize;
	if (fragmentCount > 255)
	{
		pushErrorEvent(_owner, @"payload_too_large", nil);
		return nil;
	}

	uint16_t nonce = [self nextNonce];
	uint8_t nonceHigh = (uint8_t)(nonce >> 8);
	uint8_t nonceLow = (uint8_t)(nonce & 0xFF);
	const uint8_t *rawBytes = (const uint8_t *)packetBytes.bytes;

	NSMutableArray<NSData *> *fragments = [NSMutableArray arrayWithCapacity:fragmentCount];
	for (int i = 0; i < fragmentCount; i++)
	{
		int start = i * chunkSize;
		int end = MIN(start + chunkSize, packetLen);
		int chunkLen = end - start;

		NSMutableData *fragment = [NSMutableData dataWithCapacity:kFragmentHeaderSize + chunkLen];
		uint8_t header[5] = { kProtocolVersion, nonceHigh, nonceLow, (uint8_t)i, (uint8_t)fragmentCount };
		[fragment appendBytes:header length:5];
		[fragment appendBytes:rawBytes + start length:chunkLen];
		[fragments addObject:fragment];
	}
	return fragments;
}

// ──────────────────────────────────────────────────
// Fragment reassembly (spec Section 5.4)
// ──────────────────────────────────────────────────

- (NSData *)processIncomingFragment:(NSString *)sourceKey fragmentData:(NSData *)fragmentData
{
	// Step 1: < 5 bytes => reject silently
	if (fragmentData.length < kFragmentHeaderSize)
		return nil;

	const uint8_t *bytes = (const uint8_t *)fragmentData.bytes;

	// Step 2: Parse header
	uint8_t version = bytes[0];
	uint16_t nonce = ((uint16_t)bytes[1] << 8) | bytes[2];
	uint8_t index = bytes[3];
	uint8_t count = bytes[4];

	// Step 3: version != 1 => reject silently
	if (version != kProtocolVersion)
		return nil;

	// Step 4: count == 0 => reject silently
	if (count == 0)
		return nil;

	// Step 5: index >= count => reject silently
	if (index >= count)
		return nil;

	NSData *chunk = [fragmentData subdataWithRange:NSMakeRange(kFragmentHeaderSize, fragmentData.length - kFragmentHeaderSize)];

	// Step 6: count == 1 => fast path, return payload immediately
	if (count == 1)
		return chunk;

	// Step 7: assemblyKey = sourceKey + ":" + nonce
	NSString *assemblyKey = [NSString stringWithFormat:@"%@:%u", sourceKey, nonce];

	// Get or create source map
	NSMutableDictionary<NSString *, LoveBleAssembly *> *sourceMap = _assemblerBySource[sourceKey];
	if (!sourceMap)
	{
		sourceMap = [NSMutableDictionary dictionary];
		_assemblerBySource[sourceKey] = sourceMap;
	}

	// Expire old assemblies for this source (spec Section 5.5)
	[self expireAssemblies:sourceMap];

	// Step 8: Max concurrent assemblies per source
	if ((int)sourceMap.count >= kMaxConcurrentAssembliesPerSource && !sourceMap[assemblyKey])
	{
		// Discard oldest assembly
		NSString *oldestKey = nil;
		NSDate *oldestDate = nil;
		for (NSString *key in sourceMap)
		{
			LoveBleAssembly *a = sourceMap[key];
			if (!oldestDate || [a.updatedAt compare:oldestDate] == NSOrderedAscending)
			{
				oldestDate = a.updatedAt;
				oldestKey = key;
			}
		}
		if (oldestKey)
			[sourceMap removeObjectForKey:oldestKey];
	}

	// Step 9: Look up or create assembly
	LoveBleAssembly *assembly = sourceMap[assemblyKey];
	if (!assembly)
	{
		// Step 10: Create new assembly
		assembly = [[LoveBleAssembly alloc] initWithCount:count];
		sourceMap[assemblyKey] = assembly;
	}
	else
	{
		// Step 11: Existing assembly count mismatch => discard
		if (assembly.count != count)
		{
			[sourceMap removeObjectForKey:assemblyKey];
			return nil;
		}
	}

	// Step 12: Check slot
	id existingSlot = assembly.slots[index];
	if (existingSlot != [NSNull null])
	{
		NSData *existingData = (NSData *)existingSlot;
		if ([existingData isEqualToData:chunk])
		{
			// 12a: benign duplicate, ignore
			return nil;
		}
		else
		{
			// 12b: conflict, discard entire assembly
			[sourceMap removeObjectForKey:assemblyKey];
			return nil;
		}
	}

	// Step 13: Store chunk
	assembly.slots[index] = chunk;
	assembly.receivedCount++;
	assembly.updatedAt = [NSDate date];

	// Step 14: Incomplete?
	if (assembly.receivedCount < assembly.count)
		return nil;

	// Step 15: Concatenate all slots
	NSMutableData *reassembled = [NSMutableData data];
	for (uint8_t i = 0; i < assembly.count; i++)
		[reassembled appendData:assembly.slots[i]];

	// Step 16: Remove assembly
	[sourceMap removeObjectForKey:assemblyKey];

	// Step 17: Check total length
	if (reassembled.length > kMaxPayloadLength)
	{
		pushErrorEvent(_owner, @"payload_too_large", nil);
		return nil;
	}

	// Step 18: Return reassembled bytes
	return reassembled;
}

// Spec Section 5.5: Expire assemblies older than 15 seconds
- (void)expireAssemblies:(NSMutableDictionary<NSString *, LoveBleAssembly *> *)sourceMap
{
	NSDate *now = [NSDate date];
	NSMutableArray<NSString *> *expired = [NSMutableArray array];
	for (NSString *key in sourceMap)
	{
		LoveBleAssembly *a = sourceMap[key];
		if ([now timeIntervalSinceDate:a.updatedAt] > kAssemblyTimeoutSeconds)
			[expired addObject:key];
	}
	for (NSString *key in expired)
		[sourceMap removeObjectForKey:key];
}

// ──────────────────────────────────────────────────
// Deduplication (spec Section 10)
// ──────────────────────────────────────────────────

- (BOOL)isDuplicate:(NSString *)fromPeerId msgType:(NSString *)msgType messageId:(uint16_t)messageId
{
	NSString *key = [NSString stringWithFormat:@"%@:%@:%u", fromPeerId, msgType, messageId];

	// Prune entries older than 5 seconds
	NSDate *now = [NSDate date];
	NSMutableArray<LoveBleDedupEntry *> *toRemove = [NSMutableArray array];
	for (LoveBleDedupEntry *entry in _dedupList)
	{
		if ([now timeIntervalSinceDate:entry.timestamp] > kDedupExpiry)
			[toRemove addObject:entry];
	}
	for (LoveBleDedupEntry *entry in toRemove)
	{
		[_dedupSet removeObject:entry.key];
		[_dedupList removeObject:entry];
	}

	// Prune if exceeding dedup window
	while ((int)_dedupList.count > kDedupWindow)
	{
		LoveBleDedupEntry *oldest = _dedupList.firstObject;
		[_dedupSet removeObject:oldest.key];
		[_dedupList removeObjectAtIndex:0];
	}

	// Check if duplicate
	if ([_dedupSet containsObject:key])
		return YES;

	// Add new entry
	LoveBleDedupEntry *entry = [[LoveBleDedupEntry alloc] init];
	entry.key = key;
	entry.timestamp = now;
	[_dedupList addObject:entry];
	[_dedupSet addObject:key];

	return NO;
}

- (void)clearDedupState
{
	[_dedupList removeAllObjects];
	[_dedupSet removeAllObjects];
}

// ──────────────────────────────────────────────────
// Session peers roster management
// ──────────────────────────────────────────────────

- (void)resetSessionPeers
{
	[_sessionPeers removeAllObjects];
}

- (void)addSessionPeer:(NSString *)peerId isHost:(BOOL)isHost status:(NSString *)status
{
	// Avoid duplicates
	for (LoveBleSessionPeer *p in _sessionPeers)
	{
		if ([p.peerId isEqualToString:peerId])
		{
			p.status = status;
			p.isHost = isHost;
			return;
		}
	}
	LoveBleSessionPeer *peer = [[LoveBleSessionPeer alloc] init];
	peer.peerId = peerId;
	peer.isHost = isHost;
	peer.status = status;
	[_sessionPeers addObject:peer];
}

- (void)removeSessionPeer:(NSString *)peerId
{
	NSMutableArray *toRemove = [NSMutableArray array];
	for (LoveBleSessionPeer *p in _sessionPeers)
	{
		if ([p.peerId isEqualToString:peerId])
			[toRemove addObject:p];
	}
	[_sessionPeers removeObjectsInArray:toRemove];
}

- (void)updateSessionPeerStatus:(NSString *)peerId status:(NSString *)status
{
	for (LoveBleSessionPeer *p in _sessionPeers)
	{
		if ([p.peerId isEqualToString:peerId])
		{
			p.status = status;
			return;
		}
	}
}

- (BOOL)isSessionPeer:(NSString *)peerId
{
	for (LoveBleSessionPeer *p in _sessionPeers)
	{
		if ([p.peerId isEqualToString:peerId])
			return YES;
	}
	return NO;
}

- (NSString *)sessionPeerStatus:(NSString *)peerId
{
	for (LoveBleSessionPeer *p in _sessionPeers)
	{
		if ([p.peerId isEqualToString:peerId])
			return p.status;
	}
	return nil;
}

- (int)connectedClientCount
{
	return (int)_connectedClients.count;
}

// ──────────────────────────────────────────────────
// Roster snapshot encoding (spec Section 4.3)
// ──────────────────────────────────────────────────

- (NSData *)encodeRosterSnapshotPayload
{
	// Format: session_id|host_peer_id|membership_epoch|peer1:status|peer2:status|...
	NSMutableString *payload = [NSMutableString string];
	[payload appendString:_sessionId ?: @""];
	[payload appendString:@"|"];
	[payload appendString:_localPeerId];
	[payload appendString:@"|"];
	[payload appendFormat:@"%d", _membershipEpoch];

	for (LoveBleSessionPeer *peer in _sessionPeers)
	{
		[payload appendString:@"|"];
		[payload appendString:peer.peerId];
		[payload appendString:@":"];
		[payload appendString:peer.status];
	}

	return [payload dataUsingEncoding:NSUTF8StringEncoding];
}

// ──────────────────────────────────────────────────
// Host: send control packet to a specific device
// ──────────────────────────────────────────────────

- (void)sendControlToDevice:(NSString *)deviceKey msgType:(NSString *)msgType
                   toPeerId:(NSString *)toPeerId payload:(NSData *)payload
{
	NSData *packetData = buildPacket(@"control", _localPeerId, toPeerId, msgType,
	                                  0, (const uint8_t *)payload.bytes, (uint32_t)payload.length);

	int mtu = [self mtuForDevice:deviceKey];
	int payloadLimit = mtu - kATTOverhead;
	NSArray<NSData *> *fragments = [self fragmentPacket:packetData payloadLimit:payloadLimit];
	if (!fragments) return;

	[self enqueueNotifications:fragments forDevice:deviceKey];
}

// Host: send control packet to a specific peer by peerId
- (void)sendControlToPeer:(NSString *)peerId msgType:(NSString *)msgType payload:(NSData *)payload
{
	// Find device key for this peerId
	NSString *deviceKey = nil;
	for (NSString *dk in _devicePeerMap)
	{
		if ([_devicePeerMap[dk] isEqualToString:peerId])
		{
			deviceKey = dk;
			break;
		}
	}
	if (!deviceKey) return;

	[self sendControlToDevice:deviceKey msgType:msgType toPeerId:peerId payload:payload];
}

// Host: broadcast control to all connected clients
- (void)broadcastControl:(NSString *)msgType payload:(NSData *)payload
{
	for (NSString *peerId in _connectedClients)
	{
		[self sendControlToPeer:peerId msgType:msgType payload:payload];
	}
}

// Host: broadcast control to all connected clients except one
- (void)broadcastControlExcept:(NSString *)excludePeerId msgType:(NSString *)msgType payload:(NSData *)payload
{
	for (NSString *peerId in _connectedClients)
	{
		if ([peerId isEqualToString:excludePeerId])
			continue;
		[self sendControlToPeer:peerId msgType:msgType payload:payload];
	}
}

// ──────────────────────────────────────────────────
// Host: notification queue (spec Section 15.2)
// ──────────────────────────────────────────────────

- (void)enqueueNotifications:(NSArray<NSData *> *)fragments forDevice:(NSString *)deviceKey
{
	NSMutableArray<NSData *> *queue = _notificationQueues[deviceKey];
	if (!queue)
	{
		queue = [NSMutableArray array];
		_notificationQueues[deviceKey] = queue;
	}

	[queue addObjectsFromArray:fragments];
	[self pumpNotificationQueue:deviceKey];
}

- (void)pumpNotificationQueue:(NSString *)deviceKey
{
	NSMutableArray<NSData *> *queue = _notificationQueues[deviceKey];
	if (!queue || queue.count == 0)
		return;

	// Find the CBCentral for this device
	CBCentral *central = nil;
	for (CBCentral *c in _subscribedCentrals)
	{
		if ([c.identifier.UUIDString isEqualToString:deviceKey])
		{
			central = c;
			break;
		}
	}
	if (!central || !_messageCharacteristic)
		return;

	NSData *fragment = queue.firstObject;

	BOOL sent = [_peripheralManager updateValue:fragment
	                          forCharacteristic:_messageCharacteristic
	                       onSubscribedCentrals:@[central]];
	if (sent)
	{
		[queue removeObjectAtIndex:0];
		// Pump next if queue not empty
		if (queue.count > 0)
		{
			// Dispatch async to avoid deep recursion
			dispatch_async(dispatch_get_main_queue(), ^{
				[self pumpNotificationQueue:deviceKey];
			});
		}
	}
	// If not sent, peripheralManagerIsReadyToUpdateSubscribers: will resume
}

- (int)mtuForDevice:(NSString *)deviceKey
{
	NSNumber *mtu = _deviceMTUs[deviceKey];
	if (mtu)
		return mtu.intValue;
	return kDefaultMTU;
}

// ──────────────────────────────────────────────────
// Client: write queue (spec Section 15.1)
// ──────────────────────────────────────────────────

- (void)enqueueClientWrites:(NSArray<NSData *> *)fragments
{
	[_clientWriteQueue addObjectsFromArray:fragments];
	[self pumpClientWriteQueue];
}

- (void)pumpClientWriteQueue
{
	// Step 1: If a write is already in-flight, return
	if (_writeInFlight)
		return;

	// Step 2-3: Peek first fragment
	if (_clientWriteQueue.count == 0)
		return;

	NSData *fragment = _clientWriteQueue.firstObject;

	if (!_connectedPeripheral || !_remoteCharacteristic)
		return;

	// Step 4-5: Write fragment, set writeInFlight
	_writeInFlight = YES;
	[_connectedPeripheral writeValue:fragment
	                forCharacteristic:_remoteCharacteristic
	                            type:CBCharacteristicWriteWithResponse];
}

// ──────────────────────────────────────────────────
// Client: send a packet to the host
// ──────────────────────────────────────────────────

- (void)clientSendPacket:(NSData *)packetData
{
	int payloadLimit = _negotiatedMTU - kATTOverhead;
	NSArray<NSData *> *fragments = [self fragmentPacket:packetData payloadLimit:payloadLimit];
	if (!fragments) return;

	[self enqueueClientWrites:fragments];
}

- (void)clientSendControl:(NSString *)msgType toPeerId:(NSString *)toPeerId payload:(NSData *)payload
{
	NSData *packetData = buildPacket(@"control", _localPeerId, toPeerId, msgType,
	                                  0, (const uint8_t *)payload.bytes, (uint32_t)payload.length);
	[self clientSendPacket:packetData];
}

- (void)clientSendData:(NSString *)msgType toPeerId:(NSString *)toPeerId
               payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	uint16_t msgId = [self nextMessageId];
	NSData *packetData = buildPacket(@"data", _localPeerId, toPeerId, msgType,
	                                  msgId, payload, payloadLen);
	[self clientSendPacket:packetData];
}

// ──────────────────────────────────────────────────
// Host: send data packet to specific device or broadcast
// ──────────────────────────────────────────────────

- (void)hostSendData:(NSString *)msgType toPeerId:(NSString *)toPeerId
             payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	uint16_t msgId = [self nextMessageId];
	NSData *packetData = buildPacket(@"data", _localPeerId, toPeerId, msgType,
	                                  msgId, payload, payloadLen);

	if (toPeerId.length == 0)
	{
		// Store for heartbeat re-broadcast (spec Section 9)
		_lastBroadcastPacket = packetData;
		_lastBroadcastMessageId = msgId;

		// Broadcast to all connected clients
		for (NSString *peerId in [_connectedClients allKeys])
		{
			NSString *deviceKey = nil;
			for (NSString *dk in _devicePeerMap)
			{
				if ([_devicePeerMap[dk] isEqualToString:peerId])
				{
					deviceKey = dk;
					break;
				}
			}
			if (!deviceKey) continue;

			int mtu = [self mtuForDevice:deviceKey];
			int payloadLimit = mtu - kATTOverhead;
			NSArray<NSData *> *fragments = [self fragmentPacket:packetData payloadLimit:payloadLimit];
			if (fragments)
				[self enqueueNotifications:fragments forDevice:deviceKey];
		}
	}
	else
	{
		// Directed: find the device for this peer
		[self sendDataToPeer:toPeerId packetData:packetData];
	}
}

- (void)sendDataToPeer:(NSString *)peerId packetData:(NSData *)packetData
{
	NSString *deviceKey = nil;
	for (NSString *dk in _devicePeerMap)
	{
		if ([_devicePeerMap[dk] isEqualToString:peerId])
		{
			deviceKey = dk;
			break;
		}
	}
	if (!deviceKey) return;

	int mtu = [self mtuForDevice:deviceKey];
	int payloadLimit = mtu - kATTOverhead;
	NSArray<NSData *> *fragments = [self fragmentPacket:packetData payloadLimit:payloadLimit];
	if (fragments)
		[self enqueueNotifications:fragments forDevice:deviceKey];
}

// ──────────────────────────────────────────────────
// Radio State
// ──────────────────────────────────────────────────

- (void)centralManagerDidUpdateState:(CBCentralManager *)central
{
	love::ble::Ble::RadioState newState;

	switch (central.state)
	{
		case CBManagerStatePoweredOn:
			newState = love::ble::Ble::RADIO_ON;
			break;
		case CBManagerStatePoweredOff:
			newState = love::ble::Ble::RADIO_OFF;
			break;
		case CBManagerStateUnauthorized:
			newState = love::ble::Ble::RADIO_UNAUTHORIZED;
			break;
		case CBManagerStateUnsupported:
			newState = love::ble::Ble::RADIO_UNSUPPORTED;
			break;
		default:
			newState = love::ble::Ble::RADIO_OFF;
			break;
	}

	love::ble::Ble::RadioState oldState = _radioState;
	_radioState = newState;

	if (oldState != newState)
	{
		const char *stateStr = nullptr;
		love::ble::Ble::getConstant(newState, stateStr);

		love::ble::Ble::BleEvent event;
		event.type = "radio";
		if (stateStr)
			event.fields["state"] = love::Variant(stateStr, strlen(stateStr));
		_owner->pushEvent(event);
	}
}

// ──────────────────────────────────────────────────
// Hosting: GATT Server + Advertising (spec Section 6.1)
// ──────────────────────────────────────────────────

- (void)hostWithRoomName:(NSString *)roomName maxClients:(int)maxClients transport:(char)transport
{
	if (_radioState != love::ble::Ble::RADIO_ON)
	{
		pushDiagnosticEvent(_owner, @"host: BLE not available");
		return;
	}

	// Spec 6.1 step 2: Leave existing session
	[self leaveSession];

	// Spec 6.1 step 3-6
	_sessionId = generateShortID();
	_roomName = normalizeRoomName(roomName);
	_maxClients = MAX(1, MIN(maxClients, 7));
	_transportChar = transport;
	_peerCount = 0;
	_membershipEpoch = 0;
	_hosting = YES;
	_hostServiceReady = NO;

	// Initialize host maps
	[_connectedClients removeAllObjects];
	[_devicePeerMap removeAllObjects];
	[_pendingClients removeAllObjects];
	[_notificationQueues removeAllObjects];
	[_deviceMTUs removeAllObjects];
	[_subscribedCentrals removeAllObjects];
	[self cancelAllGraceTimers];

	// Initialize roster with self as host
	[self resetSessionPeers];
	[self addSessionPeer:_localPeerId isHost:YES status:@"connected"];

	// Spec 6.1 step 7-9: Open GATT Server with service
	CBUUID *serviceUUID = [CBUUID UUIDWithString:kServiceUUID];
	CBUUID *charUUID = [CBUUID UUIDWithString:kCharacteristicUUID];

	_messageCharacteristic = [[CBMutableCharacteristic alloc]
		initWithType:charUUID
		properties:CBCharacteristicPropertyRead | CBCharacteristicPropertyWrite | CBCharacteristicPropertyNotify
		value:nil
		permissions:CBAttributePermissionsReadable | CBAttributePermissionsWriteable];

	_gattService = [[CBMutableService alloc] initWithType:serviceUUID primary:YES];
	_gattService.characteristics = @[_messageCharacteristic];

	[_peripheralManager addService:_gattService];
}

- (void)peripheralManagerDidUpdateState:(CBPeripheralManager *)peripheral
{
	if (peripheral.state != CBManagerStatePoweredOn && _hosting)
	{
		pushDiagnosticEvent(_owner, @"peripheralManager powered off while hosting");
	}
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral didAddService:(CBService *)service error:(NSError *)error
{
	if (error)
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"addService error: %@", error.localizedDescription]);
		return;
	}

	_hostServiceReady = YES;

	// Spec 6.1 step 10a: Advertise
	[self advertiseRoom];

	// Spec 6.1 step 10b: Start heartbeat
	[self startHeartbeat];

	// Spec Section 8.5: If becoming host as migration successor, emit session_resumed
	if (_migrationInProgress && _becomingHost)
	{
		[self completeMigrationResume];
		return;
	}

	// Spec 6.1 step 10c: Emit hosted event (normal fresh hosting)
	const char *transportStr = (_transportChar == 's') ? "resilient" : "reliable";

	love::ble::Ble::BleEvent event;
	event.type = "hosted";
	event.fields["session_id"] = makeStringVariant(_sessionId);
	event.fields["peer_id"] = makeStringVariant(_localPeerId);
	event.fields["transport"] = love::Variant(transportStr, strlen(transportStr));

	_owner->pushEvent(event);
}

// Spec Section 3.3: Advertise room on iOS (set as Local Name)
- (void)advertiseRoom
{
	if (!_hosting || !_hostServiceReady)
		return;

	[_peripheralManager stopAdvertising];

	NSString *adPayload = encodeRoomAdvertisement(_sessionId, _localPeerId,
	                                               _transportChar, _maxClients,
	                                               _peerCount, _roomName);

	// Spec Section 3.3 step 3: iOS sets payload as Local Name only.
	// Service UUID is NOT included — it causes iOS to truncate the Local Name
	// below the 18-byte minimum, making the room invisible to Android scanners.
	[_peripheralManager startAdvertising:@{
		CBAdvertisementDataLocalNameKey: adPayload,
	}];
}

// ──────────────────────────────────────────────────
// Scanning (spec Section 6.2)
// ──────────────────────────────────────────────────

- (void)startScan
{
	if (_radioState != love::ble::Ble::RADIO_ON)
	{
		pushDiagnosticEvent(_owner, @"scan: BLE not available");
		return;
	}

	// Spec 6.2 step 2-3
	[self stopScan];
	[_discoveredRooms removeAllObjects];

	// Spec 6.2 step 4: Low Latency, no service filter
	_scanning = YES;
	[_centralManager scanForPeripheralsWithServices:nil
	                                        options:@{CBCentralManagerScanOptionAllowDuplicatesKey: @YES}];

	[self startRoomExpiryTimer];
}

- (void)stopScan
{
	if (_scanning)
	{
		_scanning = NO;
		[_centralManager stopScan];
		[self stopRoomExpiryTimer];
	}
}

// Spec 6.2 step 5: On each scan result
- (void)centralManager:(CBCentralManager *)central
 didDiscoverPeripheral:(CBPeripheral *)peripheral
     advertisementData:(NSDictionary<NSString *, id> *)advertisementData
                  RSSI:(NSNumber *)RSSI
{
	NSString *roomId = peripheral.identifier.UUIDString;

	LoveBleRoom *room = decodeRoomFromAdvertisement(advertisementData, roomId, RSSI.integerValue);
	if (!room)
		return;

	room.peripheral = peripheral;

	_discoveredRooms[roomId] = room;

	// Spec Section 7.1: If in reconnect scan, check for matching room
	if (_reconnectInProgress && _reconnectScanInProgress)
	{
		[self onScanResultDuringReconnect:room];
		return;
	}

	// Spec Section 8.4: If in migration scan (not becoming host), check for successor
	if (_migrationInProgress && !_becomingHost)
	{
		[self onScanResultDuringMigration:room];
		return;
	}

	// Spec 6.2 step 5: emit room_found if not in migration or reconnect
	if (!_migrationInProgress && !_reconnectInProgress)
	{
		pushRoomFoundEvent(_owner, room);
	}
}

// ── Room Expiry (spec Section 3.5) ──

- (void)startRoomExpiryTimer
{
	[self stopRoomExpiryTimer];
	_roomExpiryTimer = [NSTimer scheduledTimerWithTimeInterval:1.0
	                                                   target:self
	                                                 selector:@selector(checkRoomExpiry)
	                                                 userInfo:nil
	                                                  repeats:YES];
}

- (void)stopRoomExpiryTimer
{
	[_roomExpiryTimer invalidate];
	_roomExpiryTimer = nil;
}

- (void)checkRoomExpiry
{
	NSDate *now = [NSDate date];
	NSMutableArray *expiredIds = [NSMutableArray array];

	for (NSString *roomId in _discoveredRooms)
	{
		LoveBleRoom *room = _discoveredRooms[roomId];
		if ([now timeIntervalSinceDate:room.lastSeenAt] > kRoomExpirySeconds)
			[expiredIds addObject:roomId];
	}

	for (NSString *roomId in expiredIds)
	{
		[_discoveredRooms removeObjectForKey:roomId];
		pushRoomLostEvent(_owner, roomId);
	}
}

// ──────────────────────────────────────────────────
// Joining a Room (spec Section 6.3)
// ──────────────────────────────────────────────────

- (void)joinRoom:(NSString *)roomId
{
	if (_radioState != love::ble::Ble::RADIO_ON)
	{
		pushDiagnosticEvent(_owner, @"join: BLE not available");
		return;
	}

	// Spec 6.3 step 2: Look up room by roomId
	LoveBleRoom *room = _discoveredRooms[roomId];
	if (!room)
	{
		// Spec 6.3 step 3: emit error "room_gone"
		pushErrorEvent(_owner, @"room_gone", nil);
		return;
	}

	// Spec 6.3 step 4: ConnectToRoom(room, migrationJoin=false)
	[self connectToRoom:room migrationJoin:NO];
}

// Spec Section 6.3: ConnectToRoom
- (void)connectToRoom:(LoveBleRoom *)room migrationJoin:(BOOL)migrationJoin
{
	// Step 1: Duplicate join guard
	if (_connectedPeripheral && !_clientLeaving &&
	    [_joinedRoomId isEqualToString:room.roomId] &&
	    [_joinedSessionId isEqualToString:room.sessionId] &&
	    [_hostPeerId isEqualToString:room.hostPeerId])
	{
		return;
	}

	// Step 2: Stop scan, clean up prior connection
	[self stopScan];
	[self stopClientOnly];

	// Step 3: Store session info
	_joinedRoomId = room.roomId;
	_joinedSessionId = room.sessionId;
	_hostPeerId = room.hostPeerId;
	_transportChar = room.transport;

	// Step 4: Reset flags
	_clientLeaving = NO;
	_clientJoined = NO;

	// Step 5: Reset roster if not migration/reconnect
	if (!migrationJoin && !_reconnectInProgress)
	{
		[self resetSessionPeers];
	}

	// Step 6: Connect via GATT Client with autoConnect=false
	if (!room.peripheral)
	{
		pushErrorEvent(_owner, @"room_gone", @"peripheral reference lost");
		return;
	}

	_connectedPeripheral = room.peripheral;
	_connectedPeripheral.delegate = self;
	[_centralManager connectPeripheral:_connectedPeripheral options:nil];
}

// Stop client GATT connection without full leave
- (void)stopClientOnly
{
	if (_connectedPeripheral)
	{
		[_centralManager cancelPeripheralConnection:_connectedPeripheral];
		_connectedPeripheral.delegate = nil;
		_connectedPeripheral = nil;
	}
	_remoteCharacteristic = nil;
	_writeInFlight = NO;
	[_clientWriteQueue removeAllObjects];
	_negotiatedMTU = kDefaultMTU;
}

// ──────────────────────────────────────────────────
// CBPeripheralDelegate: Client-side GATT discovery
// ──────────────────────────────────────────────────

// Spec 6.3 step 7: On GATT connected
- (void)centralManager:(CBCentralManager *)central didConnectPeripheral:(CBPeripheral *)peripheral
{
	if (peripheral != _connectedPeripheral)
		return;

	pushDiagnosticEvent(_owner, @"client: GATT connected");

	// Step 7a: Request MTU — on iOS, MTU is negotiated automatically.
	// We read it from the peripheral's maximumWriteValueLength.
	NSUInteger maxWrite = [peripheral maximumWriteValueLengthForType:CBCharacteristicWriteWithResponse];
	// maxWrite is the max value we can write. The MTU = maxWrite + ATT overhead.
	_negotiatedMTU = (int)maxWrite + kATTOverhead;
	if (_negotiatedMTU < kDefaultMTU)
		_negotiatedMTU = kDefaultMTU;

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: negotiated MTU %d", _negotiatedMTU]);

	// Step 7b: Discover services
	CBUUID *serviceUUID = [CBUUID UUIDWithString:kServiceUUID];
	[peripheral discoverServices:@[serviceUUID]];
}

- (void)centralManager:(CBCentralManager *)central didFailToConnectPeripheral:(CBPeripheral *)peripheral error:(NSError *)error
{
	if (peripheral != _connectedPeripheral)
		return;

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: connect failed: %@",
		error ? error.localizedDescription : @"unknown"]);

	[self stopClientOnly];

	// Spec Section 13 step 5: emit error
	NSString *detail = error ? error.localizedDescription : @"connection_failed";
	pushErrorEvent(_owner, @"join_failed", detail);
}

// Spec Section 13: Client Disconnect Decision Tree
- (void)centralManager:(CBCentralManager *)central didDisconnectPeripheral:(CBPeripheral *)peripheral error:(NSError *)error
{
	if (peripheral != _connectedPeripheral)
		return;

	BOOL wasJoined = _clientJoined;
	BOOL shouldEmit = !_clientLeaving;

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: disconnected (wasJoined=%d, shouldEmit=%d, err=%@)",
		wasJoined, shouldEmit, error ? error.localizedDescription : @"none"]);

	// Step 1: Clean up GATT state
	[self stopClientOnly];

	// Step 2: If active migration exists, begin migration reconnect (spec Section 13 step 2)
	if (_migrationInProgress)
	{
		pushDiagnosticEvent(_owner, @"client: disconnect during migration, beginning migration reconnect");
		[self beginMigrationReconnect];
		return;
	}

	// Step 3: If shouldEmit AND wasJoined AND transport is Resilient, attempt unexpected host recovery (spec Section 13 step 3)
	if (shouldEmit && wasJoined && _transportChar == 's')
	{
		if ([self beginUnexpectedHostRecovery])
			return;
	}

	// Step 4: If shouldEmit AND wasJoined, attempt client reconnect (spec Section 13 step 4)
	if (shouldEmit && wasJoined)
	{
		if ([self beginClientReconnect])
			return;
	}

	// Step 5: Emit events (spec Section 13 step 5)
	if (shouldEmit)
	{
		[self finishLeave:nil];
		if (wasJoined)
		{
			love::ble::Ble::BleEvent event;
			event.type = "session_ended";
			event.fields["reason"] = love::Variant("host_lost", 9);
			_owner->pushEvent(event);
		}
		else
		{
			// Was still in join process
			NSString *detail = error ? error.localizedDescription : @"connection_lost";
			pushErrorEvent(_owner, @"join_failed", detail);
		}
	}
	// Step 6: Silent cleanup (implicit — stopClientOnly already called)
}

- (void)peripheral:(CBPeripheral *)peripheral didDiscoverServices:(NSError *)error
{
	if (error)
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: service discovery error: %@", error.localizedDescription]);
		[self stopClientOnly];
		pushErrorEvent(_owner, @"join_failed", @"service_discovery_failed");
		return;
	}

	CBUUID *charUUID = [CBUUID UUIDWithString:kCharacteristicUUID];

	for (CBService *service in peripheral.services)
	{
		// Step 7c: Find Message Characteristic
		[peripheral discoverCharacteristics:@[charUUID] forService:service];
	}
}

- (void)peripheral:(CBPeripheral *)peripheral didDiscoverCharacteristicsForService:(CBService *)service error:(NSError *)error
{
	if (error)
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: characteristic discovery error: %@", error.localizedDescription]);
		[self stopClientOnly];
		pushErrorEvent(_owner, @"join_failed", @"characteristic_discovery_failed");
		return;
	}

	CBUUID *charUUID = [CBUUID UUIDWithString:kCharacteristicUUID];

	for (CBCharacteristic *characteristic in service.characteristics)
	{
		if ([characteristic.UUID isEqual:charUUID])
		{
			_remoteCharacteristic = characteristic;

			// Step 7d: Enable notifications via CCCD descriptor write
			[peripheral setNotifyValue:YES forCharacteristic:characteristic];

			pushDiagnosticEvent(_owner, @"client: found characteristic, enabling notifications");
			return;
		}
	}

	pushDiagnosticEvent(_owner, @"client: characteristic not found");
	[self stopClientOnly];
	pushErrorEvent(_owner, @"join_failed", @"characteristic_not_found");
}

- (void)peripheral:(CBPeripheral *)peripheral didUpdateNotificationStateForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	if (error)
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: notification enable error: %@", error.localizedDescription]);
		[self stopClientOnly];
		pushErrorEvent(_owner, @"join_failed", @"notification_enable_failed");
		return;
	}

	pushDiagnosticEvent(_owner, @"client: notifications enabled, completing join");

	// Step 7e: CompleteLocalJoin
	[self completeLocalJoin];
}

// ──────────────────────────────────────────────────
// Client Join Completion (spec Section 6.4)
// ──────────────────────────────────────────────────

- (void)completeLocalJoin
{
	// Step 1: Add local and host to roster
	[self addSessionPeer:_localPeerId isHost:NO status:@"connected"];
	[self addSessionPeer:_hostPeerId isHost:YES status:@"connected"];

	// Step 2: Enter pending state — do NOT emit joined yet

	// Step 3: Determine joinIntent
	NSString *joinIntent;
	if (_reconnectInProgress)
		joinIntent = @"reconnect";
	else if (_migrationInProgress)
		joinIntent = @"migration_resume";
	else
		joinIntent = @"fresh";

	// Step 4: Determine sessionId for hello payload
	NSString *helloSessionId;
	if ([joinIntent isEqualToString:@"fresh"])
		helloSessionId = @""; // Fresh join, no prior session
	else
		helloSessionId = _joinedSessionId ?: @"";

	// Step 5: Encode and send HELLO control packet
	NSString *helloPayload = [NSString stringWithFormat:@"%@|%@", helloSessionId, joinIntent];
	NSData *payloadData = [helloPayload dataUsingEncoding:NSUTF8StringEncoding];

	[self clientSendControl:@"hello" toPeerId:_hostPeerId payload:payloadData];

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: sent hello (intent=%@)", joinIntent]);

	// Step 6: Await hello_ack or join_rejected (handled in notification callback)
}

// ──────────────────────────────────────────────────
// Client: handle incoming notification from host
// ──────────────────────────────────────────────────

- (void)peripheral:(CBPeripheral *)peripheral didUpdateValueForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	if (error)
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: notification read error: %@", error.localizedDescription]);
		return;
	}

	NSData *fragmentData = characteristic.value;
	if (!fragmentData || fragmentData.length == 0)
		return;

	// Reassemble fragments. Source key for host is "host"
	NSData *reassembled = [self processIncomingFragment:@"host" fragmentData:fragmentData];
	if (!reassembled)
		return;

	// Decode the reassembled packet
	LoveBlePacket *packet = decodePacket(reassembled);
	if (!packet)
		return;

	[self handleClientReceivedPacket:packet];
}

- (void)handleClientReceivedPacket:(LoveBlePacket *)packet
{
	if ([packet.kind isEqualToString:@"control"])
	{
		[self handleClientControlPacket:packet];
	}
	else if ([packet.kind isEqualToString:@"data"])
	{
		[self handleClientDataPacket:packet];
	}
}

- (void)handleClientControlPacket:(LoveBlePacket *)packet
{
	NSString *msgType = packet.msgType;

	if ([msgType isEqualToString:@"hello_ack"])
	{
		// Spec Section 6.4: OnHelloAckReceived
		[self onHelloAckReceived];
	}
	else if ([msgType isEqualToString:@"join_rejected"])
	{
		// Spec Section 6.4: OnJoinRejectedReceived
		NSString *reason = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding] ?: @"unknown";
		[self onJoinRejectedReceived:reason];
	}
	else if ([msgType isEqualToString:@"peer_joined"])
	{
		// A new peer joined the session
		NSString *peerId = packet.fromPeerId;
		if (peerId.length > 0 && ![peerId isEqualToString:_localPeerId])
		{
			[self addSessionPeer:peerId isHost:NO status:@"connected"];

			love::ble::Ble::BleEvent event;
			event.type = "peer_joined";
			event.fields["peer_id"] = makeStringVariant(peerId);
			_owner->pushEvent(event);
		}
	}
	else if ([msgType isEqualToString:@"peer_left"])
	{
		NSString *peerId = packet.fromPeerId;
		NSString *reason = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding] ?: @"";

		if (peerId.length > 0)
		{
			[self removeSessionPeer:peerId];

			love::ble::Ble::BleEvent event;
			event.type = "peer_left";
			event.fields["peer_id"] = makeStringVariant(peerId);
			if (reason.length > 0)
				event.fields["reason"] = makeStringVariant(reason);
			_owner->pushEvent(event);
		}
	}
	else if ([msgType isEqualToString:@"roster_snapshot"])
	{
		[self handleRosterSnapshot:packet];
	}
	else if ([msgType isEqualToString:@"session_ended"])
	{
		NSString *reason = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding] ?: @"host_left";
		_clientLeaving = YES;
		[self stopClientOnly];
		[self finishLeave:nil];

		love::ble::Ble::BleEvent event;
		event.type = "session_ended";
		event.fields["reason"] = makeStringVariant(reason);
		_owner->pushEvent(event);
	}
	else if ([msgType isEqualToString:@"heartbeat"])
	{
		[self handleHeartbeatControl:packet.payload];
	}
	else if ([msgType isEqualToString:@"session_migrating"])
	{
		// Spec Section 8.4: OnSessionMigratingReceived
		[self onSessionMigratingReceived:packet];
	}
}

- (void)handleClientDataPacket:(LoveBlePacket *)packet
{
	// Spec Section 10: Dedup for data packets
	if ([self isDuplicate:packet.fromPeerId msgType:packet.msgType messageId:packet.messageId])
		return;

	// Deliver message event
	love::ble::Ble::BleEvent event;
	event.type = "message";
	event.fields["peer_id"] = makeStringVariant(packet.fromPeerId);
	event.fields["msg_type"] = makeStringVariant(packet.msgType);

	// Store payload as raw bytes in a Variant string for Codec decoding by wrap_Ble
	const char *payloadBytes = (const char *)packet.payload.bytes;
	size_t payloadLen = packet.payload.length;
	event.fields["payload"] = love::Variant(payloadBytes, payloadLen);

	_owner->pushEvent(event);
}

// Spec Section 6.4: OnHelloAckReceived
- (void)onHelloAckReceived
{
	pushDiagnosticEvent(_owner, @"client: received hello_ack");

	// Step 1
	_clientJoined = YES;

	// Step 2: Reconnect resume (spec Section 7.1)
	if (_reconnectInProgress)
	{
		[self completeReconnectResume];
		return; // Do not emit "joined" — peer was already in session
	}

	// Step 3: Migration resume (spec Section 8.5)
	if (_migrationInProgress)
	{
		[self completeMigrationResume];
		return; // Emit session_resumed instead of "joined"
	}

	// Step 4: Emit joined event (fresh join)
	const char *transportStr = (_transportChar == 's') ? "resilient" : "reliable";

	love::ble::Ble::BleEvent event;
	event.type = "joined";
	event.fields["session_id"] = makeStringVariant(_joinedSessionId);
	event.fields["room_id"] = makeStringVariant(_joinedRoomId);
	event.fields["peer_id"] = makeStringVariant(_localPeerId);
	event.fields["host_id"] = makeStringVariant(_hostPeerId);
	event.fields["transport"] = love::Variant(transportStr, strlen(transportStr));

	_owner->pushEvent(event);
}

// Spec Section 6.4: OnJoinRejectedReceived
- (void)onJoinRejectedReceived:(NSString *)reason
{
	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: join rejected: %@", reason]);

	// Step 1: Disconnect
	_clientLeaving = YES;
	[self stopClientOnly];

	// Step 2: Emit join_failed
	love::ble::Ble::BleEvent event;
	event.type = "join_failed";
	event.fields["reason"] = makeStringVariant(reason);
	event.fields["room_id"] = makeStringVariant(_joinedRoomId ?: @"");

	_owner->pushEvent(event);
}

// Handle roster_snapshot control (spec Section 4.3)
- (void)handleRosterSnapshot:(LoveBlePacket *)packet
{
	NSString *payloadStr = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding];
	if (!payloadStr) return;

	NSArray<NSString *> *parts = [payloadStr componentsSeparatedByString:@"|"];
	if (parts.count < 3) return;

	NSString *snapSessionId = parts[0];
	NSString *snapHostPeerId = parts[1];
	int snapEpoch = [parts[2] intValue];

	// Update local state
	_joinedSessionId = snapSessionId;
	_hostPeerId = snapHostPeerId;

	// Rebuild roster
	[self resetSessionPeers];
	for (NSUInteger i = 3; i < parts.count; i++)
	{
		NSArray<NSString *> *peerParts = [parts[i] componentsSeparatedByString:@":"];
		if (peerParts.count == 2)
		{
			NSString *peerId = peerParts[0];
			NSString *status = peerParts[1];
			BOOL isHost = [peerId isEqualToString:snapHostPeerId];
			[self addSessionPeer:peerId isHost:isHost status:status];
		}
	}

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"client: roster snapshot epoch=%d peers=%lu",
		snapEpoch, (unsigned long)_sessionPeers.count]);
}

// ──────────────────────────────────────────────────
// Client write callback (spec Section 15.1)
// ──────────────────────────────────────────────────

- (void)peripheral:(CBPeripheral *)peripheral didWriteValueForCharacteristic:(CBCharacteristic *)characteristic error:(NSError *)error
{
	// Step 6a: Remove written fragment
	if (_clientWriteQueue.count > 0)
		[_clientWriteQueue removeObjectAtIndex:0];

	// Step 6b: Clear in-flight
	_writeInFlight = NO;

	if (error)
	{
		// Step 6c: Write failed — clear queue and emit error
		[_clientWriteQueue removeAllObjects];
		pushErrorEvent(_owner, @"write_failed", error.localizedDescription);
		return;
	}

	// Step 6d: Pump next
	[self pumpClientWriteQueue];
}

// ──────────────────────────────────────────────────
// Host-side: write requests from clients (spec Section 6.5)
// ──────────────────────────────────────────────────

- (void)peripheralManager:(CBPeripheralManager *)peripheral didReceiveWriteRequests:(NSArray<CBATTRequest *> *)requests
{
	// Must respond to first request (CoreBluetooth requirement)
	[peripheral respondToRequest:requests.firstObject withResult:CBATTErrorSuccess];

	for (CBATTRequest *request in requests)
	{
		NSData *fragmentData = request.value;
		if (!fragmentData || fragmentData.length == 0)
			continue;

		NSString *deviceKey = request.central.identifier.UUIDString;

		// Track MTU from central
		int centralMTU = (int)request.central.maximumUpdateValueLength + kATTOverhead;
		if (centralMTU >= kDefaultMTU)
			_deviceMTUs[deviceKey] = @(centralMTU);

		// Track as pending client if not yet known
		if (!_devicePeerMap[deviceKey] && !_pendingClients[deviceKey])
			_pendingClients[deviceKey] = [NSDate date];

		// Reassemble fragment
		NSData *reassembled = [self processIncomingFragment:deviceKey fragmentData:fragmentData];
		if (!reassembled)
			continue;

		// Decode packet
		LoveBlePacket *packet = decodePacket(reassembled);
		if (!packet)
			continue;

		[self handleHostReceivedPacket:packet fromDevice:deviceKey central:request.central];
	}
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral didReceiveReadRequest:(CBATTRequest *)request
{
	request.value = [NSData data];
	[peripheral respondToRequest:request withResult:CBATTErrorSuccess];
}

// ──────────────────────────────────────────────────
// Host: handle received packet
// ──────────────────────────────────────────────────

- (void)handleHostReceivedPacket:(LoveBlePacket *)packet fromDevice:(NSString *)deviceKey central:(CBCentral *)central
{
	if ([packet.kind isEqualToString:@"control"])
	{
		if ([packet.msgType isEqualToString:@"hello"])
		{
			[self onHelloReceived:deviceKey packet:packet central:central];
		}
		else if ([packet.msgType isEqualToString:@"roster_request"])
		{
			// Respond with roster_snapshot
			NSString *peerId = _devicePeerMap[deviceKey];
			if (peerId)
			{
				NSData *rosterPayload = [self encodeRosterSnapshotPayload];
				[self sendControlToDevice:deviceKey msgType:@"roster_snapshot" toPeerId:peerId payload:rosterPayload];
			}
		}
	}
	else if ([packet.kind isEqualToString:@"data"])
	{
		[self handleHostReceivedData:packet fromDevice:deviceKey];
	}
}

// ──────────────────────────────────────────────────
// Host: HELLO handshake (spec Section 6.5)
// ──────────────────────────────────────────────────

- (void)onHelloReceived:(NSString *)deviceKey packet:(LoveBlePacket *)packet central:(CBCentral *)central
{
	// Step 1: Get peerId from packet
	NSString *peerId = packet.fromPeerId;
	if (peerId.length == 0)
	{
		pushDiagnosticEvent(_owner, @"host: hello with empty peerId, disconnecting");
		// Cannot disconnect a CBCentral directly on iOS; just ignore
		return;
	}

	// Step 2: Parse payload — "session_id|join_intent"
	NSString *payloadStr = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding];
	if (!payloadStr)
	{
		pushDiagnosticEvent(_owner, @"host: hello with invalid payload");
		return;
	}

	NSArray<NSString *> *parts = [payloadStr componentsSeparatedByString:@"|"];
	NSString *helloSessionId = (parts.count > 0) ? parts[0] : @"";
	NSString *joinIntent = (parts.count > 1) ? parts[1] : @"fresh";

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: hello from %@ intent=%@ session=%@",
		peerId, joinIntent, helloSessionId]);

	// Step 3: Validate admission

	// Check if peer is in reconnect grace
	BOOL inGrace = (_graceTimers[peerId] != nil);

	// 3a: room_full — connected clients >= maxClients AND not in grace
	if ([self connectedClientCount] >= _maxClients && !inGrace)
	{
		[self sendJoinRejected:deviceKey peerId:peerId reason:@"room_full"];
		return;
	}

	// 3b: duplicate_peer_id — already in connected clients
	if (_connectedClients[peerId] != nil)
	{
		[self sendJoinRejected:deviceKey peerId:peerId reason:@"duplicate_peer_id"];
		return;
	}

	// 3c: stale_session — sessionId non-empty and doesn't match
	if (helloSessionId.length > 0 && ![helloSessionId isEqualToString:_sessionId])
	{
		[self sendJoinRejected:deviceKey peerId:peerId reason:@"stale_session"];
		return;
	}

	// 3d: wrong_target — toPeerId doesn't match local host
	if (packet.toPeerId.length > 0 && ![packet.toPeerId isEqualToString:_localPeerId])
	{
		[self sendJoinRejected:deviceKey peerId:peerId reason:@"wrong_target"];
		return;
	}

	// 3e: migration_mismatch
	if ([joinIntent isEqualToString:@"migration_resume"] && !_migrationInProgress)
	{
		[self sendJoinRejected:deviceKey peerId:peerId reason:@"migration_mismatch"];
		return;
	}

	// Step 4: Remove from pending clients
	[_pendingClients removeObjectForKey:deviceKey];

	// Step 5: Bind device -> peer
	_devicePeerMap[deviceKey] = peerId;

	// Step 6: Bind peer -> central
	_connectedClients[peerId] = central;

	// Step 7: Send hello_ack
	[self sendControlToDevice:deviceKey msgType:@"hello_ack" toPeerId:peerId payload:[NSData data]];

	// Step 8/9: Handle reconnect grace vs new peer
	if (inGrace)
	{
		// Step 8: Reconnecting peer
		// 8a: Cancel grace timer
		NSTimer *timer = _graceTimers[peerId];
		[timer invalidate];
		[_graceTimers removeObjectForKey:peerId];

		// 8b: Update status to connected, increment epoch
		[self updateSessionPeerStatus:peerId status:@"connected"];
		_membershipEpoch++;

		// 8c: Emit peer_status
		love::ble::Ble::BleEvent event;
		event.type = "peer_status";
		event.fields["peer_id"] = makeStringVariant(peerId);
		event.fields["status"] = love::Variant("connected", 9);
		_owner->pushEvent(event);

		// 8d: Broadcast roster_snapshot
		NSData *rosterPayload = [self encodeRosterSnapshotPayload];
		[self broadcastControl:@"roster_snapshot" payload:rosterPayload];
	}
	else
	{
		// Step 9: New peer
		// 9a: Add to roster
		[self addSessionPeer:peerId isHost:NO status:@"connected"];
		_membershipEpoch++;
		_peerCount = [self connectedClientCount];

		// 9b: Emit peer_joined locally
		love::ble::Ble::BleEvent event;
		event.type = "peer_joined";
		event.fields["peer_id"] = makeStringVariant(peerId);
		_owner->pushEvent(event);

		// 9c: Broadcast peer_joined control to all other clients
		// The peer_joined control uses fromPeerId = the joining peer's id
		NSData *emptyPayload = [NSData data];
		NSData *peerJoinedPacket = buildPacket(@"control", peerId, @"", @"peer_joined",
		                                        0, (const uint8_t *)emptyPayload.bytes, 0);
		for (NSString *otherPeerId in [_connectedClients allKeys])
		{
			if ([otherPeerId isEqualToString:peerId])
				continue;
			[self sendDataToPeer:otherPeerId packetData:peerJoinedPacket];
		}

		// 9d: Broadcast roster_snapshot to all connected clients
		NSData *rosterPayload = [self encodeRosterSnapshotPayload];
		[self broadcastControl:@"roster_snapshot" payload:rosterPayload];
	}

	// Step 10: Update advertisement (peer count changed)
	_peerCount = [self connectedClientCount];
	[self advertiseRoom];

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: admitted %@ (clients=%d)", peerId, _peerCount]);
}

- (void)sendJoinRejected:(NSString *)deviceKey peerId:(NSString *)peerId reason:(NSString *)reason
{
	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: rejecting %@ reason=%@", peerId, reason]);

	NSData *reasonData = [reason dataUsingEncoding:NSUTF8StringEncoding];
	[self sendControlToDevice:deviceKey msgType:@"join_rejected" toPeerId:peerId payload:reasonData];

	// Note: Cannot forcefully disconnect a CBCentral on iOS.
	// The client will disconnect upon receiving join_rejected.
}

// ──────────────────────────────────────────────────
// Host: message routing (spec Section 4.4)
// ──────────────────────────────────────────────────

- (void)handleHostReceivedData:(LoveBlePacket *)packet fromDevice:(NSString *)deviceKey
{
	NSString *senderPeerId = _devicePeerMap[deviceKey];
	if (!senderPeerId)
		return;

	// Dedup for data packets (spec Section 10)
	if ([self isDuplicate:packet.fromPeerId msgType:packet.msgType messageId:packet.messageId])
		return;

	NSString *toPeerId = packet.toPeerId;

	if (toPeerId.length == 0)
	{
		// Broadcast: relay to all connected clients except sender, deliver to host
		NSData *packetData = buildPacket(packet.kind, packet.fromPeerId, packet.toPeerId,
		                                  packet.msgType, packet.messageId,
		                                  (const uint8_t *)packet.payload.bytes,
		                                  (uint32_t)packet.payload.length);

		for (NSString *peerId in [_connectedClients allKeys])
		{
			if ([peerId isEqualToString:senderPeerId])
				continue;
			[self sendDataToPeer:peerId packetData:packetData];
		}

		// Deliver to host (self)
		[self deliverDataToHost:packet];
	}
	else if ([toPeerId isEqualToString:_localPeerId])
	{
		// Directed to host: deliver to host, don't relay
		[self deliverDataToHost:packet];
	}
	else if (_connectedClients[toPeerId] != nil)
	{
		// Directed to a connected client: forward only to that client
		NSData *packetData = buildPacket(packet.kind, packet.fromPeerId, packet.toPeerId,
		                                  packet.msgType, packet.messageId,
		                                  (const uint8_t *)packet.payload.bytes,
		                                  (uint32_t)packet.payload.length);
		[self sendDataToPeer:toPeerId packetData:packetData];
	}
	else
	{
		// Unknown or reconnecting peer: drop silently (spec Section 4.4)
	}
}

// Deliver a data packet to the local host app
- (void)deliverDataToHost:(LoveBlePacket *)packet
{
	love::ble::Ble::BleEvent event;
	event.type = "message";
	event.fields["peer_id"] = makeStringVariant(packet.fromPeerId);
	event.fields["msg_type"] = makeStringVariant(packet.msgType);

	const char *payloadBytes = (const char *)packet.payload.bytes;
	size_t payloadLen = packet.payload.length;
	event.fields["payload"] = love::Variant(payloadBytes, payloadLen);

	_owner->pushEvent(event);
}

// ──────────────────────────────────────────────────
// Host: subscription management
// ──────────────────────────────────────────────────

- (void)peripheralManager:(CBPeripheralManager *)peripheral central:(CBCentral *)central didSubscribeToCharacteristic:(CBCharacteristic *)characteristic
{
	[_subscribedCentrals addObject:central];

	NSString *deviceKey = central.identifier.UUIDString;
	int centralMTU = (int)central.maximumUpdateValueLength + kATTOverhead;
	if (centralMTU >= kDefaultMTU)
		_deviceMTUs[deviceKey] = @(centralMTU);

	// Track as pending client
	if (!_devicePeerMap[deviceKey])
		_pendingClients[deviceKey] = [NSDate date];

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: central subscribed %@ (MTU=%d)",
		[deviceKey substringToIndex:MIN(8, (int)deviceKey.length)], centralMTU]);
}

- (void)peripheralManager:(CBPeripheralManager *)peripheral central:(CBCentral *)central didUnsubscribeFromCharacteristic:(CBCharacteristic *)characteristic
{
	[_subscribedCentrals removeObject:central];

	NSString *deviceKey = central.identifier.UUIDString;

	// Spec Section 14: Host Client-Disconnect Decision Tree
	[self onHostClientDisconnected:deviceKey];
}

// Spec Section 15.2: Resume notification pumping
- (void)peripheralManagerIsReadyToUpdateSubscribers:(CBPeripheralManager *)peripheral
{
	// Resume all notification queues
	for (NSString *deviceKey in [_notificationQueues allKeys])
	{
		[self pumpNotificationQueue:deviceKey];
	}
}

// ──────────────────────────────────────────────────
// Host: client disconnect handling (spec Section 14)
// ──────────────────────────────────────────────────

- (void)onHostClientDisconnected:(NSString *)deviceKey
{
	// Step 1: Remove from pending, MTU map, notification queues
	[_pendingClients removeObjectForKey:deviceKey];
	[_deviceMTUs removeObjectForKey:deviceKey];
	[_notificationQueues removeObjectForKey:deviceKey];

	// Step 2: Look up peerId, remove mapping
	NSString *peerId = _devicePeerMap[deviceKey];
	[_devicePeerMap removeObjectForKey:deviceKey];

	// Step 3: If peerId found
	if (peerId)
	{
		// 3a: Remove from connected clients
		[_connectedClients removeObjectForKey:peerId];

		// 3b: If hosting and not in migration departure, begin grace
		if (_hosting && !_migrationInProgress)
		{
			[self beginPeerReconnectGrace:peerId];
		}
		else
		{
			// 3c: Remove from roster
			[self removeSessionPeer:peerId];
			_peerCount = [self connectedClientCount];
			[self advertiseRoom];
		}
	}
}

// ──────────────────────────────────────────────────
// Host: reconnect grace (spec Section 7.2)
// ──────────────────────────────────────────────────

- (void)beginPeerReconnectGrace:(NSString *)peerId
{
	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: begin grace for %@", peerId]);

	// Step 1: Already removed from connected clients above

	// Step 2-3: Update status to reconnecting, increment epoch
	[self updateSessionPeerStatus:peerId status:@"reconnecting"];
	_membershipEpoch++;

	// Step 4: Do NOT notify of departure

	// Step 5: Emit peer_status
	love::ble::Ble::BleEvent event;
	event.type = "peer_status";
	event.fields["peer_id"] = makeStringVariant(peerId);
	event.fields["status"] = love::Variant("reconnecting", 12);
	_owner->pushEvent(event);

	// Step 6: Broadcast roster_snapshot
	NSData *rosterPayload = [self encodeRosterSnapshotPayload];
	[self broadcastControl:@"roster_snapshot" payload:rosterPayload];

	// Step 7: Schedule grace timeout
	NSTimer *timer = [NSTimer scheduledTimerWithTimeInterval:kReconnectTimeoutSeconds
	                                                 target:self
	                                               selector:@selector(onGraceTimeout:)
	                                               userInfo:peerId
	                                                repeats:NO];
	_graceTimers[peerId] = timer;

	// Step 8: Update advertisement
	_peerCount = [self connectedClientCount];
	[self advertiseRoom];
}

// Spec Section 7.2: OnGraceTimeout
- (void)onGraceTimeout:(NSTimer *)timer
{
	NSString *peerId = timer.userInfo;
	[_graceTimers removeObjectForKey:peerId];

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"host: grace timeout for %@", peerId]);

	// Step 1: Remove from roster, increment epoch
	[self removeSessionPeer:peerId];
	_membershipEpoch++;

	// Step 2: Emit peer_left
	love::ble::Ble::BleEvent event;
	event.type = "peer_left";
	event.fields["peer_id"] = makeStringVariant(peerId);
	event.fields["reason"] = love::Variant("timeout", 7);
	_owner->pushEvent(event);

	// Step 3: Send peer_left control to all remaining clients
	NSData *reasonData = [@"timeout" dataUsingEncoding:NSUTF8StringEncoding];
	// Build peer_left with the departed peer's id in fromPeerId
	NSData *peerLeftPacket = buildPacket(@"control", peerId, @"", @"peer_left",
	                                      0, (const uint8_t *)reasonData.bytes, (uint32_t)reasonData.length);
	for (NSString *otherPeerId in [_connectedClients allKeys])
	{
		[self sendDataToPeer:otherPeerId packetData:peerLeftPacket];
	}

	// Step 4: Broadcast roster_snapshot
	NSData *rosterPayload = [self encodeRosterSnapshotPayload];
	[self broadcastControl:@"roster_snapshot" payload:rosterPayload];

	// Step 5: Update advertisement
	_peerCount = [self connectedClientCount];
	[self advertiseRoom];
}

- (void)cancelAllGraceTimers
{
	for (NSString *peerId in _graceTimers)
	{
		[_graceTimers[peerId] invalidate];
	}
	[_graceTimers removeAllObjects];
}

// ──────────────────────────────────────────────────
// Heartbeat (spec Section 9)
// ──────────────────────────────────────────────────

// Use zlib's CRC32 — available on all Apple platforms
#include <zlib.h>

static uint32_t computeCRC32(const uint8_t *data, size_t length)
{
	return (uint32_t)crc32(0L, data, (uInt)length);
}

// Spec Section 9: Compute roster fingerprint — CRC32 of sorted, concatenated peerID:status pairs.
// Status is 'c' (connected) or 'r' (reconnecting). Pairs are pipe-delimited.
- (uint32_t)computeRosterFingerprint
{
	NSMutableArray<NSString *> *entries = [NSMutableArray array];
	for (LoveBleSessionPeer *peer in _sessionPeers)
	{
		NSString *s = [peer.status isEqualToString:@"connected"] ? @"c" : @"r";
		[entries addObject:[NSString stringWithFormat:@"%@:%@", peer.peerId, s]];
	}
	[entries sortUsingSelector:@selector(compare:)];
	NSString *joined = [entries componentsJoinedByString:@"|"];
	NSData *data = [joined dataUsingEncoding:NSUTF8StringEncoding];
	return computeCRC32((const uint8_t *)data.bytes, data.length);
}

// Spec Section 9: Start heartbeat timer
- (void)startHeartbeat
{
	[self stopHeartbeat];
	_heartbeatTimer = [NSTimer scheduledTimerWithTimeInterval:kHeartbeatInterval
	                                                  target:self
	                                                selector:@selector(heartbeatTick)
	                                                userInfo:nil
	                                                 repeats:YES];
}

- (void)stopHeartbeat
{
	[_heartbeatTimer invalidate];
	_heartbeatTimer = nil;
}

// Spec Section 9 step 3: Heartbeat tick
- (void)heartbeatTick
{
	if (!_hosting)
		return;

	// Step 3b: Disconnect pending clients older than Pending Client Timeout (5s)
	NSDate *now = [NSDate date];
	NSMutableArray *expiredPending = [NSMutableArray array];
	for (NSString *deviceKey in _pendingClients)
	{
		if ([now timeIntervalSinceDate:_pendingClients[deviceKey]] > kPendingClientTimeout)
			[expiredPending addObject:deviceKey];
	}
	for (NSString *deviceKey in expiredPending)
	{
		[_pendingClients removeObjectForKey:deviceKey];
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"heartbeat: disconnecting stale pending client %@", deviceKey]);
		// Cannot force-disconnect a CBCentral from peripheral manager side on iOS
	}

	// Step 3c-d: Compute roster fingerprint and deliver to all connected clients
	uint32_t fingerprint = [self computeRosterFingerprint];
	uint8_t fpBytes[4] = {
		(uint8_t)((fingerprint >> 24) & 0xFF),
		(uint8_t)((fingerprint >> 16) & 0xFF),
		(uint8_t)((fingerprint >> 8) & 0xFF),
		(uint8_t)(fingerprint & 0xFF)
	};
	NSData *fpPayload = [NSData dataWithBytes:fpBytes length:4];

	// Deliver fingerprint as a "heartbeat" control to all clients
	[self broadcastControl:@"heartbeat" payload:fpPayload];

	// Step 3e-f: Re-send stored broadcast packet with fresh nonce
	if (_connectedClients.count > 0 && _lastBroadcastPacket != nil)
	{
		for (NSString *peerId in [_connectedClients allKeys])
		{
			NSString *deviceKey = nil;
			for (NSString *dk in _devicePeerMap)
			{
				if ([_devicePeerMap[dk] isEqualToString:peerId])
				{
					deviceKey = dk;
					break;
				}
			}
			if (!deviceKey) continue;

			int mtu = [self mtuForDevice:deviceKey];
			int payloadLimit = mtu - kATTOverhead;
			NSArray<NSData *> *fragments = [self fragmentPacket:_lastBroadcastPacket payloadLimit:payloadLimit];
			if (fragments)
				[self enqueueNotifications:fragments forDevice:deviceKey];
		}
	}
}

// Client-side: handle heartbeat control (spec Section 9)
- (void)handleHeartbeatControl:(NSData *)payload
{
	if (!_clientJoined || payload.length < 4)
		return;

	const uint8_t *bytes = (const uint8_t *)payload.bytes;
	uint32_t remoteFingerprint = ((uint32_t)bytes[0] << 24)
	                           | ((uint32_t)bytes[1] << 16)
	                           | ((uint32_t)bytes[2] << 8)
	                           | (uint32_t)bytes[3];

	uint32_t localFingerprint = [self computeRosterFingerprint];

	// Allow one roster_request per heartbeat interval
	_rosterRequestSentThisInterval = NO;

	if (remoteFingerprint != localFingerprint && !_rosterRequestSentThisInterval)
	{
		_rosterRequestSentThisInterval = YES;
		// Send roster_request to host
		[self clientSendControl:@"roster_request" toPeerId:_hostPeerId payload:[NSData data]];
	}
}

// ──────────────────────────────────────────────────
// Successor selection (spec Section 8.3)
// ──────────────────────────────────────────────────

// Spec Section 8.3: SelectSuccessor — connected client peer IDs, exclude grace peers, sort asc, first.
- (NSString *)selectSuccessor
{
	NSMutableArray<NSString *> *candidates = [NSMutableArray array];
	for (NSString *peerId in [_connectedClients allKeys])
	{
		// Exclude peers in reconnect grace
		if (_graceTimers[peerId] != nil)
			continue;
		[candidates addObject:peerId];
	}
	if (candidates.count == 0)
		return nil;
	[candidates sortUsingSelector:@selector(compare:)];
	return candidates[0];
}

// Spec Section 8.3: SelectRecoverySuccessor — session peers with status "connected", exclude host ID, sort asc, first.
- (NSString *)selectRecoverySuccessor:(NSString *)excludeHostId
{
	NSMutableArray<NSString *> *candidates = [NSMutableArray array];
	for (LoveBleSessionPeer *peer in _sessionPeers)
	{
		if ([peer.peerId isEqualToString:excludeHostId])
			continue;
		if (![peer.status isEqualToString:@"connected"])
			continue;
		[candidates addObject:peer.peerId];
	}
	if (candidates.count == 0)
		return nil;
	[candidates sortUsingSelector:@selector(compare:)];
	return candidates[0];
}

// ──────────────────────────────────────────────────
// Graceful migration (spec Section 8.1)
// ──────────────────────────────────────────────────

- (BOOL)beginGracefulMigration
{
	pushDiagnosticEvent(_owner, @"migration: beginning graceful migration");

	// Step 1: Cancel all grace timers. Remove grace peers from roster. Increment epoch.
	NSMutableArray<NSString *> *gracePeerIds = [NSMutableArray array];
	for (NSString *peerId in [_graceTimers allKeys])
	{
		[gracePeerIds addObject:peerId];
	}
	[self cancelAllGraceTimers];
	for (NSString *peerId in gracePeerIds)
	{
		[self removeSessionPeer:peerId];
	}
	if (gracePeerIds.count > 0)
		_membershipEpoch++;

	// Step 2: Select successor
	NSString *successor = [self selectSuccessor];

	// Step 3: If no successor, return NO
	if (!successor)
	{
		pushDiagnosticEvent(_owner, @"migration: no successor available");
		return NO;
	}

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"migration: successor=%@", successor]);

	// Step 4: Encode migration payload: sessionId|successorPeerID|maxClients|roomName|membershipEpoch
	NSString *payloadStr = [NSString stringWithFormat:@"%@|%@|%d|%@|%d",
		_sessionId, successor, _maxClients, _roomName, _membershipEpoch];
	NSData *payloadData = [payloadStr dataUsingEncoding:NSUTF8StringEncoding];

	// Step 5: Broadcast session_migrating control to all clients
	[self broadcastControl:@"session_migrating" payload:payloadData];

	// Step 6: Stop accepting new data writes (set migration flag)
	_migrationInProgress = YES;

	// Step 7: Schedule departure timer (400ms)
	dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(0.4 * NSEC_PER_SEC)), dispatch_get_main_queue(), ^{
		// Step 8: On departure, call finishLeave
		pushDiagnosticEvent(self->_owner, @"migration: departure timer fired, finishing leave");
		[self finishLeave:nil];
	});

	return YES;
}

// ──────────────────────────────────────────────────
// Client reconnect (spec Section 7.1)
// ──────────────────────────────────────────────────

// Spec Section 7.1: BeginClientReconnect
- (BOOL)beginClientReconnect
{
	// Step 1: If joinedSessionId or hostPeerId empty, return NO
	if (_joinedSessionId.length == 0 || _hostPeerId.length == 0)
		return NO;

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"reconnect: beginning client reconnect (session=%@, host=%@)",
		_joinedSessionId, _hostPeerId]);

	// Step 2: Save into reconnect fields
	_reconnectSessionId = [_joinedSessionId copy];
	_reconnectHostPeerId = [_hostPeerId copy];
	_reconnectInProgress = YES;

	// Step 3: Emit peer_status "reconnecting" for local peer
	love::ble::Ble::BleEvent event;
	event.type = "peer_status";
	event.fields["peer_id"] = makeStringVariant(_localPeerId);
	event.fields["status"] = love::Variant("reconnecting", 12);
	_owner->pushEvent(event);

	// Step 4: Schedule reconnect timeout (10s)
	_reconnectTimer = [NSTimer scheduledTimerWithTimeInterval:kReconnectTimeoutSeconds
	                                                  target:self
	                                                selector:@selector(onReconnectTimeout)
	                                                userInfo:nil
	                                                 repeats:NO];

	// Step 5: Start BLE scan
	[self startScan];

	// Step 6: Set reconnectScanInProgress
	_reconnectScanInProgress = YES;

	// Step 7: Return YES
	return YES;
}

// Spec Section 7.1: OnScanResultDuringReconnect
- (void)onScanResultDuringReconnect:(LoveBleRoom *)room
{
	if (!_reconnectInProgress || !_reconnectScanInProgress)
		return;

	// Step 1: Room matches saved session/host IDs
	if ([room.sessionId isEqualToString:_reconnectSessionId] &&
	    [room.hostPeerId isEqualToString:_reconnectHostPeerId])
	{
		pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"reconnect: found matching room, rejoining"]);
		// 1a: Set reconnectJoinInProgress
		_reconnectJoinInProgress = YES;
		_reconnectScanInProgress = NO;
		// 1b: ConnectToRoom(room, migrationJoin=NO)
		[self connectToRoom:room migrationJoin:NO];
	}
	// Step 2: Same host but different session ID (host restarted)
	else if ([room.hostPeerId isEqualToString:_reconnectHostPeerId] &&
	         ![room.sessionId isEqualToString:_reconnectSessionId])
	{
		pushDiagnosticEvent(_owner, @"reconnect: host restarted with new session, failing");
		[self failReconnect];
	}
	// Step 3: Else ignore
}

// Spec Section 7.1: CompleteReconnectResume
- (void)completeReconnectResume
{
	pushDiagnosticEvent(_owner, @"reconnect: resume complete");

	// Step 1: Cancel reconnect timeout
	[_reconnectTimer invalidate];
	_reconnectTimer = nil;

	// Step 2: Clear all reconnect fields
	_reconnectSessionId = nil;
	_reconnectHostPeerId = nil;
	_reconnectInProgress = NO;
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	// Step 3: Emit peer_status "connected" for local peer
	love::ble::Ble::BleEvent event;
	event.type = "peer_status";
	event.fields["peer_id"] = makeStringVariant(_localPeerId);
	event.fields["status"] = love::Variant("connected", 9);
	_owner->pushEvent(event);
}

// Spec Section 7.1: FailReconnect
- (void)failReconnect
{
	pushDiagnosticEvent(_owner, @"reconnect: failed");

	// Step 1: Cancel reconnect timeout
	[_reconnectTimer invalidate];
	_reconnectTimer = nil;

	// Step 2: Clear all reconnect fields
	_reconnectSessionId = nil;
	_reconnectHostPeerId = nil;
	_reconnectInProgress = NO;
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	// Step 3: Stop scan
	[self stopScan];

	// Step 4: Call finishLeave(nil)
	[self finishLeave:nil];

	// Step 5: Emit session_ended "host_lost"
	love::ble::Ble::BleEvent event;
	event.type = "session_ended";
	event.fields["reason"] = love::Variant("host_lost", 9);
	_owner->pushEvent(event);
}

// Spec Section 7.1: OnReconnectTimeout
- (void)onReconnectTimeout
{
	pushDiagnosticEvent(_owner, @"reconnect: timeout expired");
	[self failReconnect];
}

// ──────────────────────────────────────────────────
// Unexpected host recovery (spec Section 8.2)
// ──────────────────────────────────────────────────

// Spec Section 8.2: BeginUnexpectedHostRecovery
- (BOOL)beginUnexpectedHostRecovery
{
	// Step 1: If transport is not resilient, return NO
	if (_transportChar != 's')
		return NO;

	// Step 2: If no valid session info, return NO
	if (_joinedSessionId.length == 0)
		return NO;

	pushDiagnosticEvent(_owner, @"migration: beginning unexpected host recovery");

	NSString *oldHostId = _hostPeerId;

	// Step 3: Remove old host from roster, add self
	[self removeSessionPeer:oldHostId];
	[self addSessionPeer:_localPeerId isHost:NO status:@"connected"];

	// Step 4: Remove grace peers from candidate set (client doesn't have grace timers,
	// but remove any peers with status "reconnecting" from consideration)
	// Note: on client side we don't maintain graceTimers, so we just use roster status.

	// Step 5: SelectRecoverySuccessor(oldHostID)
	NSString *successor = [self selectRecoverySuccessor:oldHostId];

	// Step 6: If no successor, return NO
	if (!successor)
	{
		pushDiagnosticEvent(_owner, @"migration: no recovery successor available");
		return NO;
	}

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"migration: recovery successor=%@, self=%@", successor, _localPeerId]);

	// Step 7: Create migration info. Set becomingHost = (successor == localPeerID).
	_migrationInProgress = YES;
	_migrationSuccessorId = successor;
	_migrationSessionId = [_joinedSessionId copy];
	_migrationMaxClients = 7; // default, will be overridden if migrating from known state
	_migrationRoomName = _roomName ?: @"Room";
	_migrationEpoch = _membershipEpoch;
	_becomingHost = [successor isEqualToString:_localPeerId];

	// Step 9: BeginMigrationReconnect
	[self beginMigrationReconnect];

	// Step 10: Return YES
	return YES;
}

// ──────────────────────────────────────────────────
// Migration reconnect (spec Section 8.4)
// ──────────────────────────────────────────────────

// Spec Section 8.4: BeginMigrationReconnect
- (void)beginMigrationReconnect
{
	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"migration: reconnect (becomingHost=%d)", _becomingHost]);

	// Step 1: If becoming host, begin hosting as successor
	if (_becomingHost)
	{
		[self beginHostingAsSuccessor];
	}
	else
	{
		// Step 2: Start scan to find new host's advertisement
		[self startScan];
	}

	// Step 3: Schedule migration timeout (3s)
	_migrationTimer = [NSTimer scheduledTimerWithTimeInterval:3.0
	                                                  target:self
	                                                selector:@selector(onMigrationTimeout)
	                                                userInfo:nil
	                                                 repeats:NO];
}

// Begin hosting with migrated session info (spec Section 8.4)
- (void)beginHostingAsSuccessor
{
	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"migration: becoming host for session %@", _migrationSessionId]);

	// Stop any client connection
	[self stopClientOnly];

	// Host with the migrated session info, preserving session ID
	_sessionId = [_migrationSessionId copy];
	_roomName = [_migrationRoomName copy];
	_maxClients = MAX(1, MIN(_migrationMaxClients, 7));
	_transportChar = 's'; // Migration is only for resilient transport
	_peerCount = 0;
	_membershipEpoch = _migrationEpoch;
	_hosting = YES;
	_hostServiceReady = NO;

	// Initialize host maps
	[_connectedClients removeAllObjects];
	[_devicePeerMap removeAllObjects];
	[_pendingClients removeAllObjects];
	[_notificationQueues removeAllObjects];
	[_deviceMTUs removeAllObjects];
	[_subscribedCentrals removeAllObjects];
	[self cancelAllGraceTimers];

	// Initialize roster with self as host (preserve existing non-host peers)
	// First, update self in roster as host
	[self removeSessionPeer:_localPeerId];
	[self addSessionPeer:_localPeerId isHost:YES status:@"connected"];

	// Open GATT Server with service
	CBUUID *serviceUUID = [CBUUID UUIDWithString:kServiceUUID];
	CBUUID *charUUID = [CBUUID UUIDWithString:kCharacteristicUUID];

	_messageCharacteristic = [[CBMutableCharacteristic alloc]
		initWithType:charUUID
		properties:CBCharacteristicPropertyRead | CBCharacteristicPropertyWrite | CBCharacteristicPropertyNotify
		value:nil
		permissions:CBAttributePermissionsReadable | CBAttributePermissionsWriteable];

	_gattService = [[CBMutableService alloc] initWithType:serviceUUID primary:YES];
	_gattService.characteristics = @[_messageCharacteristic];

	[_peripheralManager addService:_gattService];

	// Note: advertiseRoom and startHeartbeat will be triggered by peripheralManager:didAddService:
}

// Spec Section 8.5: CompleteMigrationResume
- (void)completeMigrationResume
{
	pushDiagnosticEvent(_owner, @"migration: resume complete");

	// Step 1: Cancel migration timeout
	[_migrationTimer invalidate];
	_migrationTimer = nil;

	NSString *savedSessionId = [_migrationSessionId copy];
	NSString *savedSuccessorId = [_migrationSuccessorId copy];
	int savedEpoch = _migrationEpoch;

	// Step 2: Clear migration state
	_migrationInProgress = NO;
	_migrationSuccessorId = nil;
	_migrationSessionId = nil;
	_migrationMaxClients = 0;
	_migrationRoomName = nil;
	_migrationEpoch = 0;
	_becomingHost = NO;

	// Step 3: Set local membershipEpoch from migration epoch
	_membershipEpoch = savedEpoch;

	// Step 4: Emit session_resumed event
	love::ble::Ble::BleEvent event;
	event.type = "session_resumed";
	event.fields["session_id"] = makeStringVariant(savedSessionId ?: @"");
	event.fields["new_host_id"] = makeStringVariant(savedSuccessorId ?: @"");

	// Include current peer roster
	std::string peersStr;
	for (LoveBleSessionPeer *peer in _sessionPeers)
	{
		if (!peersStr.empty())
			peersStr += ",";
		peersStr += std::string(peer.peerId.UTF8String) + ":" + std::string(peer.status.UTF8String);
	}
	event.fields["peers"] = love::Variant(peersStr.c_str(), peersStr.size());

	_owner->pushEvent(event);
}

// Spec Section 8.4: FailMigration
- (void)failMigration
{
	pushDiagnosticEvent(_owner, @"migration: failed");

	// Step 1: Cancel migration timeout
	[_migrationTimer invalidate];
	_migrationTimer = nil;

	// Step 2: Clear migration state
	_migrationInProgress = NO;
	_migrationSuccessorId = nil;
	_migrationSessionId = nil;
	_migrationMaxClients = 0;
	_migrationRoomName = nil;
	_migrationEpoch = 0;
	_becomingHost = NO;

	// Step 3: Call finishLeave(nil)
	[self finishLeave:nil];

	// Step 4: Emit session_ended "migration_failed"
	love::ble::Ble::BleEvent event;
	event.type = "session_ended";
	event.fields["reason"] = love::Variant("migration_failed", 16);
	_owner->pushEvent(event);
}

- (void)onMigrationTimeout
{
	pushDiagnosticEvent(_owner, @"migration: timeout expired");
	[self failMigration];
}

// Spec Section 8.4: OnSessionMigratingReceived
- (void)onSessionMigratingReceived:(LoveBlePacket *)packet
{
	NSString *payloadStr = [[NSString alloc] initWithData:packet.payload encoding:NSUTF8StringEncoding];
	if (!payloadStr)
	{
		pushDiagnosticEvent(_owner, @"migration: invalid session_migrating payload");
		return;
	}

	// Parse: sessionId|successorPeerID|maxClients|roomName|membershipEpoch
	NSArray<NSString *> *parts = [payloadStr componentsSeparatedByString:@"|"];
	if (parts.count < 5)
	{
		pushDiagnosticEvent(_owner, @"migration: session_migrating payload too short");
		return;
	}

	NSString *migSessionId = parts[0];
	NSString *successorId = parts[1];
	int migMaxClients = [parts[2] intValue];
	NSString *migRoomName = parts[3];
	int migEpoch = [parts[4] intValue];

	pushDiagnosticEvent(_owner, [NSString stringWithFormat:@"migration: received session_migrating (successor=%@, session=%@, epoch=%d)",
		successorId, migSessionId, migEpoch]);

	// Step 1: Discard write queue
	[_clientWriteQueue removeAllObjects];
	_writeInFlight = NO;

	// Step 2: Clear all in-progress fragment assemblies
	[_assemblerBySource removeAllObjects];

	// Store migration info
	_migrationInProgress = YES;
	_migrationSuccessorId = successorId;
	_migrationSessionId = migSessionId;
	_migrationMaxClients = migMaxClients;
	_migrationRoomName = migRoomName;
	_migrationEpoch = migEpoch;
	_becomingHost = [successorId isEqualToString:_localPeerId];

	// Emit session_migrating event
	love::ble::Ble::BleEvent event;
	event.type = "session_migrating";
	event.fields["session_id"] = makeStringVariant(migSessionId);
	event.fields["new_host_id"] = makeStringVariant(successorId);
	_owner->pushEvent(event);

	// Step 3: Disconnect from old host and proceed with migration reconnect (spec Section 8.4 step 3)
	[self stopClientOnly];
	[self beginMigrationReconnect];
}

// Spec Section 8.4: OnScanResultDuringMigration — called from didDiscoverPeripheral
- (void)onScanResultDuringMigration:(LoveBleRoom *)room
{
	if (!_migrationInProgress || _becomingHost)
		return;

	// Looking for the successor's advertisement with the same session ID
	if ([room.hostPeerId isEqualToString:_migrationSuccessorId] &&
	    [room.sessionId isEqualToString:_migrationSessionId])
	{
		pushDiagnosticEvent(_owner, @"migration: found successor's room, connecting");
		[self connectToRoom:room migrationJoin:YES];
	}
}

// ──────────────────────────────────────────────────
// Leave (spec Section 6.6)
// ──────────────────────────────────────────────────

- (void)leaveSession
{
	// Spec Section 6.6 step 1: If hosting with Resilient transport and clients exist,
	// attempt graceful migration. If successful, return.
	if (_hosting && _transportChar == 's' && _connectedClients.count > 0)
	{
		if ([self beginGracefulMigration])
			return;
	}

	// Spec Section 6.6 step 2: Call finishLeave with reason
	[self finishLeave:(_hosting && _connectedClients.count > 0) ? @"host_left" : nil];
}

// Spec Section 6.6: FinishLeave
- (void)finishLeave:(NSString *)remoteReason
{
	// Step 1: Cancel all timers (grace, heartbeat, migration, reconnect)
	[self cancelAllGraceTimers];
	[self stopHeartbeat];
	[_migrationTimer invalidate];
	_migrationTimer = nil;
	[_reconnectTimer invalidate];
	_reconnectTimer = nil;

	// Step 2: Clear reconnect state
	_lastBroadcastPacket = nil;
	_lastBroadcastMessageId = 0;

	// Step 3: Clear dedup
	[self clearDedupState];

	// Step 4: If remoteReason, send session_ended to all clients
	if (remoteReason && _hosting)
	{
		NSData *reasonData = [remoteReason dataUsingEncoding:NSUTF8StringEncoding];
		[self broadcastControl:@"session_ended" payload:reasonData];
	}

	// Step 5: Stop advertising, stop scanning
	[self stopScan];

	if (_hosting)
	{
		[_peripheralManager stopAdvertising];
		if (_gattService)
			[_peripheralManager removeAllServices];
		_gattService = nil;
		_messageCharacteristic = nil;
		_hostServiceReady = NO;
	}

	// Step 6: Set flags
	_hosting = NO;
	_clientLeaving = YES;

	// Step 7: Close connections
	[self stopClientOnly];

	// Step 8: Clear all maps
	[_discoveredRooms removeAllObjects];
	[_connectedClients removeAllObjects];
	[_devicePeerMap removeAllObjects];
	[_pendingClients removeAllObjects];
	[_notificationQueues removeAllObjects];
	[_deviceMTUs removeAllObjects];
	[_subscribedCentrals removeAllObjects];
	[_assemblerBySource removeAllObjects];

	// Step 9: Reset session identifiers and flags
	_sessionId = nil;
	_joinedRoomId = nil;
	_joinedSessionId = nil;
	_hostPeerId = nil;
	_peerCount = 0;
	_membershipEpoch = 0;
	_clientJoined = NO;
	_clientLeaving = NO;

	// Clear migration state
	_migrationInProgress = NO;
	_migrationSuccessorId = nil;
	_migrationSessionId = nil;
	_migrationMaxClients = 0;
	_migrationRoomName = nil;
	_migrationEpoch = 0;
	_becomingHost = NO;

	// Clear reconnect state
	_reconnectInProgress = NO;
	_reconnectSessionId = nil;
	_reconnectHostPeerId = nil;
	_reconnectScanInProgress = NO;
	_reconnectJoinInProgress = NO;

	[self resetSessionPeers];
}

// ──────────────────────────────────────────────────
// Host: broadcast and send (application-level)
// ──────────────────────────────────────────────────

- (void)hostBroadcast:(NSString *)msgType payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	if (!_hosting)
		return;

	// Spec Section 8.1 step 6: Stop accepting new data writes during migration departure
	if (_migrationInProgress)
		return;

	// Build data packet with empty toPeerId (broadcast)
	[self hostSendData:msgType toPeerId:@"" payload:payload payloadLen:payloadLen];
}

- (void)hostSendToPeer:(NSString *)peerId msgType:(NSString *)msgType
               payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	if (!_hosting)
		return;

	// Spec Section 8.1 step 6: Stop accepting new data writes during migration departure
	if (_migrationInProgress)
		return;

	// Spec Section 4.4: check if peer is connected
	if (!_connectedClients[peerId])
		return; // Unknown or reconnecting: drop silently

	[self hostSendData:msgType toPeerId:peerId payload:payload payloadLen:payloadLen];
}

// ──────────────────────────────────────────────────
// Client: broadcast and send (application-level)
// ──────────────────────────────────────────────────

- (void)clientBroadcast:(NSString *)msgType payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	if (!_clientJoined)
		return;

	// Client sends to host with empty toPeerId, host relays
	[self clientSendData:msgType toPeerId:@"" payload:payload payloadLen:payloadLen];
}

- (void)clientSendToPeer:(NSString *)peerId msgType:(NSString *)msgType
                 payload:(const uint8_t *)payload payloadLen:(uint32_t)payloadLen
{
	if (!_clientJoined)
		return;

	// Client sends directed message through host
	[self clientSendData:msgType toPeerId:peerId payload:payload payloadLen:payloadLen];
}

@end

// ──────────────────────────────────────────────────
// C++ wrapper methods
// ──────────────────────────────────────────────────

namespace love
{
namespace ble
{
namespace apple
{

Ble::Ble()
	: love::ble::Ble("love.ble.apple")
	, impl(nullptr)
{
	LoveBleImpl *objcImpl = [[LoveBleImpl alloc] initWithOwner:this];
	impl = (__bridge_retained void *)objcImpl;
}

Ble::~Ble()
{
	if (impl)
	{
		LoveBleImpl *objcImpl = (__bridge_transfer LoveBleImpl *)impl;
		[objcImpl leaveSession];
		objcImpl.centralManager.delegate = nil;
		objcImpl.peripheralManager.delegate = nil;
		objcImpl = nil;
		impl = nullptr;
	}
}

Ble::RadioState Ble::getState()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	return objcImpl.radioState;
}

void Ble::host(const std::string &roomName, int maxClients, Transport transport)
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	NSString *name = [NSString stringWithUTF8String:roomName.c_str()];
	char t = (transport == TRANSPORT_RESILIENT) ? 's' : 'r';
	[objcImpl hostWithRoomName:name maxClients:maxClients transport:t];
}

void Ble::scan()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	[objcImpl startScan];
}

void Ble::join(const std::string &roomId)
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	NSString *rid = [NSString stringWithUTF8String:roomId.c_str()];
	[objcImpl joinRoom:rid];
}

void Ble::leave()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	[objcImpl leaveSession];
}

void Ble::broadcast(const std::string &msgType, const std::vector<uint8_t> &payload)
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	NSString *mt = [NSString stringWithUTF8String:msgType.c_str()];
	const uint8_t *data = payload.empty() ? nullptr : payload.data();
	uint32_t len = (uint32_t)payload.size();

	if (objcImpl.hosting)
		[objcImpl hostBroadcast:mt payload:data payloadLen:len];
	else
		[objcImpl clientBroadcast:mt payload:data payloadLen:len];
}

void Ble::send(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload)
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	NSString *pid = [NSString stringWithUTF8String:peerId.c_str()];
	NSString *mt = [NSString stringWithUTF8String:msgType.c_str()];
	const uint8_t *data = payload.empty() ? nullptr : payload.data();
	uint32_t len = (uint32_t)payload.size();

	if (objcImpl.hosting)
		[objcImpl hostSendToPeer:pid msgType:mt payload:data payloadLen:len];
	else
		[objcImpl clientSendToPeer:pid msgType:mt payload:data payloadLen:len];
}

std::string Ble::getLocalId()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	return std::string(objcImpl.localPeerId.UTF8String);
}

bool Ble::isHost()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	return objcImpl.hosting;
}

std::vector<Ble::PeerInfo> Ble::getPeers()
{
	LoveBleImpl *objcImpl = (__bridge LoveBleImpl *)impl;
	std::vector<PeerInfo> peers;

	for (LoveBleSessionPeer *sp in objcImpl.sessionPeers)
	{
		PeerInfo info;
		info.peerId = std::string(sp.peerId.UTF8String);
		info.isHost = sp.isHost;
		info.status = std::string(sp.status.UTF8String);
		peers.push_back(info);
	}

	return peers;
}

std::string Ble::getAddress()
{
	// iOS does not expose the local BLE address; return empty
	return "";
}

} // apple
} // ble
} // love

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

#ifndef LOVE_ANDROID_H
#define LOVE_ANDROID_H

#include "config.h"

#ifdef LOVE_ANDROID

#include <string>
#include <vector>

namespace love
{
namespace android
{

/**
 * Enables or disables immersive mode where the navigation bar is hidden.
 **/
void setImmersive(bool immersive_active);
bool getImmersive();

/**
 * Gets the scale factor of the window's screen, e.g. on Retina displays this
 * will return 2.0.
 **/
double getScreenScale();

/**
 * Gets the window safe area, e.g. phone with notch display.
 * Returns false if safe area is not set.
 **/
bool getSafeArea(int &top, int &left, int &bottom, int &right);

void vibrate(double seconds);

bool directoryExists(const char *path);

bool mkdir(const char *path);

bool createStorageDirectories();

void fixupPermissionSingleFile(const std::string &savedir, const std::string &path, int mode = 0666);

void fixupExternalStoragePermission(const std::string &savedir, const std::string &path);

bool hasBackgroundMusic();

bool hasRecordingPermission();

void requestRecordingPermission();

void showRecordingPermissionMissingDialog();

bool hasBluetoothPermission();

void requestBluetoothPermission();

bool hasBluetoothLE();

std::string getBluetoothRadioState();

std::string getBluetoothAdapterAddress();

void bleApplyReliabilityConfig(double heartbeatInterval, int fragmentSpacingMs, int dedupWindow);

std::string bleDebugState();

bool bleHost(const std::string &room, int maxClients, const std::string &transport);

bool bleScan();

bool bleJoin(const std::string &roomId);

void bleLeave();

bool bleBroadcast(const std::string &msgType, const std::vector<uint8_t> &payload);

bool bleSend(const std::string &peerId, const std::string &msgType, const std::vector<uint8_t> &payload);

/**
 * Initialize Android AAsset virtual archive.
 * @return true if successful.
 */
bool initializeVirtualArchive();

/**
 * Deinitialize Android AAsset virtual archive.
 * @return true if successful.
 */
void deinitializeVirtualArchive();

/**
 * Retrieve the fused game inside the APK
 * @param physfsIO_Out Pointer to PHYSFS_Io* struct
 * @return true if there's game inside the APK. If physfsIO_Out is not null, then it contains
 * the game.love which needs to be mounted to root. false if it's not fused, in which case
 * physfsIO_Out is undefined.
 */
bool checkFusedGame(void **physfsIO_Out);

const char *getCRequirePath();

/**
 * Convert "content://" to PHYSFS_Io using SDL_IOStream.
 * @param path Path with content:// URI
 * @return PHYSFS_Io casted to void*.
 */
void *getIOFromContentProtocol(const char *path);

/**
 * Retrieve PHYSFS_AndroidInit structure.
 * @return Pointer to PHYSFS_AndroidInit structure, casted to pointer of char.
 */
const char *getArg0();

} // android
} // love

#endif // LOVE_ANDROID
#endif // LOVE_ANDROID_H

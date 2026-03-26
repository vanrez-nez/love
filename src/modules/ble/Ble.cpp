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

namespace love
{
namespace ble
{

Ble::Ble(const char *name)
	: Module(M_BLE, name)
{
}

bool Ble::poll(Event &event)
{
	std::lock_guard<std::mutex> lock(eventMutex);

	if (events.empty())
		return false;

	event = events.front();
	events.pop_front();
	return true;
}

void Ble::pushEvent(const Event &event)
{
	std::lock_guard<std::mutex> lock(eventMutex);
	events.push_back(event);
}

void Ble::pushError(const std::string &code, const std::string &detail)
{
	Event event;
	event.type = "error";
	event.fields.emplace_back("code", Variant(code));
	event.fields.emplace_back("detail", Variant(detail));
	pushEvent(event);
}

void Ble::pushDiagnostic(const std::string &platform, const std::string &message)
{
	Event event;
	event.type = "diagnostic";
	event.fields.emplace_back("platform", Variant(platform));
	event.fields.emplace_back("message", Variant(message));
	pushEvent(event);
}

STRINGMAP_CLASS_BEGIN(Ble, Ble::RadioState, Ble::RADIO_MAX_ENUM, radioState)
{
	{ "on",           Ble::RADIO_ON           },
	{ "off",          Ble::RADIO_OFF          },
	{ "unauthorized", Ble::RADIO_UNAUTHORIZED },
	{ "unsupported",  Ble::RADIO_UNSUPPORTED  },
}
STRINGMAP_CLASS_END(Ble, Ble::RadioState, Ble::RADIO_MAX_ENUM, radioState)

STRINGMAP_CLASS_BEGIN(Ble, Ble::Transport, Ble::TRANSPORT_MAX_ENUM, transport)
{
	{ "reliable",  Ble::TRANSPORT_RELIABLE  },
	{ "resilient", Ble::TRANSPORT_RESILIENT },
}
STRINGMAP_CLASS_END(Ble, Ble::Transport, Ble::TRANSPORT_MAX_ENUM, transport)

} // ble
} // love

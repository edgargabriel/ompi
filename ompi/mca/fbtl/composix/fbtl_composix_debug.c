#include "fbtl_composix_debug.h"


void composix_debug_printf(const char *str, ...)
{
	va_list args;
	va_start(args, str);

	if(DEBUG_MODE_PRINT) {
		vprintf(str, args);
	}

	va_end(args);

}


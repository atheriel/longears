#include "longears.h"

static const R_CallMethodDef longears_entries[] = {
  {"R_amqp_connect", (DL_FUNC) &R_amqp_connect, 6},
  {"R_amqp_is_connected", (DL_FUNC) &R_amqp_is_connected, 1},
  {"R_amqp_disconnect", (DL_FUNC) &R_amqp_disconnect, 1},
  {NULL, NULL, 0}
};

void R_init_longears(DllInfo *info) {
  R_registerRoutines(info, NULL, longears_entries, NULL, NULL);
  R_useDynamicSymbols(info, FALSE);
}

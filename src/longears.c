#include "longears.h"

static const R_CallMethodDef longears_entries[] = {
  {"R_amqp_connect", (DL_FUNC) &R_amqp_connect, 6},
  {"R_amqp_is_connected", (DL_FUNC) &R_amqp_is_connected, 1},
  {"R_amqp_reconnect", (DL_FUNC) &R_amqp_reconnect, 1},
  {"R_amqp_disconnect", (DL_FUNC) &R_amqp_disconnect, 1},
  {"R_amqp_declare_exchange", (DL_FUNC) &R_amqp_declare_exchange, 7},
  {"R_amqp_delete_exchange", (DL_FUNC) &R_amqp_delete_exchange, 3},
  {"R_amqp_declare_queue", (DL_FUNC) &R_amqp_declare_queue, 6},
  {"R_amqp_delete_queue", (DL_FUNC) &R_amqp_delete_queue, 4},
  {"R_amqp_bind_queue", (DL_FUNC) &R_amqp_bind_queue, 4},
  {"R_amqp_unbind_queue", (DL_FUNC) &R_amqp_unbind_queue, 4},
  {"R_amqp_publish", (DL_FUNC) &R_amqp_publish, 7},
  {"R_amqp_get", (DL_FUNC) &R_amqp_get, 3},
  {NULL, NULL, 0}
};

void R_init_longears(DllInfo *info) {
  R_registerRoutines(info, NULL, longears_entries, NULL, NULL);
  R_useDynamicSymbols(info, FALSE);
}
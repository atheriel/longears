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
  {"R_amqp_ack", (DL_FUNC) &R_amqp_ack, 3},
  {"R_amqp_nack", (DL_FUNC) &R_amqp_nack, 4},
  {"R_amqp_create_consumer", (DL_FUNC) &R_amqp_create_consumer, 7},
  {"R_amqp_listen", (DL_FUNC) &R_amqp_listen, 2},
  {"R_amqp_consume_later", (DL_FUNC) &R_amqp_consume_later, 7},
  {"R_amqp_destroy_consumer", (DL_FUNC) &R_amqp_destroy_consumer, 1},
  {"R_amqp_destroy_bg_consumer", (DL_FUNC) &R_amqp_destroy_bg_consumer, 1},
  {"R_amqp_encode_properties", (DL_FUNC) &R_amqp_encode_properties, 1},
  {"R_amqp_decode_properties", (DL_FUNC) &R_amqp_decode_properties, 1},
  {NULL, NULL, 0}
};

void R_init_longears(DllInfo *info) {
  R_registerRoutines(info, NULL, longears_entries, NULL, NULL);
  R_useDynamicSymbols(info, FALSE);
}

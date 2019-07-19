#include <stdlib.h> /* for malloc, free */
#include <string.h> /* for memcpy */

#include "tables.h"
#include "utils.h"

void encode_value(const SEXP in, amqp_field_value_t *out)
{
  if (Rf_length(in) == 0) {
    out->kind = AMQP_FIELD_KIND_VOID;
    return;
  }

  switch (TYPEOF(in)) {
  case LGLSXP: {
    int *values = LOGICAL(in), len = Rf_length(in);
    if (len == 1) {
      out->kind = AMQP_FIELD_KIND_BOOLEAN;
      out->value.boolean = values[0];
    } else {
      out->kind = AMQP_FIELD_KIND_ARRAY;
      out->value.array.num_entries = len;
      out->value.array.entries = calloc(len, sizeof(amqp_field_value_t));
      for (int i = 0; i < len; i++) {
        out->value.array.entries[i].kind = AMQP_FIELD_KIND_BOOLEAN;
        out->value.array.entries[i].value.boolean = values[i];
      }
    }
    break;
  }
  case INTSXP: {
    int *values = INTEGER(in), len = Rf_length(in);
    if (len == 1) {
      out->kind = AMQP_FIELD_KIND_I32;
      out->value.i32 = values[0];
    } else {
      out->kind = AMQP_FIELD_KIND_ARRAY;
      out->value.array.num_entries = len;
      out->value.array.entries = calloc(len, sizeof(amqp_field_value_t));
      for (int i = 0; i < len; i++) {
        out->value.array.entries[i].kind = AMQP_FIELD_KIND_I32;
        out->value.array.entries[i].value.i32 = values[i];
      }
    }
    break;
  }
  case REALSXP: {
    double *values = REAL(in);
    int len = Rf_length(in);
    if (len == 1) {
      out->kind = AMQP_FIELD_KIND_F64;
      out->value.f64 = values[0];
    } else {
      out->kind = AMQP_FIELD_KIND_ARRAY;
      out->value.array.num_entries = len;
      out->value.array.entries = calloc(len, sizeof(amqp_field_value_t));
      for (int i = 0; i < len; i++) {
        out->value.array.entries[i].kind = AMQP_FIELD_KIND_F64;
        out->value.array.entries[i].value.f64 = values[i];
      }
    }
    break;
  }
  case STRSXP: {
    int len = Rf_length(in);
    if (len == 1) {
      out->kind = AMQP_FIELD_KIND_UTF8;
      out->value.bytes = amqp_cstring_bytes(CHAR(STRING_ELT(in, 0)));
    } else {
      out->kind = AMQP_FIELD_KIND_ARRAY;
      out->value.array.num_entries = len;
      out->value.array.entries = calloc(len, sizeof(amqp_field_value_t));
      for (int i = 0; i < len; i++) {
        out->value.array.entries[i].kind = AMQP_FIELD_KIND_UTF8;
        out->value.array.entries[i].value.bytes =
          amqp_cstring_bytes(CHAR(STRING_ELT(in, i)));
      }
    }
    break;
  }
  case RAWSXP: {
    int len = Rf_length(in);
    out->kind = AMQP_FIELD_KIND_BYTES;
    out->value.bytes.len = len;
    out->value.bytes.bytes = malloc(len);
    memcpy((void *) RAW(in), out->value.bytes.bytes, len);
    break;
  }
  case VECSXP: {
    /* Named lists get turned into tables, others get turned into arrays. */
    int len = Rf_length(in);
    if (R_NilValue != Rf_getAttrib(in, R_NamesSymbol)) {
      out->kind = AMQP_FIELD_KIND_TABLE;
      encode_table(in, &out->value.table, 1);
      break;
    }
    out->kind = AMQP_FIELD_KIND_ARRAY;
    out->value.array.num_entries = len;
    out->value.array.entries = calloc(len, sizeof(amqp_field_value_t));
    for (int i = 0; i < len; i++) {
      encode_value(VECTOR_ELT(in, i), &out->value.array.entries[i]);
    }
    break;
  }
  default:
    out->kind = AMQP_FIELD_KIND_VOID;
    Rf_warning("A '%s' cannot yet be converted to a table entry. Ignoring.",
               type2char(TYPEOF(in)));
    break;
  }

  return;
}

void encode_table(const SEXP list, amqp_table_t *table, int alloc)
{
  SEXP names = PROTECT(Rf_getAttrib(list, R_NamesSymbol));
  size_t n = Rf_length(list);
  table->num_entries = (int) n;
  if (alloc) {
    table->entries = calloc(n, sizeof(amqp_table_entry_t));
  }

  SEXP elt, name;
  for (int i = 0; i < Rf_length(list); i++) {
    elt = VECTOR_ELT(list, i);
    name = STRING_ELT(names, i);

    table->entries[i].key = amqp_cstring_bytes(CHAR(name));
    encode_value(elt, &table->entries[i].value);
  }

  UNPROTECT(1);
  return;
}

SEXP decode_field_value(amqp_field_value_t value)
{
  switch (value.kind) {
  case AMQP_FIELD_KIND_VOID:
    break;

  case AMQP_FIELD_KIND_BOOLEAN:
    return ScalarLogical(value.value.boolean);
    break;

    /* Integer equivalents. */
  case AMQP_FIELD_KIND_I8:
    return ScalarInteger((int) value.value.i8);
  case AMQP_FIELD_KIND_U8:
    return ScalarInteger((int) value.value.u8);
  case AMQP_FIELD_KIND_I16:
    return ScalarInteger((int) value.value.i16);
  case AMQP_FIELD_KIND_U16:
    return ScalarInteger((int) value.value.u16);
  case AMQP_FIELD_KIND_I32:
    return ScalarInteger(value.value.i32);

    /* Numeric equivalents. */
  case AMQP_FIELD_KIND_U32:
    return ScalarReal((double) value.value.u32);
  case AMQP_FIELD_KIND_I64:
    return ScalarReal((double) value.value.i64);
  case AMQP_FIELD_KIND_F32:
    return ScalarReal((double) value.value.f32);
  case AMQP_FIELD_KIND_F64:
    return ScalarReal(value.value.f64);

  case AMQP_FIELD_KIND_UTF8:
    return amqp_bytes_to_string(&value.value.bytes);

  case AMQP_FIELD_KIND_BYTES: {
    SEXP out = PROTECT(Rf_allocVector(RAWSXP, value.value.bytes.len));
    memcpy((void *) RAW(out), value.value.bytes.bytes, value.value.bytes.len);
    UNPROTECT(1);
    return out;
  }

  case AMQP_FIELD_KIND_ARRAY: {
    int n = value.value.array.num_entries;
    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
    for (int i = 0; i < n; i++) {
      SET_VECTOR_ELT(out, i, decode_field_value(value.value.array.entries[i]));
    }
    UNPROTECT(1);
    return out;
  }

  case AMQP_FIELD_KIND_TABLE:
    return decode_table(&value.value.table);
    break;

    /* No obvious equivalents. */
  case AMQP_FIELD_KIND_TIMESTAMP:
    Rf_warning("Ignoring unsupport field type 'timestamp' in the table.");
  case AMQP_FIELD_KIND_DECIMAL:
    Rf_warning("Ignoring unsupport field type 'decimal' in the table.");
    break;
  default:
    Rf_warning("Ignoring unexpected field type '%d' in the table.", value.kind);
    break;
  }

  return R_NilValue;
}

SEXP decode_table(amqp_table_t *table)
{
  SEXP out = PROTECT(Rf_allocVector(VECSXP, table->num_entries));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, table->num_entries));

  for (int i = 0; i < table->num_entries; i++) {
    SET_STRING_ELT(names, i, amqp_bytes_to_char(&table->entries[i].key));
    SET_VECTOR_ELT(out, i, decode_field_value(table->entries[i].value));
  }

  Rf_setAttrib(out, R_NamesSymbol, names);
  UNPROTECT(2);
  return out;
}

static void free_value(amqp_field_value_t *value)
{
  /* Some field kinds allocate memory; release it here. */
  switch (value->kind) {
  case AMQP_FIELD_KIND_UTF8:
    break;

  case AMQP_FIELD_KIND_BYTES: {
    amqp_bytes_free(value->value.bytes);
    break;
  }
  case AMQP_FIELD_KIND_ARRAY: {
    for (int i = 0; i < value->value.array.num_entries; i++) {
      free_value(&value->value.array.entries[i]);
    }
    free(value->value.array.entries);
    break;
  }
  case AMQP_FIELD_KIND_TABLE:
    for (int i = 0; i < value->value.table.num_entries; i++) {
      free_value(&value->value.table.entries[i].value);
    }
    free(value->value.table.entries);
    break;
  default:
    break;
  }
}

static void R_finalize_amqp_table(SEXP ptr)
{
  amqp_table_t *table = (amqp_table_t *) R_ExternalPtrAddr(ptr);
  if (table) {
    for (int i = 0; i < table->num_entries; i++) {
      free_value(&table->entries[i].value);
    }
    free(table->entries);
    free(table);
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_table_object(amqp_table_t *table)
{
  SEXP ptr = PROTECT(R_MakeExternalPtr(table, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptr, R_finalize_amqp_table, 1);

  /* Create the "amqp_table" object. */
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 1));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 1));
  SEXP class = PROTECT(Rf_allocVector(STRSXP, 1));
  SET_VECTOR_ELT(out, 0, ptr);
  SET_STRING_ELT(names, 0, mkCharLen("ptr", 3));
  SET_STRING_ELT(class, 0, mkCharLen("amqp_table", 10));
  Rf_setAttrib(out, R_NamesSymbol, names);
  Rf_setAttrib(out, R_ClassSymbol, class);

  UNPROTECT(4);
  return out;
}

SEXP R_amqp_encode_table(SEXP list)
{
  amqp_table_t *table = malloc(sizeof(amqp_table_t));
  encode_table(list, table, 1);
  return R_table_object(table);
}

SEXP R_amqp_decode_table(SEXP ptr)
{
  amqp_table_t *table = (amqp_table_t *) R_ExternalPtrAddr(ptr);
  if (!table)
    Rf_error("Table object is no longer valid.");
  return decode_table(table);
}

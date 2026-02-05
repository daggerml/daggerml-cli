#include <stdlib.h>
#include <string.h>

#include "../third_party/msgpack/include/msgpack.h"
#include "../include/dml_msgpack.h"

static int
dml_msgpack_entry_compare(const void *left, const void *right)
{
    const DmlMapEntry *a = *(const DmlMapEntry * const *)left;
    const DmlMapEntry *b = *(const DmlMapEntry * const *)right;
    size_t min_len = a->key_len < b->key_len ? a->key_len : b->key_len;
    int cmp = 0;

    if (min_len > 0) {
        cmp = memcmp(a->key, b->key, min_len);
    }
    if (cmp != 0) {
        return cmp;
    }
    if (a->key_len < b->key_len) {
        return -1;
    }
    if (a->key_len > b->key_len) {
        return 1;
    }
    return 0;
}

static int
dml_msgpack_pack_value(msgpack_packer *packer, const DmlValue *value)
{
    size_t i;

    if (value == NULL) {
        return DML_MSGPACK_ERR_INVALID;
    }

    switch (value->type) {
    case DML_VALUE_NULL:
        return msgpack_pack_nil(packer);
    case DML_VALUE_BOOL:
        if (value->as.boolean) {
            return msgpack_pack_true(packer);
        }
        return msgpack_pack_false(packer);
    case DML_VALUE_INT:
        return msgpack_pack_long_long(packer, value->as.integer);
    case DML_VALUE_FLOAT:
        return msgpack_pack_double(packer, value->as.floating);
    case DML_VALUE_STR:
        if (msgpack_pack_str(packer, value->as.str.size) != 0) {
            return DML_MSGPACK_ERR_INVALID;
        }
        return msgpack_pack_str_body(packer, value->as.str.data, value->as.str.size);
    case DML_VALUE_LIST:
        if (msgpack_pack_array(packer, value->as.list.count) != 0) {
            return DML_MSGPACK_ERR_INVALID;
        }
        for (i = 0; i < value->as.list.count; i++) {
            if (dml_msgpack_pack_value(packer, value->as.list.items[i]) != 0) {
                return DML_MSGPACK_ERR_INVALID;
            }
        }
        return 0;
    case DML_VALUE_MAP:
        if (msgpack_pack_map(packer, value->as.map.count) != 0) {
            return DML_MSGPACK_ERR_INVALID;
        }
        if (value->as.map.count > 1) {
            DmlMapEntry **sorted = (DmlMapEntry **)calloc(value->as.map.count, sizeof(DmlMapEntry *));
            if (sorted == NULL) {
                return DML_MSGPACK_ERR_NOMEM;
            }
            for (i = 0; i < value->as.map.count; i++) {
                sorted[i] = &value->as.map.entries[i];
            }
            qsort(sorted, value->as.map.count, sizeof(DmlMapEntry *), dml_msgpack_entry_compare);
            for (i = 0; i < value->as.map.count; i++) {
                DmlMapEntry *entry = sorted[i];
                if (msgpack_pack_str(packer, entry->key_len) != 0) {
                    free(sorted);
                    return DML_MSGPACK_ERR_INVALID;
                }
                if (msgpack_pack_str_body(packer, entry->key, entry->key_len) != 0) {
                    free(sorted);
                    return DML_MSGPACK_ERR_INVALID;
                }
                if (dml_msgpack_pack_value(packer, entry->value) != 0) {
                    free(sorted);
                    return DML_MSGPACK_ERR_INVALID;
                }
            }
            free(sorted);
            return 0;
        }
        for (i = 0; i < value->as.map.count; i++) {
            DmlMapEntry *entry = &value->as.map.entries[i];
            if (msgpack_pack_str(packer, entry->key_len) != 0) {
                return DML_MSGPACK_ERR_INVALID;
            }
            if (msgpack_pack_str_body(packer, entry->key, entry->key_len) != 0) {
                return DML_MSGPACK_ERR_INVALID;
            }
            if (dml_msgpack_pack_value(packer, entry->value) != 0) {
                return DML_MSGPACK_ERR_INVALID;
            }
        }
        return 0;
    case DML_VALUE_REF:
        if (msgpack_pack_ext(packer, value->as.ref.size, DML_MSGPACK_EXT_REF) != 0) {
            return DML_MSGPACK_ERR_INVALID;
        }
        if (value->as.ref.size == 0) {
            return 0;
        }
        return msgpack_pack_ext_body(packer, value->as.ref.data, value->as.ref.size);
    case DML_VALUE_RESOURCE: {
        DmlMsgpackBuffer buffer;
        DmlValue *payload = NULL;
        DmlValue *uri = NULL;
        DmlValue *data = NULL;
        DmlValue *adapter = NULL;
        int rc;

        uri = dml_value_new_str(value->as.resource.uri, value->as.resource.uri_len);
        if (uri == NULL) {
            return DML_MSGPACK_ERR_NOMEM;
        }
        if (value->as.resource.data != NULL) {
            if (value->as.resource.data->type != DML_VALUE_REF) {
                dml_value_free(uri);
                return DML_MSGPACK_ERR_INVALID;
            }
            data = dml_value_new_ref(
                value->as.resource.data->as.ref.data,
                value->as.resource.data->as.ref.size
            );
        } else {
            data = dml_value_new_null();
        }
        if (data == NULL) {
            dml_value_free(uri);
            return DML_MSGPACK_ERR_NOMEM;
        }
        if (value->as.resource.adapter != NULL) {
            adapter = dml_value_new_str(value->as.resource.adapter, value->as.resource.adapter_len);
        } else {
            adapter = dml_value_new_null();
        }
        if (adapter == NULL) {
            dml_value_free(uri);
            dml_value_free(data);
            return DML_MSGPACK_ERR_NOMEM;
        }

        payload = dml_value_new_list(3);
        if (payload == NULL) {
            dml_value_free(uri);
            dml_value_free(data);
            dml_value_free(adapter);
            return DML_MSGPACK_ERR_NOMEM;
        }
        if (dml_value_list_set(payload, 0, uri) != 0 ||
            dml_value_list_set(payload, 1, data) != 0 ||
            dml_value_list_set(payload, 2, adapter) != 0) {
            dml_value_free(payload);
            return DML_MSGPACK_ERR_INVALID;
        }

        rc = dml_msgpack_pack(payload, &buffer);
        dml_value_free(payload);
        if (rc != DML_MSGPACK_OK) {
            return rc;
        }
        if (msgpack_pack_ext(packer, buffer.size, DML_MSGPACK_EXT_RESOURCE) != 0) {
            dml_msgpack_free_buffer(buffer.data);
            return DML_MSGPACK_ERR_INVALID;
        }
        if (buffer.size > 0 &&
            msgpack_pack_ext_body(packer, buffer.data, buffer.size) != 0) {
            dml_msgpack_free_buffer(buffer.data);
            return DML_MSGPACK_ERR_INVALID;
        }
        dml_msgpack_free_buffer(buffer.data);
        return 0;
    }
    default:
        return DML_MSGPACK_ERR_INVALID;
    }
}

int
dml_msgpack_pack(const DmlValue *value, DmlMsgpackBuffer *out_buffer)
{
    msgpack_sbuffer buffer;
    msgpack_packer packer;
    int rc;

    if (out_buffer == NULL) {
        return DML_MSGPACK_ERR_INVALID;
    }

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(&packer, &buffer, msgpack_sbuffer_write);

    rc = dml_msgpack_pack_value(&packer, value);
    if (rc != 0) {
        msgpack_sbuffer_destroy(&buffer);
        return DML_MSGPACK_ERR_INVALID;
    }

    out_buffer->data = buffer.data;
    out_buffer->size = buffer.size;
    buffer.data = NULL;
    buffer.size = 0;
    buffer.alloc = 0;
    msgpack_sbuffer_destroy(&buffer);
    return DML_MSGPACK_OK;
}

static DmlValue *
dml_msgpack_from_object(const msgpack_object *obj)
{
    DmlValue *result = NULL;
    size_t i;

    switch (obj->type) {
    case MSGPACK_OBJECT_NIL:
        return dml_value_new_null();
    case MSGPACK_OBJECT_BOOLEAN:
        return dml_value_new_bool(obj->via.boolean ? 1 : 0);
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
        return dml_value_new_int((long long)obj->via.u64);
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
        return dml_value_new_int((long long)obj->via.i64);
    case MSGPACK_OBJECT_FLOAT32:
    case MSGPACK_OBJECT_FLOAT64:
        return dml_value_new_float(obj->via.f64);
    case MSGPACK_OBJECT_STR:
        return dml_value_new_str(obj->via.str.ptr, obj->via.str.size);
    case MSGPACK_OBJECT_EXT:
        if (obj->via.ext.type == DML_MSGPACK_EXT_REF) {
            return dml_value_new_ref(obj->via.ext.ptr, obj->via.ext.size);
        }
        if (obj->via.ext.type == DML_MSGPACK_EXT_RESOURCE) {
            DmlValue *payload = NULL;
            DmlValue *resource = NULL;
            DmlValue *data = NULL;
            DmlValue *item = NULL;
            int rc;

            rc = dml_msgpack_unpack(obj->via.ext.ptr, obj->via.ext.size, &payload);
            if (rc != DML_MSGPACK_OK || payload == NULL) {
                dml_value_free(payload);
                return NULL;
            }
            if (payload->type != DML_VALUE_LIST || payload->as.list.count != 3) {
                dml_value_free(payload);
                return NULL;
            }
            item = payload->as.list.items[0];
            if (item == NULL || item->type != DML_VALUE_STR) {
                dml_value_free(payload);
                return NULL;
            }
            item = payload->as.list.items[1];
            if (item != NULL && item->type != DML_VALUE_NULL && item->type != DML_VALUE_REF) {
                dml_value_free(payload);
                return NULL;
            }
            if (item != NULL && item->type == DML_VALUE_REF) {
                data = dml_value_new_ref(item->as.ref.data, item->as.ref.size);
                if (data == NULL) {
                    dml_value_free(payload);
                    return NULL;
                }
            }
            item = payload->as.list.items[2];
            if (item != NULL && item->type != DML_VALUE_NULL && item->type != DML_VALUE_STR) {
                dml_value_free(payload);
                dml_value_free(data);
                return NULL;
            }
            resource = dml_value_new_resource(
                payload->as.list.items[0]->as.str.data,
                payload->as.list.items[0]->as.str.size,
                data,
                item != NULL && item->type == DML_VALUE_STR ? item->as.str.data : NULL,
                item != NULL && item->type == DML_VALUE_STR ? item->as.str.size : 0
            );
            dml_value_free(payload);
            if (resource == NULL) {
                return NULL;
            }
            return resource;
        }
        return NULL;
    case MSGPACK_OBJECT_ARRAY:
        result = dml_value_new_list(obj->via.array.size);
        if (result == NULL) {
            return NULL;
        }
        for (i = 0; i < obj->via.array.size; i++) {
            DmlValue *item = dml_msgpack_from_object(&obj->via.array.ptr[i]);
            if (item == NULL || dml_value_list_set(result, i, item) != 0) {
                dml_value_free(item);
                dml_value_free(result);
                return NULL;
            }
        }
        return result;
    case MSGPACK_OBJECT_MAP:
        result = dml_value_new_map(obj->via.map.size);
        if (result == NULL) {
            return NULL;
        }
        for (i = 0; i < obj->via.map.size; i++) {
            const msgpack_object_kv *kv = &obj->via.map.ptr[i];
            if (kv->key.type != MSGPACK_OBJECT_STR) {
                dml_value_free(result);
                return NULL;
            }
            DmlValue *item = dml_msgpack_from_object(&kv->val);
            if (item == NULL ||
                dml_value_map_set(result, i, kv->key.via.str.ptr, kv->key.via.str.size, item) != 0) {
                dml_value_free(item);
                dml_value_free(result);
                return NULL;
            }
        }
        if (dml_value_map_sort(result) != 0) {
            dml_value_free(result);
            return NULL;
        }
        return result;
    default:
        return NULL;
    }
}

int
dml_msgpack_unpack(const char *data, size_t size, DmlValue **out_value)
{
    msgpack_unpacked unpacked;
    msgpack_unpack_return ret;
    DmlValue *value = NULL;

    if (out_value == NULL) {
        return DML_MSGPACK_ERR_INVALID;
    }
    *out_value = NULL;

    msgpack_unpacked_init(&unpacked);
    ret = msgpack_unpack_next(&unpacked, data, size, NULL);
    if (ret != MSGPACK_UNPACK_SUCCESS) {
        msgpack_unpacked_destroy(&unpacked);
        return DML_MSGPACK_ERR_INVALID;
    }

    value = dml_msgpack_from_object(&unpacked.data);
    msgpack_unpacked_destroy(&unpacked);
    if (value == NULL) {
        return DML_MSGPACK_ERR_INVALID;
    }

    *out_value = value;
    return DML_MSGPACK_OK;
}

void
dml_msgpack_free_buffer(void *data)
{
    free(data);
}

-- Copyright (C) idevz (idevz.org)

local brz_tools = require "resty.breeze.tools"
local floor = require "math".floor
local ffi = require "ffi"
local C = ffi.C

local breeze = ffi.load("breeze")

ffi.cdef([[
typedef enum {
    StringType              = 0x3f,
    DirectStringMinType     = 0x00,
    DirectStringMaxType     = 0x3e,
    Int32Type               = 0x7f,
    DirectInt32MinType      = 0x40,
    DirectInt32MaxType      = 0x7e,
    Int64Type               = 0x98,
    DirectInt64MinType      = 0x80,
    DirectInt64MaxType      = 0x97,
    NullType                = 0x99,
    TrueType                = 0x9a,
    FalseType               = 0x9b,
    ByteType                = 0x9c,
    BytesType               = 0x9d,
    Int16Type               = 0x9e,
    Float32Type             = 0x9f,
    Float64Type             = 0xa0,
    MapType                 = 0xd9,
    ArrayType               = 0xda,
    PackedMapType           = 0xdb,
    PackedArrayType         = 0xdc,
    SchemaType              = 0xdd,
    MessageType             = 0xde,
    RefMessageType          = 0xdf,
    DirectRefMessageMaxType = 0xff
} breeze_type_t;

typedef enum {
    Int32Zero                      = 0x50,
    Int64Zero                      = 0x88,
    DirectStringMaxLength          = DirectStringMaxType,
    DirectInt32MinValue            = DirectInt32MinType - Int32Zero,
    DirectInt32MaxValue            = DirectInt32MaxType - Int32Zero,
    DirectInt64MinValue            = DirectInt64MinType - Int64Zero,
    DirectInt64MaxValue            = DirectInt64MaxType - Int64Zero,
    DirectRefMessageMaxValue       = DirectRefMessageMaxType - RefMessageType
} breeze_direct_value_limit_t;

typedef enum {
    B_BIG_ENDIAN,
    B_LITTLE_ENDIAN
} byte_order_t;

typedef void* breeze_buf_ctx_t;

typedef struct {
    uint8_t *buffer;
    byte_order_t order;
    uint32_t write_pos;
    uint32_t read_pos;
    size_t capacity;
    uint8_t _read_only;
    breeze_buf_ctx_t *bb_ctx;
} breeze_bytes_buf_t;

extern breeze_bytes_buf_t *
breeze_new_bytes_buf(size_t capacity, byte_order_t order);

extern breeze_bytes_buf_t *
breeze_new_bytes_buf_from_bytes(const uint8_t *raw_bytes, size_t size, byte_order_t order, uint8_t read_only);

extern void breeze_free_bytes_buffer(breeze_bytes_buf_t *bb);

extern void bb_write_bytes(breeze_bytes_buf_t *bb, const uint8_t *bytes, int len);

extern void bb_write_byte(breeze_bytes_buf_t *bb, uint8_t u);

extern void bb_write_uint16(breeze_bytes_buf_t *bb, uint16_t u);

extern void bb_write_uint32(breeze_bytes_buf_t *bb, uint32_t u);

extern void bb_write_uint64(breeze_bytes_buf_t *bb, uint64_t u);

extern void bb_write_zigzag32(breeze_bytes_buf_t *bb, uint32_t u);

extern void bb_write_zigzag64(breeze_bytes_buf_t *bb, uint64_t u);

extern void bb_write_varint(breeze_bytes_buf_t *bb, uint64_t u);

extern void bb_set_write_pos(breeze_bytes_buf_t *bb, uint32_t pos);

extern uint32_t bb_get_write_pos(breeze_bytes_buf_t *bb);

extern void bb_set_read_pos(breeze_bytes_buf_t *bb, uint32_t pos);

extern uint32_t bb_get_read_pos(breeze_bytes_buf_t *bb);

extern int bb_remain(breeze_bytes_buf_t *bb);

extern void bb_reset(breeze_bytes_buf_t *bb);

extern int bb_read_bytes(breeze_bytes_buf_t *bb, uint8_t *bs, int len);

extern int bb_read_byte(breeze_bytes_buf_t *bb, uint8_t *u);

extern int bb_read_uint16(breeze_bytes_buf_t *bb, uint16_t *u);

extern int bb_read_uint32(breeze_bytes_buf_t *bb, uint32_t *u);

extern int bb_read_uint64(breeze_bytes_buf_t *bb, uint64_t *u);

extern int bb_read_varint(breeze_bytes_buf_t *bb, uint64_t *u);
]])

local write_bool_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.TrueType)
end

local write_string_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.StringType)
end

local write_byte_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.ByteType)
end

local write_bytes_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.BytesType)
end

local write_int16_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.Int16Type)
end

local write_int32_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.Int32Type)
end

local write_int64_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.Int64Type)
end

local write_float32_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.Float32Type)
end

local write_float64_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.Float64Type)
end

local write_packed_map_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.PackedMapType)
end

local write_packed_array_type = function(buf)
    breeze.bb_write_byte(buf.buf, C.PackedArrayType)
end

local write_string
local write_message_type = function(buf, name)
    local index = buf.ctx:get_msg_type_index(name)
    if index < 0 then -- first write
        breeze.bb_write_byte(buf.buf, C.MessageType)
        write_string(buf, name, false)
        buf.ctx:put_msg_type(name)
    else
        if index > C.DirectRefMessageMaxValue then
            breeze.bb_write_byte(buf.buf, C.RefMessageType)
            breeze.bb_write_varint(buf.buf, index)
        else
            breeze.bb_write_byte(buf.buf, C.RefMessageType + index)
        end
    end
end

local skip_length = function(buf)
    local pos = breeze.bb_get_write_pos(buf.buf)
    breeze.bb_set_write_pos(buf.buf, pos + 4)
    return pos
end

local write_length = function(buf, keep_pos)

    local cur_pos = breeze.bb_get_write_pos(buf.buf)
    breeze.bb_set_write_pos(buf.buf, keep_pos)
    breeze.bb_write_uint32(buf.buf, cur_pos - keep_pos - 4)
    breeze.bb_set_write_pos(buf.buf, cur_pos)
end

local write_bool = function(buf, b, with_type)
    if b then
        breeze.bb_write_byte(buf.buf, C.TrueType)
    else
        breeze.bb_write_byte(buf.buf, C.FalseType)
    end
end

write_string = function(buf, s, with_type)
    assert(s, "must write a string not nil")
    local s_len = #s
    if with_type then
        if s_len <= C.DirectStringMaxLength then -- direct string
            breeze.bb_write_byte(buf.buf, s_len)
            breeze.bb_write_bytes(buf.buf, s, s_len)
            return
        end
        write_string_type(buf)
    end
    breeze.bb_write_varint(buf.buf, s_len)
    breeze.bb_write_bytes(buf.buf, s, s_len)
end

local write_byte = function(buf, b, with_type)
    if with_type then
        write_byte_type(buf)
    end
    breeze.bb_write_byte(buf.buf, b)
end

local write_bytes = function(buf, bytes, with_type)
    local b_len = #bytes
    if with_type then
        write_bytes_type(buf)
    end
    breeze.bb_write_uint32(buf.buf, b_len)
    breeze.bb_write_bytes(buf.buf, bytes, b_len)
end

local write_int16 = function(buf, i, with_type)
    if with_type then
        write_int16_type(buf)
    end
    breeze.bb_write_uint16(buf.buf, i)
end

local write_int32 = function(buf, i, with_type)
    if with_type then
        if i >= C.DirectInt32MinValue and i <= C.DirectInt32MaxValue then
            breeze.bb_write_byte(buf.buf, i + C.Int32Zero)
            return
        end
        write_int32_type(buf)
    end
    breeze.bb_write_zigzag32(buf.buf, i)
end

local write_int64 = function(buf, i, with_type)
    if with_type then
        if i >= C.DirectInt64MinValue and i <= C.DirectInt64MaxValue then
            breeze.bb_write_byte(buf.buf, i + C.Int64Zero)
            return
        end
        write_int64_type(buf)
    end
    breeze.bb_write_zigzag64(buf.buf, i)
end

local write_float32 = function(buf, f, with_type)
    if with_type then
        write_float32_type(buf)
    end
    local f_bytes = string.reverse(brz_tools.single_to(f))
    breeze.bb_write_bytes(buf.buf, f_bytes, #f_bytes)
end

local write_float64 = function(buf, f, with_type)
    if with_type then
        write_float64_type(buf)
    end
    local f_bytes = string.reverse(brz_tools.double_to(f))
    breeze.bb_write_bytes(buf.buf, f_bytes, #f_bytes)
end

local write_packed_map = function(buf, with_type, size, f)
    if with_type then
        write_packed_map_type(buf)
    end
    breeze.bb_write_varint(buf.buf, size)
    f(buf)
end

local write_packed_array = function(buf, with_type, size, f)
    if with_type then
        write_packed_array_type(buf)
    end
    breeze.bb_write_varint(buf.buf, size)
    f(buf)
end

local write_message = function(buf, msg, with_type)
    if with_type == true then
        write_message_type(buf, msg:get_name())
    end
    return msg:write_to(buf)
end

local write_string_string_map_entries = function(buf, m)
    write_string_type(buf)
    write_string_type(buf)
    for k,v in pairs(m) do
        write_string(buf, k, false)
        write_string(buf, v, false)
    end
end

local write_string_int32_map_entries = function(buf, m)
    write_string_type(buf)
    write_int32_type(buf)
    for k,v in pairs(m) do
        write_string(buf, k, false)
        write_int32(buf, v, false)
    end
end

local write_string_int64_map_entries = function(buf, m)
    write_string_type(buf)
    write_int64_type(buf)
    for k,v in pairs(m) do
        write_string(buf, k, false)
        write_int64(buf, v, false)
    end
end

local write_string_array_elems = function(buf, a)
    write_string_type(buf)
    for _,v in ipairs(a) do
        write_string(buf, v, false)
    end
end

local write_int32_array_elems = function(buf, a)
    write_int32_type(buf)
    for _,v in ipairs(a) do
        write_int32(buf, v, false)
    end
end

local write_int64_array_elems = function(buf, a)
    write_int64_type(buf)
    for _,v in ipairs(a) do
        write_int64(buf, v, false)
    end
end

local write_msg_without_type = function(buf, fields_func)
    local pos = skip_length(buf)
    fields_func(buf)
    write_length(buf, pos)
end

local write_bool_field = function(buf, index, b)
    if b ~= nil then
        breeze.bb_write_varint(buf.buf, index)
        write_bool(buf, b, true)
    end
end

local write_string_field = function(buf, index, s)
    if s ~= "" then
        breeze.bb_write_varint(buf.buf, index)
    
        write_string(buf, s, true)
    end
end

local write_byte_field = function(buf, index, b)
    if b == nil then
        return
    end
    breeze.bb_write_varint(buf.buf, index)
    -- @TODO check this convertion
    -- php pack('C') is right? compare with golang
    if type(b) == 'string' then
        b = string.byte(b)
    end
    write_byte(buf, b, true)
end

local write_bytes_field = function(buf, index, b)
    if b ~= nil then
        breeze.bb_write_varint(buf.buf, index)
        write_bytes(buf, b, true)
    end
end

local write_int16_field = function(buf, index, i)
    if i ~= 0 then
        breeze.bb_write_varint(buf.buf, index)
        write_int16(buf, i, true)
    end
end

local write_int32_field = function(buf, index, i)
        if i ~= 0 then
        breeze.bb_write_varint(buf.buf, index)
        write_int32(buf, i, true)
    end
end

local write_int64_field = function(buf, index, i)
    if i ~= 0 then
        breeze.bb_write_varint(buf.buf, index)
        write_int64(buf, i, true)
    end
end

local write_float32_field = function(buf, index, f)
    if f ~= 0 then
        breeze.bb_write_varint(buf.buf, index)
        write_float32(buf, f, true)
    end
end

local write_float64_field = function(buf, index, f)
    if f ~= 0 then
        breeze.bb_write_varint(buf.buf, index)
        write_float64(buf, f, true)
    end
end

local write_map_field = function(buf, index, size, f)
    breeze.bb_write_varint(buf.buf, index)
    write_packed_map(buf, true, size, f)
end

local write_array_field = function(buf, index, size, f)
    breeze.bb_write_varint(buf.buf, index)
    write_packed_array(buf, true, size, f)
end

local write_message_field = function(buf, index, m)
    breeze.bb_write_varint(buf.buf, index)
    
    write_message_type(buf, m:get_name())
    m:write_to(buf)
end

local check_type = function(param)
    local brz_type, write_func_name, write_type_func_name
    local p_type = type(param)
    if p_type == "string" then
        brz_type = C.StringType
        write_func_name = 'write_string'
        write_type_func_name = 'write_string_type'
    elseif p_type == "boolean" then
        if param then
            brz_type = C.TrueType
        else
            brz_type = C.FalseType
        end
        write_func_name = 'write_bool'
        write_type_func_name = 'write_bool_type'
    elseif p_type == "number" then
        if floor(param) ~= param then
            brz_type = C.Float64Type
            write_func_name = 'write_float64'
            write_type_func_name = 'write_float64_type'
        else
            brz_type = C.Int64Type
            write_func_name = 'write_int64'
            write_type_func_name = 'write_int64_type'
        end
    elseif p_type == "table" then
        local exsit, is_brz_msg = pcall(param.is_breeze_msg)
        if exsit and is_brz_msg == true then
            brz_type = C.MessageType
            write_func_name = 'write_message'
            write_type_func_name = 'write_message_type'
        else
            if brz_tools.is_assoc(param) then
                brz_type = C.PackedMapType
                write_func_name = 'c_write_packed_map'
                write_type_func_name = 'write_packed_map_type'
            else
                brz_type = C.PackedArrayType
                write_func_name = 'c_write_packed_array'
                write_type_func_name = 'write_packed_map_type'
            end
        end
    elseif p_type == "nil" or params == ngx.null then
        brz_type = C.NullType
        write_func_name = 'c_write_nil'
    end
    return brz_type, write_func_name, write_type_func_name
end

local c_write_nil = function(buf, n, with_type)
    write_byte(buf, C.NullType, false)
end

local get_write_type_func
local get_write_func
local c_write_packed_map = function(buf, map, with_type)
    if with_type then
        write_packed_map_type(buf)
    end
    local size = brz_tools.arr_size(map)
    breeze.bb_write_varint(buf.buf, size)
    if size > 0 then
        local k_mt, v_mt = next(map)
        local k_type, k_write_fn, k_write_type_fn = check_type(k_mt)
        local v_type, v_write_fn, v_write_type_fn = check_type(v_mt)
        if k_write_type_fn ~= nil then
            get_write_type_func(k_write_type_fn)(buf)
        end
        if v_write_type_fn ~= nil then
            if v_type == C.MessageType then
                get_write_type_func(v_write_type_fn)(buf, v_mt:get_name())
            else
                get_write_type_func(v_write_type_fn)(buf)
            end
        end
        for k,v in pairs(map) do
            get_write_func(k_write_fn)(buf, k, false)
            get_write_func(v_write_fn)(buf, v, false)
        end
    end
end

local c_write_packed_array = function(buf, arr, with_type)
    if with_type then
        write_packed_array_type(buf)
    end
    local size = #arr
    breeze.bb_write_varint(buf.buf, size)
    if size > 0 then
        local item_type, item_write_fn, item_write_type_fn = check_type(arr[1])
        if item_write_type_fn ~= nil then
            if item_type == C.MessageType then
                get_write_type_func(item_write_type_fn)(buf, arr[1]:get_name())
            else
                get_write_type_func(item_write_type_fn)(buf)
            end
        end
        for _,v in ipairs(arr) do
            get_write_func(item_write_fn)(buf, v, false)
        end
    end
end

local item_write_type_funcs = {
    write_string_type = write_string_type,
    write_bool_type = write_bool_type,
    write_float64_type = write_float64_type,
    write_int64_type = write_int64_type,
    write_message_type = write_message_type,
    write_packed_map_type = write_packed_map_type,
    write_packed_map_type = write_packed_map_type,
}

local item_write_funcs = {
    write_string = write_string,
    write_bool = write_bool,
    write_float64 = write_float64,
    write_int64 = write_int64,
    write_message = write_message,
    c_write_packed_map = c_write_packed_map,
    c_write_packed_array = c_write_packed_array,
    c_write_nil = c_write_nil,
}

get_write_type_func = function(func_name)
    return item_write_type_funcs[func_name]
end

get_write_func = function(func_name)
    return item_write_funcs[func_name]
end

local write_value = function(buf, params)
    if type(params) == 'table' then
        local exsit, is_brz_msg = pcall(params.is_breeze_msg)
        if exsit and is_brz_msg == true then
            write_message(buf, params, true)
            return
        end
    end
    
    local _, write_func_name = check_type(params)
    get_write_func(write_func_name)(buf, params, true)
    return
end

local _M = {
    _VERSION = "0.1.0",
    write_bool_type = write_bool_type,
    write_string_type = write_string_type,
    write_byte_type = write_byte_type,
    write_bytes_type = write_bytes_type,
    write_int16_type = write_int16_type,
    write_int32_type = write_int32_type,
    write_int64_type = write_int64_type,
    write_float32_type = write_float32_type,
    write_float64_type = write_float64_type,
    write_packed_map_type = write_packed_map_type,
    write_packed_array_type = write_packed_array_type,
    write_string = write_string,
    write_message_type = write_message_type,
    write_length = write_length,
    write_bool = write_bool,
    write_byte = write_byte,
    write_bytes = write_bytes,
    write_int16 = write_int16,
    write_int32 = write_int32,
    write_int64 = write_int64,
    write_float32 = write_float32,
    write_float64 = write_float64,
    write_packed_map = write_packed_map,
    write_packed_array = write_packed_array,
    write_message = write_message,
    write_string_string_map_entries = write_string_string_map_entries,
    write_string_int32_map_entries = write_string_int32_map_entries,
    write_string_int64_map_entries = write_string_int64_map_entries,
    write_string_array_elems = write_string_array_elems,
    write_int32_array_elems = write_int32_array_elems,
    write_int64_array_elems = write_int64_array_elems,
    write_msg_without_type = write_msg_without_type,
    write_bool_field = write_bool_field,
    write_string_field = write_string_field,
    write_byte_field = write_byte_field,
    write_bytes_field = write_bytes_field,
    write_int16_field = write_int16_field,
    write_int32_field = write_int32_field,
    write_int64_field = write_int64_field,
    write_float32_field = write_float32_field,
    write_float64_field = write_float64_field,
    write_map_field = write_map_field,
    write_array_field = write_array_field,
    write_packed_map_field = write_map_field,
    write_packed_array_field = write_array_field,
    write_message_field = write_message_field,
    write_value = write_value,
}
return _M
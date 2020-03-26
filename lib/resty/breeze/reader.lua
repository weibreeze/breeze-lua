-- Copyright (C) idevz (idevz.org)

local ffi = require "ffi"
local C = ffi.C
local brz_tools = require "resty.breeze.tools"

local breeze = ffi.load("breeze")

ffi.cdef([[
typedef enum {
BREEZE_OK = 0,
E_BREEZE_BUFFER_NOT_ENOUGH  = -1,
E_BREEZE_OVERFLOW  = -2,
E_BREEZE_UNSUPPORTED_TYPE  = -3,
E_BREEZE_MEMORY_NOT_ENOUGH  = -4,
E_BREEZE_WRONG_SIZE  = -5
} breeze_error_code_t
]])

local read_value
local read_message
local read_message_type
local read_value_by_type
local read_int64_without_type
local read_int32_without_type
local read_string_without_type
local read_map_without_type
local read_packed_map_without_type
local read_array_without_type
local read_packed_array_without_type
local read_byte_without_type
local read_bytes_without_type
local read_int16_without_type
local read_float32_without_type
local read_float64_without_type

local read_bool = function(buf, b)
    local err = read_bool_without_type(buf, b)
    return err
end

local read_type = function(bbuf)
    local msg_name = ''
    local b_msg_type = ffi.new('uint8_t[1]')
    local err = breeze.bb_read_byte(bbuf.buf, b_msg_type)
    local msg_type = b_msg_type[0]

    if err ~= C.BREEZE_OK then
        return msg_type, msg_name, error("read type error, ", err)
    end
    if msg_type >= C.MessageType then
        msg_name, err = read_message_type(bbuf, msg_type)
        msg_type = C.MessageType
    end
    return msg_type, msg_name, nil
end

read_message_type = function(bbuf, msg_type)
    local msg_name = ''
    local err
    if msg_type == C.MessageType then
        msg_name, err = read_string_without_type(bbuf.buf)
        if err == nil then
            bbuf.ctx:put_msg_type(msg_name)
        end
    elseif msg_type == C.RefMessageType then
        local index = ffi.new('uint64_t *')
        local err = breeze.bb_read_varint(bbuf.buf, index)
        if err ~= C.BREEZE_OK then
            return msg_name, 
            error("read message type with bb_read_varint error, ", err)
        end
        msg_name = bbuf.ctx:get_msg_type_name(
            C.tonumber(index))
    else
        msg_name = bbuf.ctx:get_msg_type_name(
            msg_type - C.RefMessageType)
    end
    return msg_name, err
end

read_int64_without_type = function(buf)
    local i64 = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_zigzag64(buf, i64)
    if err ~= C.BREEZE_OK then
        return nil,
        error("read int64 without type using read_int64_without_type error, ", err)
    end
    return tonumber(i64[0]), nil
end

read_int32_without_type = function(buf)
    local i32 = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_zigzag32(buf, i32)
    if err ~= C.BREEZE_OK then
        return nil,
        error("read int32 without type using bb_read_zigzag32 error, ", err)
    end
    return tonumber(i32[0]), nil
end

read_int16_without_type = function(buf)
    local i16 = ffi.new('uint16_t[1]')
    local err = breeze.bb_read_uint16(buf, i16)
    if err ~= C.BREEZE_OK then
        return nil,
        error("read int16 without type using bb_read_uint16 error, ", err)
    end
    return tonumber(i16[0]), nil
end

read_string_without_type = function(buf)
    local c_size = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_varint(buf, c_size)
    if err ~= C.BREEZE_OK then
        return '', 
        error("read message with out type using bb_read_varint error, ", err)
    end
    local size = c_size[0]
    local bytes = ffi.new('uint8_t[?]', size)
    err = breeze.bb_read_bytes(buf, bytes, size)
    if err ~= C.BREEZE_OK then
        return '',
        error("read message with out type using bb_read_bytes error, ", err)
    end
    return ffi.string(bytes, size), nil
end

read_map_without_type = function(bbuf)
    local size_c = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_varint(bbuf.buf, size_c)
    if err ~= C.BREEZE_OK then
        return nil, 
        error('read map without type using bb_read_varint error, ', err)
    end
    local res = {}
    local size = tonumber(size_c[0])
    if size > 0 then
        for i=1,size do
            local k, err = read_value(bbuf)
            if err ~= nil then
                return nil, err
            end
            local v, err = read_value(bbuf)
            if err ~= nil then
                return nil, err
            end
            res[k] = v
        end
    end
    return res, nil
end

read_packed_map_without_type = function(bbuf)
    local size_c = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_varint(bbuf.buf, size_c)
    if err ~= C.BREEZE_OK then
        return nil, error('read map without type using bb_read_varint error, ', err)
    end
    local res = {}
    local size = tonumber(size_c[0])
    if size > 0 then
        local key_type, value_type, key_msg_name, value_msg_name
        key_type, key_msg_name, err = read_type(bbuf)
        if err ~= nil then
            return nil, err
        end
        value_type, value_msg_name, err = read_type(bbuf)
        if err ~= nil then
            return nil, err
        end
        for i=1,size do
            local p_k, err = read_value_by_type(bbuf, false, key_type, key_msg_name)
            if err ~= nil then
                return nil, err
            end
            
            local p_v, err = read_value_by_type(bbuf, false, value_type, value_msg_name)
            if err ~= nil then
                return nil, err
            end
            res[p_k] = p_v
        end
    end
    return res, nil
end

read_array_without_type = function(bbuf)
    local size_c = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_varint(bbuf.buf, size_c)
    if err ~= C.BREEZE_OK then
        return nil, error('read map without type using bb_read_varint error, ', err)
    end
    local res = {}
    local size = tonumber(size_c[0])
    if size > 0 then
        for i=1,size do
            local arr, err = read_value(bbuf)
            if err ~= nil then
                return nil, err
            end
            res[i] = arr
        end
    end
    return res, nil
end

read_packed_array_without_type = function(bbuf)
    local size_c = ffi.new('uint64_t[1]')
    local err = breeze.bb_read_varint(bbuf.buf, size_c)
    if err ~= C.BREEZE_OK then
        return nil, error('read map without type using bb_read_varint error, ', err)
    end
    local res = {}
    local size = tonumber(size_c[0])
    if size > 0 then
        local arr_type, arr_msg_name
        arr_type, arr_msg_name, err = read_type(bbuf)
        if err ~= nil then
            return nil, err
        end
        for i=1,size do
            local arr, err = read_value_by_type(bbuf, false, arr_type, arr_msg_name)
            if err ~= nil then
                return nil, err
            end
            res[i] = arr
        end
    end
    return res, nil
end

read_byte_without_type = function(bbuf)
    local f_byte_c = ffi.new('uint8_t[1]')
    local err = breeze.bb_read_byte(bbuf.buf, f_byte_c)
    if err ~= C.BREEZE_OK then
        return nil, error("read byte without type bb_read_byte error, ", err)
    end
    return string.char(f_byte_c[0]), nil
end

read_bytes_without_type = function(bbuf)
    local bytes_len_c = ffi.new('uint32_t[1]')
    local err = breeze.bb_read_uint32(bbuf.buf, bytes_len_c)
    if err ~= C.BREEZE_OK then
        return nil, error("read bytes lens bb_read_uint32 error, ", err)
    end
    local bytes_len = bytes_len_c[0]
    local bytes_c = ffi.new('uint8_t[?]', bytes_len)
    err = breeze.bb_read_bytes(bbuf.buf, bytes_c, bytes_len)
    if err ~= C.BREEZE_OK then
        return nil, error("read bytes without type bb_read_bytes error, ", err)
    end
    return ffi.string(bytes_c, bytes_len), nil
end

read_float32_without_type = function(bbuf)
    local f_bytes_c = ffi.new('uint8_t[?]', 4)
    local err = breeze.bb_read_bytes(bbuf.buf, f_bytes_c, 4)
    if err ~= C.BREEZE_OK then
        return nil, error("read float32 without type bb_read_bytes error, ", err)
    end
    return brz_tools.single_from(string.reverse(ffi.string(f_bytes_c, 4)))
end

read_float64_without_type = function(bbuf)
    local f_bytes_c = ffi.new('uint8_t[?]', 8)
    local err = breeze.bb_read_bytes(bbuf.buf, f_bytes_c, 8)
    if err ~= C.BREEZE_OK then
        return nil, error("read float64 without type error to read bytes, ", err)
    end
    return brz_tools.double_from(string.reverse(ffi.string(f_bytes_c, 8)))
end

read_value = function(bbuf)
    return read_value_by_type(bbuf, true, t, msg_name)
end

read_value_by_type = function(bbuf, with_type, msg_type, msg_name)
    local err
    if with_type then
        msg_type, msg_name, err = read_type(bbuf)
        if err ~= nil then
            return nil, err
        end
    end
    
    -- string
    if msg_type <= C.StringType then
        local s = ''
        if msg_type == C.StringType then
            s, err = read_string_without_type(bbuf.buf)
            if err ~= nil then
                return nil, err
            end
            return s, nil
        else
            local bytes = ffi.new('uint8_t[?]', msg_type)
            err = breeze.bb_read_bytes(bbuf.buf, bytes, msg_type)
            if err ~= C.BREEZE_OK then
                return nil, 
                error("read value by type read small string error, ", err)
            end
            return ffi.string(bytes, msg_type), nil
        end
    end

    -- int32
    if msg_type >= C.DirectInt32MinType and msg_type <= C.Int32Type then
        local i32
        if msg_type == C.Int32Type then
            i32, err = read_int32_without_type(bbuf.buf)
        else
            i32 = msg_type - C.Int32Zero
        end
        if err ~= nil then
            return nil, err
        end
        return i32, nil
    end

    -- int64
    if msg_type >= C.DirectInt64MinType and msg_type <= C.Int64Type then
        local i64
        if msg_type == C.Int64Type then
            i64, err = read_int64_without_type(bbuf.buf)
        else
            i64 = msg_type - C.Int64Zero
        end
        if err ~= nil then
            return nil, err
        end
        return i64, nil
    end

    if msg_type == C.NullType then
        return nil, nil
    elseif msg_type == C.MessageType then
        local brz_gmsg = require "resty.breeze.generic_msg"
        local gmsg = brz_gmsg:new(msg_name)
        gmsg:read_from(bbuf)
        return gmsg, nil
    elseif msg_type == C.MapType then
        return read_map_without_type(bbuf)
    elseif msg_type == C.PackedMapType then
        return read_packed_map_without_type(bbuf)
    elseif msg_type == C.ArrayType then
        return read_array_without_type(bbuf)
    elseif msg_type == C.PackedArrayType then
        return read_packed_array_without_type(bbuf)
    elseif msg_type == C.TrueType then
        return true, nil
    elseif msg_type == C.FalseType then
        return false, nil
    elseif msg_type == C.ByteType then
        return read_byte_without_type(bbuf)
    elseif msg_type == C.BytesType then
        return read_bytes_without_type(bbuf)
    elseif msg_type == C.Int16Type then
        return read_int16_without_type(bbuf.buf)
    elseif msg_type == C.Float32Type then
        return read_float32_without_type(bbuf)
    elseif msg_type == C.Float64Type then
        return read_float64_without_type(bbuf)
    end
    return nil, error("breezeread upsupported type: ", msg_type)
end

read_message = function(bbuf, read_fields_func)
    assert(type(read_fields_func) == 'function', 
        "must have a callable read_fields_func")
    local buf = bbuf.buf
    local c_total = ffi.new('uint32_t[1]')
    local err = breeze.bb_read_uint32(buf, c_total)
    if err ~= C.BREEZE_OK then
        error("read message error when bb_read_uint32", err)
        return nil, err
    end
    local total = c_total[0]
    if total > 0 then
        local e = breeze.bb_get_read_pos(buf) + total
        local c_index = ffi.new('uint64_t[1]')
        local index = 0
        
        while breeze.bb_get_read_pos(buf) < e do
            err = breeze.bb_read_varint(buf, c_index)
            if err ~= C.BREEZE_OK then
                error("read message error when bb_read_varint", err)
                return nil, err
            end
            index = tonumber(c_index[0])
            read_fields_func(bbuf, index)
        end
        if breeze.bb_get_read_pos(buf) ~= e then
            error("Breeze: read byte size not correct")
        end
    end
end

local _M = {
    _VERSION = "0.0.1",
    read_value = read_value,
    read_message = read_message,
    read_value_by_type = read_value_by_type,
}

return _M

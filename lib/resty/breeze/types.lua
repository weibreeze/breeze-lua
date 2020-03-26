-- Copyright (C) idevz (idevz.org)

local C = require "ffi".C
local tab_concat = table.concat
local brz_w = require "resty.breeze.writer"

local brz_types = {
    string = C.StringType,
    direct_string_min = C.DirectStringMinType,
    direct_string_max = C.DirectStringMaxType,
    int32 = C.Int32Type,
    direct_int32_min = C.DirectInt32MinType,
    direct_int32_max = C.DirectInt32MaxType,
    int64 = C.Int64Type,
    direct_int64_min = C.DirectInt64MinType,
    direct_int64_max = C.DirectInt64MaxType,
    null = C.NullType,
    bool = C.TrueType,
    byte = C.ByteType,
    bytes = C.BytesType,
    int16 = C.Int16Type,
    float32 = C.Float32Type,
    float64 = C.Float64Type,
    map = C.MapType,
    array = C.ArrayType,
    packed_map = C.PackedMapType,
    packed_array = C.PackedArrayType,
    schema = C.SchemaType,
    message = C.MessageType,
    ref_message = C.RefMessageType,
    direct_ref_message_max = C.DirectRefMessageMaxType
}



local M = {
    _VERSION = "0.0.1"
}

local mt = {
    __index = function(self, name)
        if name == "write" then
            return brz_w[tab_concat({name, "_", self.type_name})]
        elseif name == "read" then
            return brz_r[tab_concat({name, "_", self.type_name})]
        elseif name == "write_type" then
            return brz_w[tab_concat({"write_", self.type_name, "_type"})]
        end
    end
}

function M.new(self, type_name, ...)
    assert(type_name, "must have a type name")
    assert(brz_types[type_name] ~= nil, "type name is not in the brz_types list")
    if type_name == 'message' then
        -- body
    end
    local brz_type_t = {
        type_name = type_name,
    }
    return setmetatable(brz_type_t, mt)
end

return M
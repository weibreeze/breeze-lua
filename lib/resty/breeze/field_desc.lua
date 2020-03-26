-- Copyright (C) idevz (idevz.org)

local ffi = require "ffi"

ffi.cdef([[
typedef struct {
    uint8_t index;
    const char *name;
    const char *bf_type;
} breeze_field_desc_t;
]])

local breeze_field_desc_t_mt = {
    __index = {
        get_index = function(fdesc)
            return tonumber(fdesc.index)
        end,
        set_index = function(fdesc, index)
            fdesc.index = index
        end,
        get_name = function(fdesc)
            return ffi.string(fdesc.name)
        end,
        set_name = function(fdesc, name)
            fdesc.name = name
        end,
        get_type = function(fdesc)
            return ffi.string(fdesc.bf_type)
        end,
        set_type = function(fdesc, bf_type)
            fdesc.bf_type = bf_type
        end
    }
}

return  ffi.metatype("breeze_field_desc_t", breeze_field_desc_t_mt)
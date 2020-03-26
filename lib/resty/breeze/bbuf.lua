-- Copyright (C) idevz (idevz.org)

local ffi = require "ffi"

local breeze = ffi.load('breeze')

local _brz_buf_ctx = {}
local _brz_buf_ctx_mt = {__index = _brz_buf_ctx}

function _brz_buf_ctx.new(self)
    local brz_buf_ctx_t = {
        msg_type_ref_count = 0,
        msg_type_ref_name = {},
        msg_type_ref_index = {}
    }
    return setmetatable(brz_buf_ctx_t, _brz_buf_ctx_mt)
end

function _brz_buf_ctx.get_msg_type_name(self, index)
    local name
    if self.msg_type_ref_name ~= nil then
        name = self.msg_type_ref_name[index]
    end
    return name
end

function _brz_buf_ctx.get_msg_type_index(self, name)
    if self.msg_type_ref_index ~= nil and 
    self.msg_type_ref_index[name] ~= nil then
        return self.msg_type_ref_index[name]
    end
    return -1
end

function _brz_buf_ctx.put_msg_type(self, name)
    self.msg_type_ref_count = self.msg_type_ref_count + 1
    self.msg_type_ref_name[self.msg_type_ref_count] = name
    self.msg_type_ref_index[name] = self.msg_type_ref_count
end

return {
    breeze_new_bytes_buf = function(...)
        return {
            buf = breeze.breeze_new_bytes_buf(...),
            ctx = _brz_buf_ctx:new()
        }
    end,
    breeze_new_bytes_buf_from_bytes = function(...)
        return {
            buf = breeze.breeze_new_bytes_buf_from_bytes(...),
            ctx = _brz_buf_ctx:new()
        }
    end
}

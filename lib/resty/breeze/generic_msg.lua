-- Copyright (C) idevz (idevz.org)

local brz_w = require "resty.breeze.writer"
local brz_r = require "resty.breeze.reader"
local brz_tools = require "resty.breeze.tools"

local _M = {
    _VERSION = "0.0.1",
    brz_generic_msg_name = name,
    alias = "",
    fields = {},
    name_to_index = {},
    schema = {},
    checked = false -- is check schema
}

local check_schema = function(gmsg) -- gmsg like this
    if not gmsg.checked and brz_tools.is_empty(gmsg.schema) then
        local get_schema = brz_tools:get_schema_seeker():seek_schema(gmsg.brz_generic_msg_name)
        -- this check avoid a 'stack overflow' error for check schema loop in __index
        if get_schema ~= nil then
            gmsg.schema = get_schema
        end
        if not brz_tools.is_empty(gmsg.schema) then
            for _,field in ipairs(gmsg.schema:get_fields()) do
                gmsg.name_to_index[field:get_name()] = field:get_index()
            end
        end
    end
end

local mt = {
    __index = function(self, name)
        check_schema(self)
        if not brz_tools.is_empty(self.name_to_index) and 
        self.name_to_index[name] ~= nil then
            return self.fields[self.name_to_index[name]]
        end
        local err_msg = table.concat({
            'can not found field by name:',
             name,
             ', message name:', self.brz_generic_msg_name
        })
        print(err_msg)
    end,
    __newindex = function(self, name, value)
        check_schema(self)
        if not brz_tools.is_empty(self.name_to_index) and
        self.name_to_index[name] ~= nil then
            self.fields[self.name_to_index[name]] = value
            return
        end
        local err_msg = table.concat({
            'can not found field by name:',
            name,
            ', message name:', self.brz_generic_msg_name
        })
        print(err_msg)
    end
}

function _M.new(self, name )
    assert(name ~= nil, "generic message must has a name")
    local gmsg = brz_tools.deepcopy(self)
    gmsg.brz_generic_msg_name = name
    return setmetatable(gmsg, mt)
end

function _M.put_name_index(self, name, index)
    self.name_to_index[name] = index
    return self
end

function _M.set_name_index(self, name_to_index)
    self.name_to_index = name_to_index
end

-- put a field into message.
-- @param int $index must greater than -1
-- @param mixed $value must not null
function _M.put_field(self, index, value)
    if index > -1 and value ~= nil then
        self.fields[index] = value
    end
end

function _M.get_field(self, index)
    return self.fields[index]
end

function _M.write_to(self, buf)
    if self.fields ~= nil then
        check_schema(self)
        brz_w.write_msg(buf, function(fbuf)
            for i,v in ipairs(self.fields) do
                local type
                if self.schema ~= nil then
                    local f = self.schema:get_field(index)
                    if f ~= nil then
                        type = f.type
                    end
                end
                if type == nil then
                    type = brz_w.check_type(v)
                end
                brz_w.write_msg_field(fbuf, index, v, type)
            end
        end)
    end
end

function _M.read_from(self, bbuf)
    check_schema(self)
    brz_r.read_message(bbuf, function(fbuf, index)
        if self.schema.get_field ~= nil then
            local f = self.schema:get_field(index)
            if f ~= nil then
                self.fields[index] = brz_r.read_value_by_type(fbuf, true, nil, nil)
                return
            end
        end
        self.fields[index] = brz_r.read_value(fbuf)
    end)
end

function _M.get_name(self)
    return self.brz_generic_msg_name
end

function _M.set_message_name(self, name)
    self.brz_generic_msg_name = name
end

function _M.message_alias(self)
    return self.alias
end

function _M.set_message_alias(self, alias)
    self.alias = alias
end

function _M.get_schema(self)
    return self.schema
end

function _M.set_schema(self, schema)
    self.schema = schema
end

function _M.default_instance(self)
    return _M.new(self, self.name) -- with last name for write
end

return _M

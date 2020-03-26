-- Copyright (C) idevz (idevz.org)

local _M = {
    _VERSION = "0.1.0"
}

local mt = {__index = _M}

function _M.new(self, name)
    local schema = {
        brz_schema_name = name or "",
        alias = "",
        fields = {}
    }
    return setmetatable(schema, mt)
end

function _M.is_breeze_schema(self)
    return true
end

function _M.get_name(self)
    return self.brz_schema_name
end

function _M.set_name(self, name)
    self.brz_schema_name = name
end

function _M.get_alias(self)
    return self.alias
end

function _M.set_alias(self, alias)
    self.alias = alias
end

function _M.get_fields(self)
    return self.fields
end

function _M.set_fields(self, fields)
    self.fields = fields
end

function _M.put_field(self, field)
    table.insert(self.fields, field)
    return self
end

function _M.get_field(self, index)
    return self.fields[index]
end

return _M

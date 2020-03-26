local _M = {
    _VERSION = "0.0.1",
    schemas = {},
}

_M.add_schema = function(self, msg_name, schema)
    if self.schemas[msg_name] == nil and schema:is_breeze_schema() then
        self.schemas[msg_name] = schema
    end
end

_M.seek_schema = function(self, msg_name )
    return self.schemas[msg_name]
end

return _M
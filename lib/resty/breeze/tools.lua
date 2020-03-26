-- Copyright (C) idevz (idevz.org)

local NULL = {}
local _M = {
    _VERSION = "0.0.1",
    schema_seeker = NULL,
}

local mt = {__index = _M}

_M.get_schema_seeker = function(self)
    if self.schema_seeker ~= NULL then
        return self.schema_seeker
    end
    return require "resty.breeze.seekers.common"
end

_M.set_schema_seeker = function(self, seeker)
    if seeker ~= nil and type(seeker.seek_schema) ~= 'function' then
        self.schema_seeker = seeker
    end
end

local deepcopy
deepcopy = function(orig, copies)
    copies = copies or {}
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        if copies[orig] then
            copy = copies[orig]
        else
            copy = {}
            copies[orig] = copy
            for orig_key, orig_value in next, orig, nil do
                copy[deepcopy(orig_key, copies)] = deepcopy(orig_value, copies)
            end
            setmetatable(copy, deepcopy(getmetatable(orig), copies))
        end
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end
_M.deepcopy = deepcopy

local grab_byte = function(v)
    return math.floor(v / 256), string.char(math.floor(v) % 256)
end
-- Converts an 8-byte little-endian string to a IEEE754 double number
local double_from = function(x)
    local sign = 1
    local mantissa = string.byte(x, 7) % 16
    for i = 6, 1, -1 do mantissa = mantissa * 256 + string.byte(x, i) end
    if string.byte(x, 8) > 127 then sign = -1 end
    local exponent = (string.byte(x, 8) % 128) * 16 +
                     math.floor(string.byte(x, 7) / 16)
    if exponent == 0 then return 0 end
    mantissa = (math.ldexp(mantissa, -52) + 1) * sign
    return math.ldexp(mantissa, exponent - 1023)
end
_M.double_from = double_from

-- Converts a 4-byte little-endian string to a IEEE754 single number
local single_from = function(x)
    local sign = 1
    local mantissa = string.byte(x, 3) % 128
    for i = 2, 1, -1 do mantissa = mantissa * 256 + string.byte(x, i) end
    if string.byte(x, 4) > 127 then sign = -1 end
    local exponent = (string.byte(x, 4) % 128) * 2 +
                     math.floor(string.byte(x, 3) / 128)
    if exponent == 0 then return 0 end
    mantissa = (math.ldexp(mantissa, -23) + 1) * sign
    return math.ldexp(mantissa, exponent - 127)
end
_M.single_from = single_from

-- Converts a IEEE754 single number to a 4-byte little-endian string
local single_to = function(x)
    local sign = 0
    if x < 0 then sign = 1; x = -x end
    local mantissa, exponent = math.frexp(x)
    if x == 0 then -- zero
      mantissa = 0; exponent = 0
    else
      mantissa = (mantissa * 2 - 1) * math.ldexp(0.5, 24)
      exponent = exponent + 126
    end
    local v, byte = "" -- convert to bytes
    x, byte = grab_byte(mantissa); v = v..byte -- 7:0
    x, byte = grab_byte(x); v = v..byte -- 15:8
    x, byte = grab_byte(exponent * 128 + x); v = v..byte -- 23:16
    x, byte = grab_byte(sign * 128 + x); v = v..byte -- 31:24
    return v
end
_M.single_to = single_to

-- Converts a IEEE754 double number to an 8-byte little-endian string
local double_to = function(x)
    local sign = 0
    if x < 0 then sign = 1; x = -x end
    local mantissa, exponent = math.frexp(x)
    if x == 0 then -- zero
      mantissa, exponent = 0, 0
    else
      mantissa = (mantissa * 2 - 1) * math.ldexp(0.5, 53)
      exponent = exponent + 1022
    end
    local v, byte = "" -- convert to bytes
    x = mantissa
    for i = 1,6 do
      x, byte = grab_byte(x); v = v..byte -- 47:0
    end
    x, byte = grab_byte(exponent * 16 + x); v = v..byte -- 55:48
    x, byte = grab_byte(sign * 128 + x); v = v..byte -- 63:56
    return v
end
_M.double_to = double_to

_M.is_empty = function(t)
    return t == nil or next(t) == nil
end

_M.is_assoc = function(t)
    local orgin_len = #t
    if #t == 0 then
        return true
    end
    t['check_if_is_a_array_or_a_hash'] = false
    local new_len = #arr_keys(t)
    t['check_if_is_a_array_or_a_hash'] = nil
    if new_len - 1 == orgin_len then
        return false
    else
        return true
    end
end

_M.arr_size = function(t)
    local size = 0
    for _ in pairs(t) do size = size + 1 end
    return size
end

return _M

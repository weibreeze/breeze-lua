-- Copyright (C) idevz (idevz.org)
package.path = "/Users/idevz/code/z/git/weibo-or/motan-openresty/lib/?.lua;/Users/idevz/code/breeze/lua-resty-breeze/lib/?.lua;" .. package.path

local utils = require "motan.utils"
local consts = require "motan.consts"
local brz_w = require "resty.breeze.writer"
local brz_r = require "resty.breeze.reader"
local brz_buf = require "resty.breeze.bbuf"

local ffi = require "ffi"
local C = ffi.C

local breeze = ffi.load("breeze")

print_r = function(...)
    print(utils.sprint_r(...))
end

sprint_r = function( ... )
    return utils.sprint_r(...)
end

print_b = function(buf, index)
    local index = index or 0
    local res_arr = {string.byte(buf, 1, -1)}
    -- print_r(res_arr)
    for i,v in ipairs(res_arr) do
        print(i - 2 + index,v)
    end
    print(#res_arr)
end

local _M = {
    _VERSION = "0.1.0"
}

ffi.cdef([[
typedef struct {
	uint8_t *buffer;
	byte_order_t order;
	uint32_t write_pos;
	uint32_t read_pos;
	size_t capacity;
	uint8_t _read_only;
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

extern void bb_write_varint(breeze_bytes_buf_t *bb, uint64_t u, int *len);

extern void bb_set_write_pos(breeze_bytes_buf_t *bb, uint32_t pos);

extern void bb_set_read_pos(breeze_bytes_buf_t *bb, uint32_t pos);

extern int bb_remain(breeze_bytes_buf_t *bb);

extern void bb_reset(breeze_bytes_buf_t *bb);

extern int bb_read_bytes(breeze_bytes_buf_t *bb, uint8_t *bs, int len);

extern int bb_read_byte(breeze_bytes_buf_t *bb, uint8_t *u);

extern int bb_read_uint16(breeze_bytes_buf_t *bb, uint16_t *u);

extern int bb_read_uint32(breeze_bytes_buf_t *bb, uint32_t *u);

extern int bb_read_uint64(breeze_bytes_buf_t *bb, uint64_t *u);

extern int bb_read_zigzag32(breeze_bytes_buf_t *bb, uint64_t *u);

extern int bb_read_zigzag64(breeze_bytes_buf_t *bb, uint64_t *u);

extern int bb_read_varint(breeze_bytes_buf_t *bb, uint64_t *u);
]])

function _M.serialize(param)
    local bbuf = brz_buf.breeze_new_bytes_buf(256, breeze.B_BIG_ENDIAN)
    brz_w.write_value(bbuf, param)
    return ffi.string(bbuf.buf.buffer, bbuf.buf.write_pos), nil
end

function _M.serialize_multi(params)
    if utils.is_empty(params) then
        return nil, nil
    end
    local bbuf = brz_buf.breeze_new_bytes_buf(256, breeze.B_BIG_ENDIAN)
    for _,param in ipairs(params) do
        brz_w.write_value(bbuf, param)
    end
    return ffi.string(bbuf.buf.buffer, bbuf.buf.write_pos), nil
end

function _M.get_serialize_num()
    return consts.MOTAN_SERIALIZE_BREEZE
end

function _M.deserialize(data)
    local bbuf = brz_buf.breeze_new_bytes_buf_from_bytes(data,
        #data, breeze.B_BIG_ENDIAN, 1)
    
    local ok, res, err = pcall(brz_r.read_value, bbuf)
    if not ok then
        return nil, err
    end

    return res, nil
end

function _M.deserialize_multi(data, args_num)
    local res, err = {}
    local data_len = #data
    local bbuf = brz_buf.breeze_new_bytes_buf_from_bytes(data,
        data_len, breeze.B_BIG_ENDIAN, 1)
    if args_num ~= nil then
        for i=1,args_num do
            local tmp, err = brz_r.read_value(bbuf)
            if err ~= nil then
                return nil, err
            end
            table.insert(res, tmp)
        end
    else
        while(bbuf.buf.read_pos < data_len) do
            local tmp, err = brz_r.read_value(bbuf)
            if err ~= nil then
                if bbuf.buf.read_pos == data_len then
                    break
                end
                return nil, err
            end
            table.insert(res, tmp)
        end
    end
    return res, nil
end

-- return _M

local sub_message_t = require "resty.breeze.gcode.testsubmsg"
local message_t = require "resty.breeze.gcode.testmsg"
local myenum_t = require "resty.breeze.gcode.myenum"


local sub_msg1 = sub_message_t:new({
    myString = "test_brz_sub_msg1_string",
    myInt = 2^32,
    myInt64 = 2^34,
    myFloat32 = 3.2,
    myFloat64 = 5.8,
    myByte = 'k',
    myBytes = "test_brz_mybytest",
    myMap1 = {
        key1 = "bytes1",
        key2 = "bytes2",
    },
    myMap2 = {
        {1,2,3,4},
        {5,6,7},
    },
    myArray = {8,9,10,11},
    myBool = false
})

local sub_msg2 = sub_message_t:new({
    myString = "test_brz_sub_msg2_string"
})

local my_enum1 = myenum_t:new(1)
local my_enum2 = myenum_t:new(2)
local my_enum3 = myenum_t:new(3)

local msg = message_t:new({
    myInt = 99999,
    myString = "test_brz_msg_string",
    myMap = {
        sub_msg1 = sub_msg1,
        sub_msg2 = sub_msg2
    },
    myArray = {sub_msg1, sub_msg2},
    subMsg = sub_msg1,
    myEnum = my_enum1,
    enumArray = {my_enum1, my_enum2, my_enum3}
})

-- local t = {a=sub_msg1, b=sub_msg2}
-- local t = {sub_msg2}
local t = msg
local res = _M.serialize(t)
-- print_b(res, 1)
local lres = _M.deserialize(res)
print_r(lres.myMap.sub_msg1.myMap2[1])
print_r(lres.enumArray[1].enumNumber)


-- local t = {{'a', 'b'}, 'c', sub_msg1}
-- -- local t = {sub_msg2}
-- local res = _M.serialize_multi(t)
-- print_b(res, 1)
-- local lres = _M.deserialize_multi(res)
-- print_r(lres)

-- local res = _M.serialize_multi(msg)
-- -- print_b(res, 1)
-- local lres = _M.deserialize(res)
-- -- print_r(lres.myMap.sub_msg1.myMap1.key1)
-- -- print_r(lres.myMap.sub_msg1.myFloat32)
-- -- print_r(lres.myMap.sub_msg1.myFloat64)
-- t = {{'a'}, {'b'}, 'c'}
-- print(#t)
-- print_r(utils.is_assoc(t))

-- print_r(utils.lsb_stringtonumber(utils.double_to(4.2)))

-- print_r({string.byte( string.reverse( utils.double_to(4.2) ),1,-1 )})
-- ffi.cdef([[
--     char *itoa(uint64_t value, char *result, int base);
-- ]])

-- local big_int = ffi.new('uint64_t', utils.lsb_stringtonumber(utils.double_to(4.2)))
-- print('--->', big_int)
-- local res = ffi.new('char [100]')
-- print(ffi.string(res))
-- breeze.itoa(big_int, res, 10)
-- print(ffi.string(res))

-- local lres = _M.deserialize(res)
-- print_r(lres.uid)
-- lres.uid = 222
-- print_r(lres.uid)
-- print_r(lres.uinfo.name)
-- lres.uinfo.name = 'oooooo'
-- print_r(lres.uinfo.name)


-- local sub_msg1 = sub_message_t:new({
--     myString = "test_brz_sub_msg1_string",
--     myInt = 2^32,
--     myInt64 = 2^34,
--     myFloat32 = 3.2,
--     myFloat64 = 5.8,
--     myByte = 'k',
--     myBytes = "test_brz_mybytest",
--     myMap1 = {
--         key1 = "bytes1",
--         key2 = "bytes2",
--     },
--     myMap2 = {
--         {1,2,3,4},
--         {5,6,7},
--     },
--     myArray = {8,9,10,11},
--     myBool = false
-- })

-- local sub_msg2 = sub_message_t:new({
--     myString = "test_brz_sub_msg2_string"
-- })

-- local my_enum1 = myenum_t:new(1)
-- local my_enum2 = myenum_t:new(2)
-- local my_enum3 = myenum_t:new(3)

-- local msg = message_t:new({
--     myInt = 99999,
--     myString = "test_brz_msg_string",
--     myMap = {
--         sub_msg1 = sub_msg1,
--         sub_msg2 = sub_msg2
--     },
--     myArray = {sub_msg1, sub_msg2},
--     subMsg = sub_msg1,
--     myEnum = my_enum1,
--     enumArray = {my_enum1, my_enum2, my_enum3}
-- })




-- local sub_msg1 = sub_message_t:new({
--     myString = "s1",
--     myInt = 1,
--     myInt64 = 2,
--     myFloat32 = 3.1,
--     myFloat64 = 4.2,
--     myByte = 'k',
--     myBytes = "ye",
--     myMap1 = {
--         k = "v",
--     },
--     myMap2 = {
--         {1,2},
--     },
--     myArray = {3,4},
--     myBool = false
-- })

-- local sub_msg2 = sub_message_t:new({
--     myString = "s2"
-- })

-- local my_enum1 = myenum_t:new(1)
-- local my_enum2 = myenum_t:new(2)
-- local my_enum3 = myenum_t:new(3)

-- local msg = message_t:new({
--     myInt = 9,
--     myString = "ss",
--     myMap = {
--         s1 = sub_msg1,
--     },
--     myArray = {sub_msg1},
--     subMsg = sub_msg1,
--     myEnum = my_enum1,
--     enumArray = {my_enum1}
-- })



-- local sub_msg1 = sub_message_t:new({
--     myString = "test_brz_sub_msg1_string",
--     -- myInt = 2^32,
--     -- myInt64 = 2^34,
--     myInt = 32,
--     myInt64 = 34,
--     myFloat32 = 3.2,
--     myFloat64 = 5.8,
--     myByte = 'k',
--     myBytes = "test_brz_mybytest",
--     myMap1 = {
--         key1 = "bytes1",
--         -- key2 = "bytes2",
--     },
--     myMap2 = {
--         {1,2,3,4},
--         -- {5,6,7},
--     },
--     myArray = {8,9,10,11},
--     myBool = false
-- })

-- local sub_msg2 = sub_message_t:new({
--     myString = "test_brz_sub_msg2_string"
-- })

-- local my_enum1 = myenum_t:new(1)
-- local my_enum2 = myenum_t:new(2)
-- local my_enum3 = myenum_t:new(3)

-- local msg = message_t:new({
--     myInt = 99999,
--     myString = "test_brz_msg_string",
--     myMap = {
--         sub_msg1 = sub_msg1,
--         -- sub_msg2 = sub_msg2
--     },
--     myArray = {sub_msg1, sub_msg2},
--     subMsg = sub_msg1,
--     myEnum = my_enum1,
--     enumArray = {my_enum1, my_enum2, my_enum3}
-- })

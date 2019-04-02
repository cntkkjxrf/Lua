box.cfg {
    log = 'storage.log',
}

log = require('log')

box.once("bootstrap", function()
    s = box.schema.space.create('storage')
    s:create_index('key',
        {type = 'hash', parts = {1, 'string'}})
end)

local json = require('json')

local post = function(cmd)
    local body = cmd:json()
    local key = body["key"]
    local val = body["value"]

    if key == nil or val == nil or type(key) ~= 'string' then
        log.info("Bad body!")
        return {
            status = 400
        }
    end

    if box.space.storage:count(key) > 0 then
        log.info("The key already exists!")
        return {
            status = 409
        }
    end

    box.space.storage:insert{key, val}
    log.info("POST done")
    return {
        status = 200
    }
end

local put = function(cmd)
    local body = cmd:json()
    local val = body["value"]
    local key = cmd:stash('key')

    if val == nil then
        log.info("Bad Body!")
        return {
            status = 400
        }
    end

    if box.space.storage:count(key) < 1 then
        log.info("The key doesn't exist!")
        return {
            status = 404
        }
    end

    box.space.storage:update(key, {{'=', 2, val}})
    log.info("PUT done")
    return {
        status = 200
    }
end

local get = function(cmd)
    local key = cmd:stash('key')

    if box.space.storage:count(key) < 1 then
        log.info("The key doesn't exist!")
        return {
            status = 404
        }
    end

    log.info("GET ok")
    return {
        body = json.encode(box.space.storage:get(key)[2]),
        status = 200
    }
end

local delete = function(cmd)
    local key = cmd:stash('key')

    if box.space.storage:count(key) > 0 then
        box.space.storage:delete(key)
        log.info("DELETE done")
        return {
            status = 200
        }
    end

    log.info("The key doesn't exist!")
    return {
        status = 404
    }
end

local port = 8080
local http_server = require('http.server').new(nil, port)
local st_path = '/kv'
http_server:route({path = st_path, method = "POST"}, post)
http_server:route({path = st_path..'/:key', method = "PUT"}, put)
http_server:route({path = st_path..'/:key', method = "GET"}, get)
http_server:route({path = st_path..'/:key', method = "DELETE"}, delete)
http_server:start()

-- SOME TESTS
--[[
local http_client = require('http.client')
local url = "http://localhost:8080/kv"

print(http_client.post(url, json.encode({key = "key1", value = "some_str"})).status)
print(http_client.post(url, json.encode({key = "key2", value = "some_string"})).status)
print(http_client.post(url, json.encode({key = "key3", value = "last_string"})).status)
print(http_client.post(url, json.encode({key = "key4"})).status)
print(http_client.post(url, json.encode({})).status)
print(http_client.post(url, json.encode({key = 56, value = "bad_key"})).status)

print(http_client.put(url..'/key2', json.encode({value = "smth_new"})).status)
print(http_client.put(url..'/key4', json.encode({value = "smth_new"})).status)

print(http_client.get(url..'/key3').status)
print(http_client.get(url..'/key3').body)

print(http_client.delete(url..'/key4').status)
print(http_client.delete(url..'/key1').status)
print(http_client.get(url..'/key1').status)

os.exit()
--]]

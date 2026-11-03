-- delete_entry.lua
-- Atomically delete a cache entry and update eviction metadata.
-- Also increments the version counter for the key.
--
-- KEYS:
--   [1]  data key
--   [2]  meta key  (sorted set)
--   [3]  mem key   (memory hash)
--   [4]  ver key   (version hash)
--   [5]  freq key  (LFU frequency hash — may be empty string if not LFU)
--
-- Returns: new version number

local data_key = KEYS[1]
local meta_key = KEYS[2]
local mem_key  = KEYS[3]
local ver_key  = KEYS[4]
local freq_key = KEYS[5]

-- Deduct size
local old_size = tonumber(redis.call('HGET', mem_key, data_key) or '0') or 0
if old_size > 0 then
    redis.call('HINCRBY', mem_key, '__total', -old_size)
    redis.call('HDEL', mem_key, data_key)
end

-- Remove from eviction index
redis.call('ZREM', meta_key, data_key)

-- Remove frequency counter (LFU)
if freq_key ~= '' then
    redis.call('HDEL', freq_key, data_key)
end

-- Delete the actual data key
redis.call('DEL', data_key)

-- Increment and return version
local new_ver = redis.call('HINCRBY', ver_key, data_key, 1)
return new_ver


-- set_entry.lua
-- Atomically write a cache entry and update eviction metadata.
--
-- KEYS:
--   [1]  data key  (namespaced cache key, e.g. "cf:items:42")
--   [2]  meta key  (sorted set for eviction order, e.g. "cf:__meta:lru")
--   [3]  mem key   (string counter for total memory usage, e.g. "cf:__mem")
--   [4]  ver key   (hash of version counters, e.g. "cf:__ver")
--
-- ARGV:
--   [1]  serialized entry value (JSON string)
--   [2]  TTL in milliseconds  (0 = no expiry)
--   [3]  eviction score       (float: Unix timestamp for LRU/FIFO, 0 for LFU)
--   [4]  entry size in bytes  (integer)
--   [5]  expected version     (-1 = skip version check)
--   [6]  frequency increment  (1 for new / update, 0 for version-check-only)
--
-- Returns:
--   1  success
--   0  version mismatch (entry NOT written)

local data_key = KEYS[1]
local meta_key = KEYS[2]
local mem_key  = KEYS[3]
local ver_key  = KEYS[4]

local value      = ARGV[1]
local ttl_ms     = tonumber(ARGV[2])
local score      = tonumber(ARGV[3])
local size_bytes = tonumber(ARGV[4])
local exp_ver    = tonumber(ARGV[5])
local freq_incr  = tonumber(ARGV[6])

-- Version check (stale repopulation guard)
if exp_ver >= 0 then
    local cur_ver = tonumber(redis.call('HGET', ver_key, data_key) or '0') or 0
    if cur_ver ~= exp_ver then
        return 0
    end
end

-- Deduct old size from memory counter (if key exists)
local old_size = tonumber(redis.call('HGET', mem_key, data_key) or '0') or 0
if old_size > 0 then
    redis.call('HINCRBY', mem_key, '__total', -old_size)
    redis.call('HDEL', mem_key, data_key)
end

-- Write the entry
if ttl_ms > 0 then
    redis.call('SET', data_key, value, 'PX', ttl_ms)
else
    redis.call('SET', data_key, value)
end

-- Update eviction metadata
if freq_incr > 0 then
    -- For LFU: increment frequency in a secondary hash
    local freq_key = meta_key .. ':freq'
    local new_freq = redis.call('HINCRBY', freq_key, data_key, freq_incr)
    -- LFU score = (freq << 20) | (unix_ms & 0xFFFFF)  — lower = older/rarer
    local ts = redis.call('TIME')
    local unix_ms = tonumber(ts[1]) * 1000 + math.floor(tonumber(ts[2]) / 1000)
    score = new_freq * 1048576 + (unix_ms % 1048576)
end
redis.call('ZADD', meta_key, score, data_key)

-- Update memory counter
redis.call('HSET', mem_key, data_key, size_bytes)
redis.call('HINCRBY', mem_key, '__total', size_bytes)

return 1


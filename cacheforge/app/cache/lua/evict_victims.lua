-- evict_victims.lua
-- Select and remove the N lowest-scored entries from the eviction sorted set.
-- Called when the cache exceeds its memory or entry limits.
--
-- KEYS:
--   [1]  meta key  (sorted set — lower score = higher eviction priority)
--   [2]  mem key   (memory usage hash)
--   [3]  freq key  (LFU frequency hash — may be empty string)
--
-- ARGV:
--   [1]  n  — number of victims to evict
--
-- Returns: list of evicted data_keys

local meta_key = KEYS[1]
local mem_key  = KEYS[2]
local freq_key = KEYS[3]
local n        = tonumber(ARGV[1])

-- Get N lowest-scored keys
local victims = redis.call('ZRANGE', meta_key, 0, n - 1)
local evicted = {}

for _, data_key in ipairs(victims) do
    -- Deduct memory
    local old_size = tonumber(redis.call('HGET', mem_key, data_key) or '0') or 0
    if old_size > 0 then
        redis.call('HINCRBY', mem_key, '__total', -old_size)
        redis.call('HDEL', mem_key, data_key)
    end
    -- Remove from metadata
    redis.call('ZREM', meta_key, data_key)
    -- Remove LFU frequency
    if freq_key ~= '' then
        redis.call('HDEL', freq_key, data_key)
    end
    -- Delete the data key itself
    redis.call('DEL', data_key)
    table.insert(evicted, data_key)
end

return evicted


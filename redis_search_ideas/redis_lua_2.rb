amenities = %w(park hoa dogs_allowed cats_allowed ski_resort parking_garage).map { |amenity_type| ["amenity:#{amenity_type}", "amenity:not_#{amenity_type}"] }.flatten

and_queries = %w(amenity:park amenity:hoa)
or_queries  = %w(amenity:dogs_allowed amenity:cats_allowed)
max_score   = or_queries.size

or_queries_score = or_queries.map do |or_query|
  "redis.call('sismember', '#{or_query}', id)"
end
or_queries_score = or_queries_score.join(" + ")

and_queries = and_queries.map do |and_query|
  "'#{and_query}'"
end
and_queries = and_queries.join(", ")

k = <<-EOF
local function score(id)
   return #{or_queries_score}
end

local max_score = #{max_score}
local buf = {}
-- cache this stuff or something 'temp_list_' ..
-- Beadsort-ish. Linear O(n) :)

local iterator = max_score
local converter = {}
while (iterator >= 0) do
    converter[iterator] = 'temp_list_' .. iterator
    redis.call('del', converter[iterator])
    iterator = iterator - 1
end

local ids = redis.call('sinter', #{and_queries})
for i, id in ipairs(ids) do
   redis.call('lpush', converter[score(id)], id)
end

iterator = max_score - 1
local base_list_key = converter[max_score]
while (iterator >= 0) do
   local list_length = redis.call('llen', converter[iterator])
   while (list_length > 0) do
      redis.call('RPOPLPUSH', converter[iterator], base_list_key)
      list_length = list_length - 1
   end
   iterator = iterator - 1
end

-- local times = redis.call('llen', base_list_key)
-- print('hello world')
-- while (times > 0) do
--    local id = redis.call('RPOPLPUSH', base_list_key, base_list_key)
--    buf = {}
--    buf[#buf+1] = id
--    buf[#buf+1] = ' '
--    buf[#buf+1] = score(id)
--    print(table.concat(buf))
--    times = times - 1
-- end
EOF
puts (k.lines.each_with_index.map do |n, i|
  "(#{i+1}) #{n}".inspect
end)
puts k.lines.join.inspect

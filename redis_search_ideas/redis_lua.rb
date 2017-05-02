amenities = %w(park hoa dogs_allowed cats_allowed ski_resort parking_garage).map { |amenity_type| ["amenity:#{amenity_type}", "amenity:not_#{amenity_type}"] }.flatten

and_queries = %w(amenity:park amenity:hoa)
or_queries  = %w(amenity:dogs_allowed amenity:cats_allowed)

or_queries_score = or_queries.map do |or_query|
  "redis.call('sismember', '#{or_query}', id)"
end
or_queries_score = or_queries_score.join(" + ")

and_queries = and_queries.map do |and_query|
  "'#{and_query}'"
end
and_queries = and_queries.join(", ")

# Supposedly this could be a simple O(n) sort, if it's done correctly

k = <<-EOF
local function score(id)
   return #{or_queries_score}
end

local ids = redis.call('sinter', #{and_queries})
for i, id in ipairs(ids) do
   redis.call('lpush', 'temp_list', id)
   redis.call('set', 'score_' .. id,  score(id))
end
local n = redis.call('SORT', 'temp_list', 'BY', 'score_*', 'STORE', 'sorted_list')
local times = redis.call('llen', 'temp_list')
local buf = {}

-- while (times > 0) do
--    buf = {}
--    local id = redis.call('RPOPLPUSH', 'sorted_list', 'sorted_list')
--    buf[#buf+1] = id
--    buf[#buf+1] = ' '
--    buf[#buf+1] = redis.call('get', 'score_' .. id)
--    print(table.concat(buf))
--    times = times - 1
-- end
return times
EOF
puts (k.lines.each_with_index.map do |n, i|
  "(#{i+1}) #{n}".inspect
end)
puts k.lines.join.inspect

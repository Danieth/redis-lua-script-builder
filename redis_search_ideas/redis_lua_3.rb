and_queries = %w(amenity:park amenity:hoa)
or_queries  = %w(amenity:dogs_allowed amenity:cats_allowed amenity:ski_resort amenity:parking_garage)
max_score   = or_queries.size

or_queries_score = or_queries.map do |or_query|
  "redis.call('sismember', '#{or_query}', id)"
end
or_queries_score = or_queries_score.join(" + ")

and_queries = and_queries.map do |and_query|
  "'#{and_query}'"
end
and_queries = and_queries.join(", ")

# TODO:
# 1. Namespace the search to a token to prevent the search from being overwritten.
# 2. Turn the interpolated ruby variables into ARGV and KEYS that will be passed into the EVAL. Will most likely incur a minor performance penalty, but, it will allow for us to use EVALSHA on the redis side, which should speed up queries. Depending on which way we go (if we used a complicated scoring query) and our budget, we might be able to get away with dynamically creating the lua code.
# 3. https://springrts.com/wiki/Lua_Performance#TEST_9:_for-loops see if for loops can be written faster - or other things from this website.
# 4. Consider if we should stop after gathering x many of the max_score results (enough to fill the first minilisting page) - could do the rest of the scoring work over time instead of upfront. Space requirements should be relatively the same. Could still stream the existence results over time - while they are passed to the UI we could score them, but it would let the UI show the first page of results immediately. Depending upon the complexity of the or query / scoring algorithm, it might make a lot of sense to do this.
# 5. Document the capabilites of the scoring function. The score could be weighted, or based on a number of interesting variables etc. The only requirement at the moment is that integers are used (but if done correctly, fractional scores are possible as long as we use integer numerators and denominators, aka. it's a fraction. Could also just round to the nearest integer.). Could also put in numerical range queries (aka. need to have more than 500 square feet, but, nice to have more than 800 square feet etc.) into the scoring algorithm. An intelligent scoring algorithm could be what differentiate's us from our competitors.
# 6. Testing on what high level steps in this algorithm could be sped up - aka. testing each section to find the bottlenecks.
# 7. Breaking up the logic into separate sections because redis scripts are atomic (aka. no other redis query can run while this script is running). There are advantages to the script being atomic. If we only use redis for searches, it makes sense to use a purely atomic, one step search query because they will be queued to run one after the other.

# Notes:
# 1. As long as max_score is calculated correctly, the pigeonhole algorithm will work. As long as max_score isn't ridiculously high, this algorithm will remain a lot faster than the traditional sorting algorithms.

lua_code = <<-EOF
-- Init Variables
local r          = redis
local max_score  = #{max_score}

-- Redis Commands
local del        = 'del'
local sinter     = 'sinter'
local lpush      = 'lpush'
local rpoplpush  = 'rpoplpush'
local llen       = 'llen'
local temp_list_ = 'temp_list_'

-- Perform and query - Grab all records that fulfill all of the amenity conditions
local ids = r.call(sinter, #{and_queries})
-- Geo query and range checks for sqrt feet etc. will go here


-- Create converter hash from score to the list_key
local iterator = max_score
local converter = {}

while (iterator >= 0) do
    converter[iterator] = temp_list_ .. iterator
    r.call(del, converter[iterator])
    iterator = iterator - 1
end

-- https://en.wikipedia.org/wiki/Pigeonhole_sort
-- Assuming the max_score is not too large, we can do this linear time in O(max_score + n) (where n is the number of ids remaining after the and query)
-- Scoring is based on or_queries above. 1 Point if the condition is fulfilled. Could use a weighted system in the future.
for i = 1, #ids do
   local id = ids[i]
   r.call(lpush, converter[#{or_queries_score}], id)
end

-- Append the lists with lower scores to the list with the highest score. Works even if the lists are empty etc.
-- TO CONSIDER: less work could be done if we moved the smaller lists into the largest list, but the constant that might incur could cost more. The algorithm would simply be append if lower score than longest list, prepend if higher score - do nothing if longest list, etc.
iterator = max_score - 1
local base_list_key = converter[max_score]
while (iterator >= 0) do
   local list_key = converter[iterator]
   local list_length = r.call(llen, list_key)
   while (list_length > 0) do
      r.call(rpoplpush, list_key, base_list_key)
      list_length = list_length - 1
   end
   iterator = iterator - 1
end
-- Return the length of the final sorted list. Probably going to flip to doing a circular read of list extracting the data from the actual home hashes
return r.call(llen, base_list_key)
EOF

# Minify the lua code slightly.
# TODO: Should consider a true minify algorithm to reduce code sent to redis. Might be worth looking into writing a small gem to handle that work for us - similar to minify plugins for javascript etc.
puts "Debug output:"
lua_code = lua_code.lines.reject do |l|
  l.strip.start_with?('--') || l.strip.empty?
end
longest_column = "#{lua_code.length+1}: ".size
lua_code.each_with_index do |n, i|
  s = "#{i+1}: ".ljust(longest_column)
  puts "#{s}#{n}"
end
puts ""
puts "Script output for redis eval:"
lua_code = lua_code.map { |l|
  l = l.gsub(" =", "=") while l.gsub(" =", "=") != l
  l = l.gsub("= ", "=") while l.gsub("= ", "=") != l
  l = l.gsub(" +", "+") while l.gsub(" +", "+") != l
  l = l.gsub("+ ", "+") while l.gsub("+ ", "+") != l
  l = l.gsub(" -", "-") while l.gsub(" -", "-") != l
  l = l.gsub("- ", "-") while l.gsub("- ", "-") != l
  l = l.gsub(" ,", ",") while l.gsub(" ,", ",") != l
  l = l.gsub(", ", ",") while l.gsub(", ", ",") != l
  "#{l.strip}\n"
}.join
puts lua_code.inspect
# Run in redis-cli like
# eval lua_code 0
# Where lua_code is a copy and paste of the last line returned from this code (including the quotes)

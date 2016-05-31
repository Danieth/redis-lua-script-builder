class RedisLuaScriptBuilder
  attr_accessor :and_query, :and_range_query, :scoring_query, :highest_score, :geo_query, :namespace

  def initialize(namespace)
    @namespace         = namespace
    @sinter_query      = []
    @scoring_query     = []
    @requirement_query = ["RUBY_SUBSTITUTE_LPUSH_SCORE"]
    @geo_query         = []
    @max_score         = 0
  end

  GEO_SPATIAL_ZSET_KEY = :home_locations

  ########## Scoring queries (methods). Allow id's will be sorted by the sum of the conditions they meet based on the queries below.
  # Queries are:
  #  Range (integer range)
  #  Existence (boolean)
  #  Distance (geospatial)

  # Current ranges are monthly_rent && square_feet. Need to add full and half baths.
  # Score the id by one of it's attributes that operate in ranges. If the value is in the range add the weight passed in by the score parameter. If not add 0.
  # Does nothing if score <= 0 or min > max
  def add_range_scoring_query(hash_table, range, score = 1)
    min, max = range.min, range.max
    return if score <= 0 || min > max
    @max_score += score
    @scoring_query << "((#{min} <= #{hash_table} and #{hash_table} <= #{max}) and #{score} or 0)"
  end

  # Score the id by it's existence in other tables, such as amenity:dogs_allowed. If the value exists in the table add the weight passed in by the score parameter. If not add 0.
  # Does nothing if score <= 0
  def add_table_existence_scoring_query(table, score = 1)
    return if score <= 0
    @max_score += score
    @scoring_query << "r.call('sismember', '#{table}', id)"
  end

  # Score the id by it's distance from the latitude and longitude. If it's within the distance add the weight passsed in by the score parameter. If not add 0.
  # Does nothing if score <= 0
  def add_distance_scoring_query(latitude, longitude, distance, score = 1)
    return if score <= 0
    @max_score += score
    geo_member_key = "#{namespace}_scoring_distance"
    @geo_query << [geo_member_key, latitude, longitude]
    @scoring_query << "((tonumber(r.call(geodist, '#{GEO_SPATIAL_ZSET_KEY}', id, '#{geo_member_key}', 'mi')) <= #{distance}) and #{score} or 0)"
  end

  ########## Requirement queries. All id's returned MUST meet all of the requirements from the queries below. Adding more requirement queries speeds up the runtime because it reduces the number of id's that have to be scored/sorted.
  # Queries are:
  #  Range (integer range)
  #  Existence (boolean)
  #  Distance (geospatial)

  # Select all id's (after running the add_table_existence_requirement_query tables) that have integer attributes that are within the range provided by min and max.
  # Does nothing if min > max
  def add_range_requirement_query(hash_table, range)
    min, max = range.min, range.max
    return if min > max
    @requirement_query.unshift("if #{min} <= #{hash_table} and #{hash_table} <= #{max} then")
    @requirement_query.push("end")
  end

  # Select all id's that exist in the intersection of all tables added using this method
  def add_table_existence_requirement_query(table)
    @sinter_query.push("'#{table}'")
  end

  # Select all id's that are within distance (miles) of the provided latitude and longitude
  def add_distance_requirement_query(latitude, longitude, distance)
    geo_member_key = "#{namespace}_requirement_distance"
    @geo_query << [geo_member_key, latitude, longitude]
    @requirement_query.unshift("if (tonumber(r.call(geodist, '#{GEO_SPATIAL_ZSET_KEY}', id, '#{geo_member_key}', 'mi')) <= #{distance}) then")
    @requirement_query.push("end")
  end

  ########## String formatting and lua code
  def to_s
    to_lua_code
  end

  def to_lua_code(debug = false)
    sinter_query      = @sinter_query.join(", ")
    requirement_query = @requirement_query.join("\n")
    max_score         = @max_score
    lua_code = <<-LUA
      -- Local variables
      local r          = redis
      local max_score  = #{max_score}

      -- Redis commands used
      local del        = 'del'
      local sinter     = 'sinter'
      local lpush      = 'lpush'
      local rpoplpush  = 'rpoplpush'
      local llen       = 'llen'
      local hget       = 'hget'
      local geoadd     = 'geoadd'
      local geodist    = 'geodist'
      local zrem       = 'zrem'

      #{converter_lua_code}
      local base_list_key = #{base_list_key_lua_code}
      #{geo_add_lua_code}

      local ids = r.call(sinter, #{sinter_query})
      for i = 1, #ids do
         local id = ids[i]
         -- This sections could definitely use improvement. Look at TODO below. Also should order the && queries to reduce entropy/work as much as possible.
         local square_feet  = tonumber(r.call(hget, 'square_feet',  id))
         local monthly_rent = tonumber(r.call(hget, 'monthly_rent', id))
         local half_baths   = tonumber(r.call(hget, 'half_baths',   id))
         local full_baths   = tonumber(r.call(hget, 'full_baths',   id))
         #{requirement_query}
      end
      #{geo_remove_lua_code}

      #{pigeonhole_merge_lua_code}
      return r.call(llen, base_list_key)
    LUA
    lua_code = substitutions(lua_code)
    lua_code = light_minify(lua_code)
    print_debug_code(lua_code) if (debug)
    minify(lua_code)
  end

  def substitutions(lua_code)
    lua_code.gsub("RUBY_SUBSTITUTE_LPUSH_SCORE", lpush_score_lua_code)
  end

  # Could be named better
  # Removes comments and empty lines
  def light_minify(lua_code)
    lua_code_lines = lua_code.lines.reject do |l|
      l.strip.start_with?('--') || l.strip.empty?
    end
    lua_code_lines.join
  end

  # Compresses spaces around specific tokens and strips the line removing whitespace - add's back an end of line character.
  TOKENS = %w(= + - * / , <= >= < >)
  def minify(lua_code)
    lua_code_lines = lua_code.lines.map do |l|
      TOKENS.each do |token|
        l = l.gsub(" #{token}", "#{token}") while l.gsub(" #{token}", "#{token}") != l
        l = l.gsub("#{token} ", "#{token}") while l.gsub("#{token} ", "#{token}") != l
      end
      "#{l.strip}\n"
    end
    lua_code_lines.join
  end

  # Used to determine if the scoring/sorting code should be loaded.
  def scoring?
    @max_score > 0
  end

  # Used for debugging the lua code based on terminal output of `eval lua_code 0`
  def print_debug_code(lua_code)
    lua_code_lines = lua_code.lines
    longest_column = "#{lua_code_lines.length + 1}: ".size
    lua_code_lines.each_with_index do |n, i|
      s = "#{i+1}: ".ljust(longest_column)
      puts "#{s}#{n}"
    end
  end

  # Lua code for converting the different scores to their respective pigeonhole lists. This reduces the amount of lua string manipulation which dramatically impoves the speed. Also deletes old keys (not needed in production because of namespaces). Only needed when scoring.
  def converter_lua_code
    if scoring?
      <<-LUA
        local temp_list_ = '#{namespace}' .. 'temp_list_'
        local iterator   = max_score
        local converter  = {}
        while (iterator >= 0) do
          converter[iterator] = temp_list_ .. iterator
          r.call(del, converter[iterator])
          iterator = iterator - 1
        end
      LUA
    else
      ""
    end
  end

  # Code for merging the different pigeonhole lists in order so that the final list is sorted in order. Runs in O(max_score + n)
  def pigeonhole_merge_lua_code
    if scoring?
      <<-LUA
        iterator = max_score - 1
        while (iterator >= 0) do
           local list_key = converter[iterator]
           local list_length = r.call(llen, list_key)
           while (list_length > 0) do
              r.call(rpoplpush, list_key, base_list_key)
              list_length = list_length - 1
           end
           iterator = iterator - 1
        end
      LUA
    else
      ""
    end
  end

  # Code for pushing the id into it's pigeonhole list (or single list if not sorting). Has to be substituted at runtime to allow for the requirement_query.
  def lpush_score_lua_code
    scoring? ? "r.call(lpush, converter[#{@scoring_query.join(' + ')}], id)" : "r.call(lpush, '#{@namespace}_list_key', id)"
  end

  # The key of the list that is sorted and meets the requirement queries. If sorting it's the highest scored pigeonhole list. If not sorting it's the default namespaced list key.
  def base_list_key_lua_code
    scoring? ? "converter[max_score]" : "'#{@namespace}_list_key'"
  end

  # Code for adding the lat and long for the scoring and requirement geo queries. Adding it as an scored set temporarily is much faster than manually calculating haversine distance outside of database.
  def geo_add_lua_code
    (@geo_query.map do |geo|
       geo_member_key = geo[0]
       longitude      = geo[1]
       latitude       = geo[2]
       "r.call(geoadd, '#{GEO_SPATIAL_ZSET_KEY}', #{longitude}, #{latitude}, '#{geo_member_key}')"
     end).join("\n")
  end

  # Code for removing the lat and long for the scoring and requirement geo queries - to prevent the redis db from getting cluttered. Run after executing the requirement and scoring queries, so the geo_key is no longer needed.
  def geo_remove_lua_code
    (@geo_query.map do |geo|
       geo_member_key = geo[0]
       "r.call(zrem, '#{GEO_SPATIAL_ZSET_KEY}', '#{geo_member_key}')"
     end).join("\n")
  end
end

def self.test_query()
  # Namespace query to q1_. In Prod these namespaces will be different per search.
  rlsb = RedisLuaScriptBuilder.new("q1_")

  # TODO: Add table does not exist - should be more space efficient (and possibly faster) than sinter store on the amenity_not_* lists.
  # TODO: Improve naming
  # TODO: Turn these query methods into objects and add methods to them to determine if the queries cancel each other out / are invalid. At the same time, improve the local variable usage and creation in the marked TODO region of the core lua_code.

  # These id's must have these features/amenities etc.
  rlsb.add_table_existence_requirement_query(:amenity_cats_allowed)
  rlsb.add_table_existence_requirement_query(:amenity_ski_resort)

  rlsb.add_table_existence_requirement_query(:amenity_not_hoa) # See TODO above

  # In the future, searches could also look like
  # rlsb.add_table_existence_requirement_query(:city_reston)
  # rlsb.add_table_existence_requirement_query(:county_fairfax)
  # rlsb.add_table_existence_requirement_query(:state_va)
  # Etc. Anything binary.


  # The returned id's attributes must be within these ranges.
  rlsb.add_range_requirement_query(:square_feet, 400..1000)
  rlsb.add_range_requirement_query(:monthly_rent, 0..2000)
  rlsb.add_range_requirement_query(:half_baths, 2..5)

  # In the future, searches could also look like
  # rlsb.add_range_requirement_query(:stoves, 1..2) etc.
  # Etc. Anything represented by an integer range


  # Random coordinates in US bounding box
  longitude = rand(-124.848974..-66.885444)
  latitude  = rand(24.396308..49.384358)
  # The returned id's must be within 100 miles of the longitude and latitude provided above
  rlsb.add_distance_requirement_query(longitude, latitude, 100)

  # In the future, we could add polygon and square support etc.


  # The order of the results will be weighted based on if the id exists in the table
  rlsb.add_table_existence_scoring_query(:amenity_parking_garage, 3)
  rlsb.add_table_existence_scoring_query(:amenity_dogs_allowed, 1)
  rlsb.add_table_existence_scoring_query(:amenity_not_smoking_allowed, 2)

  # The order of the results will be weighted based on if the attributes of the id exists within the provided ranges
  rlsb.add_range_scoring_query(:square_feet, 400..100000, 3)
  rlsb.add_range_scoring_query(:monthly_rent, 0..1200, 1)

  # The order of the results will be weighted based on if the location is within the 50 miles of the provided lat and long
  rlsb.add_distance_scoring_query(longitude, latitude, 50, 5)

  # Output the lua_code (and print the debug output)
  puts rlsb.to_lua_code(true)
end
# test_query

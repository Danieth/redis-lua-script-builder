# Be sure to start the redis server using redis-server in terminal
require 'redis'
require 'benchmark'
require './redis_lua_script_builder.rb'

class RedisWriter
  attr_accessor :r
  attr_accessor :time_elapsed

  def initialize()
    @r = Redis.new
    @time_elapsed = Benchmark.measure {}
  end

  # Apartments.com has 750_000 houses. This example uses 850_000
  RANDOM_INTEGER_COUNT = 1_000_000
  MAX_RANDOM_INTEGER   = 1_500_000

  def self.random_integers(n = RANDOM_INTEGER_COUNT, max = MAX_RANDOM_INTEGER)
    n.times.map { rand(max) }
  end

  def reset_db()
    puts "Flushing Database"
    total_time = Benchmark.measure {
      @r.flushall
    }
    @time_elapsed += total_time
    puts "Database flushed (in #{total_time})\n"
  end

  def add_amenity_data()
    amenities = %w(park hoa dogs_allowed cats_allowed ski_resort parking_garage smoking_allowed).map { |amenity_type| ["amenity_#{amenity_type}", "amenity_not_#{amenity_type}"] }.flatten

    puts "Running test with #{amenities.size} keys"

    puts "Adding #{RANDOM_INTEGER_COUNT} integer values to the set on each key (unique per key), with each having a range of (0..#{MAX_RANDOM_INTEGER})"

    total_time = Benchmark.measure() {
      r.pipelined do |redis|
        amenities.each { |amenity| redis.sadd(amenity, RedisWriter.random_integers) }
      end
    }
    @time_elapsed += total_time
    puts "Add amenity data"
    puts "Total:   #{total_time}"
    puts "Average: #{total_time / amenities.size}"
    puts ""
  end

  def add_range_data()
    puts "Adding a random values for all ids (0..#{MAX_RANDOM_INTEGER}) for the ('square_feet', 'monthly_rent', 'half_baths', 'full_baths') ranges"
    total_time = Benchmark.measure() {
      @r.pipelined do |redis|
        (0..MAX_RANDOM_INTEGER).each do |id|
          redis.hset("square_feet",  id, rand(1001) + 100)
          redis.hset("monthly_rent", id, rand(7501) + 500)
          redis.hset("half_baths",   id, rand(6))
          redis.hset("full_baths",   id, rand(4) + 1)
        end
      end
    }
    @time_elapsed += total_time
    puts "Add range data"
    puts "Total:   #{total_time}"
    puts "Average: #{total_time / 4}"
    puts ""
  end

  def add_geospatial_data()
    total_time = Benchmark.measure() {
      @r.pipelined do |redis|
        (0..MAX_RANDOM_INTEGER).each do |id|
          # Bounding box of USA, supposedly. https://www.quora.com/What-is-the-longitude-and-latitude-of-a-bounding-box-around-the-continental-United-States
          longitude = rand(-124.848974..-66.885444)
          latitude  = rand(24.396308..49.384358)
          redis.geoadd('home_locations', longitude, latitude, id)
        end
      end
    }
    @time_elapsed += total_time
    puts "Add geospatial data"
    puts "Total:   #{total_time}"
    puts "Average: #{total_time / MAX_RANDOM_INTEGER}"
    puts ""
  end

  def testing_query()
    query = ""
    total_time = Benchmark.measure() {
      rlsb = RedisLuaScriptBuilder.new("q1_")
      # Has amenities
      rlsb.add_table_existence_requirement_query(:amenity_cats_allowed)
      rlsb.add_table_existence_requirement_query(:amenity_ski_resort)
      rlsb.add_table_existence_requirement_query(:amenity_not_hoa)

      # Has ranges
      rlsb.add_range_requirement_query(:square_feet, 400..1000)
      rlsb.add_range_requirement_query(:monthly_rent, 0..2000)
      rlsb.add_range_requirement_query(:half_baths, 2..5)

      # Within 100 miles of lat long
      longitude = rand(-124.848974..-66.885444)
      latitude  = rand(24.396308..49.384358)
      rlsb.add_distance_requirement_query(longitude, latitude, 100)

      # Increase score if has amenities
      rlsb.add_table_existence_scoring_query(:amenity_parking_garage, 3)
      rlsb.add_table_existence_scoring_query(:amenity_dogs_allowed, 1)
      rlsb.add_table_existence_scoring_query(:amenity_not_smoking_allowed, 2)

      # Increase score if has ranges
      rlsb.add_range_scoring_query(:square_feet, 400..100000, 3)
      rlsb.add_range_scoring_query(:monthly_rent, 0..1200, 1)

      # Increase score if within 50 miles of this lat and long
      rlsb.add_distance_scoring_query(longitude, latitude, 50, 5)
      # Sorts by score, with the highest scored items coming first

      query = rlsb.to_lua_code(false)
    }
    @time_elapsed += total_time
    puts "Created query #{total_time}"
    total_time = Benchmark.measure() {
      r.eval(query)
    }
    @time_elapsed += total_time
    puts "Running query #{total_time}"
  end

  def report_redis_status()
    puts "DB size = #{@r.dbsize}"
    puts "Total Redis Time #{@time_elapsed}"
  end

  def default_setup
    reset_db
    add_amenity_data
    add_range_data
    add_geospatial_data
    testing_query
    report_redis_status
  end
end

r = RedisWriter.new
r.default_setup

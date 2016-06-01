# Be sure to start the redis server using redis-server in terminal
require 'redis'
require 'benchmark'
require 'set'
require 'prime'
include Benchmark

# redis_config = {
#   host: "127.0.0.1",
#   port: 6379
# }
R = Redis.new

# Apartments.com has 750_000 houses.
RANDOM_INTEGER_COUNT = 500_000
MAX_RANDOM_INTEGER   = 750_000

def self.random_integers(n = RANDOM_INTEGER_COUNT, max = MAX_RANDOM_INTEGER)
  n.times.map { rand(max) }
end

def self.reset_db()
  puts "Flushing Database"
  puts Benchmark.measure {
    R.flushall
  }
  puts "Database flushed\n"
end

def self.default_db()
  amenities = %w(park hoa dogs_allowed cats_allowed ski_resort parking_garage).map { |amenity_type| ["amenity:#{amenity_type}", "amenity:not_#{amenity_type}"] }.flatten

  puts "Running test with #{amenities.size} keys"

  puts "Adding #{RANDOM_INTEGER_COUNT} integer values to the set on each key (unique per key), with each having a range of (0..#{MAX_RANDOM_INTEGER})"

  total_time = Benchmark.measure() {
    R.pipelined do |redis|
      amenities.each { |amenity| redis.sadd(amenity, random_integers) }
    end
  }
  puts "Add"
  puts "Total:   #{total_time}"
  puts "Average: #{total_time / amenities.size}"
  puts ""
end

def self.report_redis_status()
  puts "DB size = #{R.dbsize}"
end

reset_db
default_db

def newest_union()
  and_query = %w(amenity:cats_allowed amenity:hoa)
  or_queries  = %w(amenity:dogs_allowed amenity:parking_garage amenity:ski_resort)
  Benchmark.bm(55) do |x|
    x.report("Create set cache") {
      R.sinterstore("scache:1", and_query)
      # Geo stuff would be interstore here. Minimal effect on speed.
    }

    diff_keys = []

    # Caching for faster 'or' lookups
    or_queries = or_queries.sort.map(&:to_sym) # To sym for faster hashes and all.

    # Flip from primes to typical bitset...
    keys = primes = Prime.first(or_queries.size)
    query_key_to_combination_cache_index = {}
    or_queries.each_with_index do |or_query, index|
      query_key_to_combination_cache_index[or_query] = primes[index]
      query_key_to_combination_cache_index[primes[index]] = or_query
    end

    keys.each do |key|
      scache_key = "scache_combination:#{key}"
      R.sinterstore(scache_key, ["scache:1", query_key_to_combination_cache_index[key]])
      R.zinterstore("zcache:1", [scache_key], { weights: [1] })
    end

    used_keys = Set.new

    weight = 0
    previous_keys = keys
    keys.size.times do
      new_keys = []
      weight += 1
      previous_keys.each do |key|
        primes.each do |prime|
          # Easier to do existence and change in primes than bitset, but bitset number must be faster...
          next if (key % prime == 0 || used_keys.include?(new_key = key * prime))

          used_keys.add(new_key)
          new_keys << new_key

          old_key    = "scache_combination:#{key}"
          scache_key = "scache_combination:#{new_key}"

          R.sinterstore(scache_key, [old_key, query_key_to_combination_cache_index[prime]])
          R.zinterstore("zcache:1", [scache_key], { weights: [weight] })
        end
      end
      previous_keys = new_keys
    end

    # x.report("Find and add rank 0 items. (1 sdiffstore, 1 zinterscore)") {
    #   # Find the set where all are ranked 0 - which is all items that have none of the amenities
    #   R.sdiffstore("scache:diff_1", "scache:1", *or_queries)

    #   # Add that set to the zcache with rank 0
    #   R.zinterstore("zcache:1", ["scache:diff_1"], weights: [ 0.0 ])
    # }

    x.report("Extracting results. (1 zrevrange)") {
      puts R.zrevrange("zcache:1", 0, -1, with_scores: true).inspect
    }
  end
end


# Faster. 3 seconds for 500k items
def self.union_through_distinct_combinations()
  and_query = %w(amenity:cats_allowed amenity:hoa)
  or_queries  = %w(amenity:dogs_allowed amenity:parking_garage amenity:ski_resort)
  Benchmark.bm(155) do |x|
    # R.pipelined do |redis|
      x.report("Create set cache") {
        R.sinterstore("scache:1", and_query)
        # Geo stuff would be interstore here. Minimal effect on speed.
      }

      diff_keys = []
      x.report("Create rank 1 sets, for usage in extracting the ranks (1 sinterstore per or query )") {
        or_queries.each do |o|
          key = "scache:#{o}:1"
          R.sinterstore(key, ["scache:1", o])
          diff_keys << key
        end
      }

      x.report("Calculate ranks for different combinations (All combinations, of all sizes, zinterstore queries) ") {
        # This could be greatly improved if we built up the next solution using only the prior one - using
        # rank 0: {}
        # rank 1: {1}, {2}, {3}
        # rank 2: {1, 2}, {1, 3}, {2, 3}

        "zcache:1"

        weights = [1.0]
        (2..(diff_keys.size)).each do |size_of_combination_sets|
          weights << 1.0
          diff_keys.combination(size_of_combination_sets).each do |combination|
            R.zunionstore("zcache:1", combination.push("zcache:1"), { weights: weights })
          end
        end
      }

      x.report("Find and add rank 0 items. (1 sdiffstore, 1 zinterscore)") {
        # Find the set where all are ranked 0 - which is all items that have none of the amenities
        R.sdiffstore("scache:diff_1", "scache:1", *diff_keys)

        # Add that set to the zcache with rank 0
        R.zunionstore("zcache:1", ["scache:diff_1", "zcache:1"], weights: [0.0])
      }

      x.report("Extracting results. (1 zrevrange)") {
        R.zrevrange("zcache:1", 0, -1, with_scores: true).inspect
      }
    # end
  end
  puts R.zrevrange("zcache:1", 0, -1, with_scores: true).inspect
end
# union_through_distinct_combinations()

# Fairly slow. Iteration 1
def self.union_through_zincrby()
  Benchmark.bm(35) do |x|
    x.report("create scache:1") {
      R.sinterstore("scache:1", and_query)
    }
    x.report("create zcache:1") {
      R.zinterstore("zcache:1", and_query, { weights: [0] })
    }

    x.report("increment zscores for correct sort\n") {
      or_query.each do |o|
        x.report("-> #{o}") {
          R.sinter("scache:1", o).each do |e|
            R.zincrby("zcache:1", 1.0, e)
          end
        }
      end
    }
    x.report("zcache retrieval") {
      R.zrevrange("zcache:1", 0, -1, with_scores: true).inspect
    }
  end
end

# Extremely slow. Iteration 1
def self.simple_union_join()
  puts Benchmark.measure {
    R.zinterstore("&cache:1", and_query)


    puts Benchmark.measure("q1") {
      weights = [0, [1] * or_query.size].flatten
      R.zunionstore("|cache:1", ["&cache:1", *or_query],   { weights: weights })
    }
    puts Benchmark.measure("q2") {
      weights = [1, [1] * or_query_2.size].flatten
      R.zunionstore("|cache:2", ["&cache:1", *or_query_2], { weights: weights })
    }
  }

  require 'set'
  s = Set.new
  i = 0
  R.zscan_each("|cache:1") { |e|
    s.add(e[1])
  }
  puts s.inspect
  puts ""

  s = Set.new
  R.zscan_each("|cache:2") { |e|
    s.add(e[1])
  }
  puts s.inspect
  puts ""
end

# Be sure to start the redis server using redis-server in terminal
require 'redis'
require 'benchmark'
include Benchmark

R = Redis.new

RANDOM_INTEGER_COUNT = 750_000    # Assumption that average amenity will be on 200_000 properties.
MAX_RANDOM_INTEGER   = 7_500_000  # Based on Apartments.com having 750_000 houses.

def self.random_integers(n = RANDOM_INTEGER_COUNT, max = MAX_RANDOM_INTEGER)
  n.times.map { rand(max) }
end

def self.reset_db()
  puts "Flushing Database"
  R.flushall
  puts "Database flushed\n"
end

def self.report_redis_status()
  puts "DB size = #{R.dbsize}"
end

def self.test()

end

reset_db

# Not should really be the inverse of the other key, but I wrote it like this for faster testing. Just needed more amenities for testing.
# two_parking_slots extra_fees hub_zone recommended
amenities = %w(park hoa dogs_allowed cats_allowed ski_resort parking_garage).map { |amenity_type| ["amenity:#{amenity_type}", "amenity:not_#{amenity_type}"] }.flatten

puts "Running test with #{amenities.size} keys"

puts "Building random integer groups for add"
random_integer_groups = amenities.size.times.map { random_integers } # So it doesn't effect speed of add test

puts "Testing add (Adding #{RANDOM_INTEGER_COUNT} integer values to the set on each key (unique per key), with each having a range of (0..#{MAX_RANDOM_INTEGER}))"

times = amenities.each_with_index.map do |amenity, i|
  Benchmark.measure(amenity) { R.sadd(amenity, random_integer_groups[i]) }
end
total_time = times.reduce(&:+)
puts "Total Time:   #{total_time}"
puts "Average Time: #{total_time / amenities.size}"


combination_size = 2
puts "Building combinations of #{combination_size}"
combinations = amenities.combination(combination_size).to_a
number_of_combinations = combinations.size
tenth_of_number_of_combinations = number_of_combinations / 10

puts "Testing SINTER on combinations of #{combination_size} (#{number_of_combinations} unique sets of #{combination_size}). AKA Testing && queries on #{combination_size} amenities)"
times = combinations.each_with_index.map do |combination, i|
  puts "#{(((i-1).fdiv(number_of_combinations)*100).round(2))}% complete" if (i-1) > 0 && (i-1) % tenth_of_number_of_combinations == 0
  Benchmark.measure(combination.join(",")) { R.sinter(*combination) }
end
total_time = times.reduce(&:+)
puts "Total Time:   #{total_time}"
puts "Average Time: #{total_time / number_of_combinations}"

# combination_size = 3
# puts "Building combinations of #{combination_size}"
# combinations = amenities.combination(combination_size).to_a
# number_of_combinations = combinations.size
# tenth_of_number_of_combinations = number_of_combinations / 10
# puts "Testing SUNION on combinations of #{combination_size} (#{number_of_combinations} unique sets of #{combination_size}). AKA Testing || queries on #{combination_size} amenities)"
# times = combinations.each_with_index.map do |combination, i|
#   puts "#{(((i-1).fdiv(number_of_combinations)*100).round(2))}% complete" if (i-1) > 0 && (i-1) % tenth_of_number_of_combinations == 0
#   Benchmark.measure(combination.join(",")) { R.sunion(*combination) }
# end
# total_time = times.reduce(&:+)
# puts "Total Time:   #{total_time}"
# puts "Average Time: #{total_time / number_of_combinations}"

report_redis_status()

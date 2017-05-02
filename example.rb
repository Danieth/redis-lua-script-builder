require 'pry'
require './lib/redis_lua_script_builder'
require './lib/redis_writer'

redis_writer = RedisWriter.new
redis_writer.default_setup
binding.pry

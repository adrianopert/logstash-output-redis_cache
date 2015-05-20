require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/redis_cache"
require "logstash/json"
require "redis"

describe LogStash::Outputs::Redis_Cache, :redis_cache => true do
  

  describe "cache selected key-values from event in redis cache" do
    key = 10.times.collect { rand(10).to_s }.join("")
    event_count = 1

    config <<-CONFIG
      input {
        generator {
          message => 'clave=#{key} usuario=adriano host=loco pirulo=filtrado'
          count => #{event_count}
          type => "generator"
        }
      }
			filter {
				kv {
				}
			}
      output {
        redis_cache {
          host => "127.0.0.1"
          key => "clave"
          fields => ["usuario", "host", "f1", "otro"]
        }
      }
    CONFIG

    agent do
      # Query redis directly and inspect the goodness.
      redis = Redis.new(:host => "127.0.0.1", :db => 1)

      # The list should contain the number of elements our agent pushed up.
      insist { redis.dbsize } == event_count

      # Now check all events for order and correctness.
      event_count.times do |value|
				element = redis.get(key)
        cached = LogStash::Event.new(LogStash::Json.load(element))
        insist { cached["host"] } == "loco"
        insist { cached["usuario"] } == "adriano"
				redis.del( key )
      end

      # The list should now be empty
      insist { redis.dbsize } == 0
    end # agent
  end
end

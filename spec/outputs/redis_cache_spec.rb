require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/redis_cache"
require "logstash/json"
require "redis"

describe LogStash::Outputs::Redis_Cache, :redis_cache => true do
  

  describe "cache selected key-values from event in redis cache" do
#    key = 10.times.collect { rand(10).to_s }.join("")
		keys = ["192.168.1.1", "172.1.0.123"]
#    event_count = 10000 + rand(500)
    event_count = 1

    config <<-CONFIG
      input {
        generator {
          message => 'ip_SRC=192.168.1.1 usuario=adriano host=loco pirulo=filtrado'
          count => #{event_count}
          type => "generator"
        }
      }
			filter {
				kv {
					add_field => { "ip" => "%{ip_SRC}" }
				}
			}
      output {
        redis_cache {
          host => "127.0.0.1"
          key => "ip_SRC"
          fields => ["usuario", "host", "f1", "otro", "ip"]
        }
      }
    CONFIG

    agent do
      # Query redis directly and inspect the goodness.
      redis = Redis.new(:host => "127.0.0.1", :db => 1)

      # The list should contain the number of elements our agent pushed up.
      insist { redis.dbsize } == [ event_count, keys.length ].min

      # Now check all events for order and correctness.
      event_count.times do |value|
        ip = keys[value]
				element = redis.get(ip)
        cached = LogStash::Event.new(LogStash::Json.load(element))
        insist { cached["ip"] } == ip
        insist { cached["usuario"] } == "adriano"
				redis.del( ip )
      end

      # The list should now be empty
      insist { redis.dbsize } == 0
    end # agent
  end

#  describe "batch mode" do
#    key = 10.times.collect { rand(10).to_s }.join("")
#    event_count = 200000
#
#    config <<-CONFIG
#      input {
#        generator {
#         message => "hello world"
#          count => #{event_count}
#          type => "generator"
#        }
#      }
#      output {
#        redis {
#          host => "127.0.0.1"
#          key => "#{key}"
#          data_type => list
#          batch => true
#          batch_timeout => 5
#          timeout => 5
#        }
#      }
#    CONFIG
#
#    agent do
#      # we have to wait for teardown to execute & flush the last batch.
#      # otherwise we might start doing assertions before everything has been
#      # sent out to redis.
#      sleep 2
#
#      redis = Redis.new(:host => "127.0.0.1")
#
#      # The list should contain the number of elements our agent pushed up.
#      insist { redis.llen(key) } == event_count
#
#      # Now check all events for order and correctness.
#      event_count.times do |value|
#        id, element = redis.blpop(key, 0)
#        event = LogStash::Event.new(LogStash::Json.load(element))
#        insist { event["sequence"] } == value
#        insist { event["message"] } == "hello world"
#      end
#
#      # The list should now be empty
#      insist { redis.llen(key) } == 0
#    end # agent
#  end
#
#  describe "converts US-ASCII to utf-8 without failures" do
#    key = 10.times.collect { rand(10).to_s }.join("")
#
#    config <<-CONFIG
#      input {
#        generator {
#          charset => "US-ASCII"
#          message => "\xAD\u0000"
#          count => 1
#          type => "generator"
#        }
#      }
#      output {
#        redis {
#          host => "127.0.0.1"
#          key => "#{key}"
#          data_type => list
#        }
#      }
#    CONFIG
#
#    agent do
#      # Query redis directly and inspect the goodness.
#      redis = Redis.new(:host => "127.0.0.1")
#
#      # The list should contain no elements.
#      insist { redis.llen(key) } == 1
#    end # agent
#  end
end


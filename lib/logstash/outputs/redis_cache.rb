# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"

# This output will cache some selected key-value pairs
# to Redis using set command.
# 
class LogStash::Outputs::Redis_Cache < LogStash::Outputs::Base

  config_name "redis_cache"

  # Name is used for logging in case there are multiple instances.
  # TODO: delete
  config :name, :validate => :string, :default => 'default',
    :deprecated => true

  # The hostname(s) of your Redis server(s). Ports may be specified on any
  # hostname, which will override the global port config.
  #
  # For example:
  # [source,ruby]
  #     "127.0.0.1"
  config :host, :validate => :string, :default => "127.0.0.1"

  # The default port to connect on. Can be overridden on any hostname.
  config :port, :validate => :number, :default => 6379

  # The Redis database number. Defaults to 1 to differenciate from redis plugin which writes to "0 database" by default.
  config :db, :validate => :number, :default => 1

  # Redis initial connection timeout in seconds.
  config :timeout, :validate => :number, :default => 5

  # Password to authenticate with.  There is no authentication by default.
  config :password, :validate => :password

  # The name of an existing field in event that will become in the Redis key.
  config :key, :validate => :string, :required => true

	# List of field events that want to cache
	config :fields, :validate => :array, :required => true

  # Interval for reconnecting to failed Redis connections
  config :reconnect_interval, :validate => :number, :default => 1

  def register
    require 'redis'
    @redis = nil
  end # def register

  def receive(event)
    return unless output?(event)

    key = event[@key]
    # TODO(sissel): We really should not drop an event, but historically
    # we have dropped events that fail to be converted to json.
    # TODO(sissel): Find a way to continue passing events through even
    # if they fail to convert properly.
    begin
			# Get only the keys listed in @fields (first order keys). Prepare the event with some filters to assure the existence of the required fields.
      payload = LogStash::Json.dump(event.to_hash.select { |k, v| @fields.include?(k) } )
    rescue Encoding::UndefinedConversionError, ArgumentError
      puts "FAILUREENCODING"
      @logger.error("Failed to convert event to JSON. Invalid UTF-8, maybe?",
                    :event => event.inspect)
      return
    end

    begin
      @redis ||= connect
			@redis.set(key, payload)
				
    rescue => e
      @logger.warn("Failed to send event to Redis", :event => event,
                   :identity => identity, :exception => e,
                   :backtrace => e.backtrace)
      sleep @reconnect_interval
      @redis = nil
      retry
    end
  end # def receive

  private
  def connect
    params = {
      :host => @host,
      :port => @port,
      :timeout => @timeout,
      :db => @db
    }
    @logger.debug(params)

    if @password
      params[:password] = @password.value
    end

    Redis.new(params)
  end # def connect

  # A string used to identify a Redis instance in log messages
  def identity
    @name || "redis://#{@password}@#{@current_host}:#{@current_port}/#{@db} #{@data_type}:#{@key}"
  end

end

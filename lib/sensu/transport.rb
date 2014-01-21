module Sensu
  class Transport
    include Utilities
    @@subclasses = {}

    def self.create(base)
      Dir.glob(File.join(File.dirname(__FILE__), 'transports/*.rb')).each do |file|
          require file
      end
      @logger = base.logger
      @settings = base.settings

      type = @settings[:transport] || 'rabbitmq'
      @logger.info("Creating #{type} transport")
      c = @@subclasses[type]
      if c
        c.new(base)
      else
        raise "Bad transport type #{type}"
      end
    end

    def self.register_transport name
       @@subclasses[name] = self
    end

    def redis=(new_redis)
      @redis = new_redis
    end
  end
end

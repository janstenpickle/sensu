module Sensu
  class Transport
    @@subclasses = {}

    def self.require_directory(directory)
      path = directory.gsub(/\\(?=\S)/, '/')
      Dir.glob(File.join(path, '**/*.rb')).each do |file|
        begin
          require File.expand_path(file)
        rescue ScriptError => error
          @logger.error('failed to require transport', {
            :transport_file => file,
            :error => error
          })
          @logger.warn('ignoring transport', {
            :transport_file => file
          })
        end
      end
    end

    def self.load_all
      require_directory(File.join(File.dirname(__FILE__), 'transports'))
    end

    def self.create(base, parent)
      load_all

      @settings = base.settings
      @logger = base.logger

      type = @settings[:transport] || 'rabbitmq'
      @logger.info("Creating #{type} transport")
      c = @@subclasses[type]
      if c
        c.new(base, parent)
      else
        raise "Bad transport type #{type}"
      end
    end

    def initialize(base, parent)
      @settings = base.settings
      @logger = base.logger
      @parent = parent
    end

    def redis=(new_redis)
      @redis = new_redis
    end

    def self.register_transport name
       @@subclasses[name] = self
    end
  end
end

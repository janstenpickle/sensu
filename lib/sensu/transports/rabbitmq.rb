module Sensu
  module Transports
    class RabbitMQ < Sensu::Transport

      def initialize(base)
        @logger = base.logger
        @settings = base.settings
      end

      def setup(redis, &block)
        self.redis = redis

        @logger.debug('connecting to rabbitmq', {
          :settings => @settings[:rabbitmq]
        })
        @rabbitmq = Sensu::RabbitMQ.connect(@settings[:rabbitmq])
        @rabbitmq.on_error do |error|
          @logger.fatal('rabbitmq connection error', {
            :error => error.to_s
          })
          instance_exec('stop', &block) if block
        end
        @rabbitmq.before_reconnect do
          unless testing?
            @logger.warn('reconnecting to rabbitmq')
            instance_exec('pause', &block) if block
          end
        end
        @rabbitmq.after_reconnect do
          @logger.info('reconnected to rabbitmq')
          instance_exec('resume', &block) if block
        end
        @amq = @rabbitmq.channel
      end

      def setup_keepalives(&block)
        @logger.debug('subscribing to keepalives')
        @keepalive_queue = @amq.queue!('keepalives', :auto_delete => true)
        @keepalive_queue.bind(@amq.direct('keepalives')) do
          block.call if block
        end
        @keepalive_queue.subscribe(:ack => true) do |header, payload|
          client = Oj.load(payload)
          @logger.debug('received keepalive', {
            :client => client
          })
          @redis.set('client:' + client[:name], Oj.dump(client)) do
            @redis.sadd('clients', client[:name]) do
              header.ack
            end
          end
        end
      end


      def unsubscribe
        @logger.warn('unsubscribing from keepalive and result queues')
        if @rabbitmq.connected?
          @keepalive_queue.unsubscribe
          @result_queue.unsubscribe
          @amq.recover
        else
          @keepalive_queue.before_recovery do
            @keepalive_queue.unsubscribe
          end
          @result_queue.before_recovery do
            @result_queue.unsubscribe
          end
        end
      end

      register_transport 'rabbitmq'
    end
  end
end

module Sensu
  module Transports
    class RabbitMQ < Transport
      include Utilities

      def setup()
        @logger.debug('connecting to rabbitmq', {
          :settings => @settings[:rabbitmq]
        })
        @rabbitmq = Sensu::RabbitMQ.connect(@settings[:rabbitmq])
        @rabbitmq.on_error do |error|
          @logger.fatal('rabbitmq connection error', {
            :error => error.to_s
          })
          @parent.stop
        end
        @rabbitmq.before_reconnect do
          unless testing?
            @logger.warn('reconnecting to rabbitmq')
            @parent.pause if @parent.method_defined?(:pause)
          end
        end
        @rabbitmq.after_reconnect do
          @logger.info('reconnected to rabbitmq')
          @parent.resume if @parent.method_defined?(:resume)
        end
        @amq = @rabbitmq.channel
        @subscribed_queues = {}
      end

      def bind(queue, &block)
        @subscribed_queues[queue] = @amq.queue!(queue, :auto_delete => true)
        @subscribed_queues[queue].bind(@amq.direct(queue)) do
          block.call(@subscribed_queues[queue]) unless block.nil?
        end
      end

      def subscribe(queue, &block)
        @subscribed_queues[queue].subscribe(:ack => true) do |header, payload|
          block.call(header, payload)
        end
      end

      def unsubscribe
        @logger.warn('unsubscribing from queues')
        if @rabbitmq.connected?
          @subscribed_queues.each do | _, queue |
            queue.unsubscribe
          end
          @amq.recover
        else
          @subscribed_queues.each do | _, queue |
            queue.before_recovery do
              queue.unsubscribe
            end
          end
        end
      end

      def publish(queue, payload)
        begin
          @amq.direct(queue).publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error("failed to publish to #{queue} queue", {
            :payload => payload,
            :error => error.to_s
          })
        end
      end

      def handle(handler, event_data)
        exchange_name = handler[:exchange][:name]
        exchange_type = handler[:exchange].has_key?(:type) ? handler[:exchange][:type].to_sym : :direct
        exchange_options = handler[:exchange].reject do |key, value|
          [:name, :type].include?(key)
        end
        unless event_data.empty?
          begin
            @amq.method(exchange_type).call(exchange_name, exchange_options).publish(event_data)
          rescue AMQ::Client::ConnectionClosedError => error
            @logger.error('failed to publish event data to an exchange', {
              :exchange => handler[:exchange],
              :payload => event_data,
              :error => error.to_s
            })
           end
         end
      end

      def setup_subscriptions(*subscriptions, &block)
        @logger.debug('subscribing to client subscriptions')
        @subscribed_queues['check_request'] = @amq.queue('', :auto_delete => true) do |queue|
          subscriptions.each do |exchange_name|
            @logger.debug('binding queue to exchange', {
              :queue_name => queue.name,
              :exchange_name => exchange_name
            })
            queue.bind(@amq.fanout(exchange_name))
          end
          queue.subscribe do |payload|
            block.call(payload)
          end
        end
      end

      def info(&block)
        info = {
          :keepalives => {
            :messages => nil,
            :consumers => nil
          },
          :results => {
            :messages => nil,
            :consumers => nil
          },
          :connected => @rabbitmq.connected?
        }
        if @rabbitmq.connected?
          @amq.queue('keepalives', :auto_delete => true).status do |messages, consumers|
            info[:keepalives][:messages] = messages
            info[:keepalives][:consumers] = consumers
            @amq.queue('results', :auto_delete => true).status do |messages, consumers|
              info[:results][:messages] = messages
              info[:results][:consumers] = consumers
              block.call(info)
            end
          end
        else
          block.call(info)
        end
      end

      def publish_multi(*exchanges, payload)
        exchanges.each do | exchange_name |
          begin
            @amq.fanout(exchange_name).publish(Oj.dump(payload))
          rescue AMQ::Client::ConnectionClosedError => error
            @logger.error('failed to publish check request', {
              :exchange_name => exchange_name,
              :payload => payload,
              :error => error.to_s
            })
          end
        end
      end

      def connected?
        @rabbitmq.connected?
      end

      def close
        @rabbitmq.close
      end

      register_transport 'rabbitmq'
    end
  end
end

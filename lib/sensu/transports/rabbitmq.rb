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

      def setup_results(&block)
        @logger.debug('subscribing to results')
        @result_queue = @amq.queue!('results', :auto_delete => true)
        @result_queue.bind(@amq.direct('results')) do
          block.call if block
        end
        @result_queue.subscribe(:ack => true) do |header, payload|
          result = Oj.load(payload)
          @logger.debug('received result', {
            :result => result
          })
          @parent.process_result(result)
          EM::next_tick do
            header.ack
          end
        end
      end

      def publish_result(payload)
        begin
          @amq.direct('results').publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error('failed to publish check result', {
            :payload => payload,
            :error => error.to_s
          })
        end
      end

      def server_unsubscribe
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

      def publish_keepalive(payload)
        begin
         @amq.direct('keepalives').publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error('failed to publish keepalive', {
            :payload => payload,
            :error => error.to_s
          })
        end
      end

      def handle(handler, event_data)
        exchange_name = handler[:exchange][:name]
        exchange_type = handler[:exchange].has_key?(:type) ?
handler[:exchange][:type].to_sym : :direct
        exchange_options = handler[:exchange].reject do |key, value|
          [:name, :type].include?(key)
        end
        unless event_data.empty?
          begin
            @amq.method(exchange_type).call(exchange_name,
exchange_options).publish(event_data)
          rescue AMQ::Client::ConnectionClosedError => error
            @logger.error('failed to publish event data to an exchange', {
              :exchange => handler[:exchange],
              :payload => event_data,
              :error => error.to_s
            })
           end
         end
      end

      def publish_result(payload)
        begin
          @amq.direct('results').publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error('failed to publish check result', {
            :payload => payload,
            :error => error.to_s
          })
        end
      end

      def setup_subscriptions
        @logger.debug('subscribing to client subscriptions')
        @check_request_queue = @amq.queue('', :auto_delete => true) do |queue|
          @settings[:client][:subscriptions].each do |exchange_name|
            @logger.debug('binding queue to exchange', {
              :queue_name => queue.name,
              :exchange_name => exchange_name
            })
            queue.bind(@amq.fanout(exchange_name))
          end
          queue.subscribe do |payload|
            check = Oj.load(payload)
            @logger.info('received check request', {
              :check => check
            })
            @parent.process_check(check) unless @parent.nil?
          end
        end
      end

      def client_unsubscribe
        @logger.warn('unsubscribing from client subscriptions')
         if @rabbitmq.connected?
          @check_request_queue.unsubscribe
         else
          @check_request_queue.before_recovery do
            @check_request_queue.unsubscribe
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
          @amq.queue('keepalives', :auto_delete => true).status do |messages,
consumers|
            info[:keepalives][:messages] = messages
            info[:keepalives][:consumers] = consumers
            @amq.queue('results', :auto_delete => true).status do |messages,
consumers|
              info[:results][:messages] = messages
              info[:results][:consumers] = consumers
              block.call(info)
            end
          end
        else
          block.call(info)
        end
      end

      def resolve_event(payload)
        begin
          @amq.direct('results').publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error('failed to publish check result', {
            :payload => payload,
            :error => error.to_s
          })
        end
      end 

      def publish_check_request(*exchanges, payload)
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

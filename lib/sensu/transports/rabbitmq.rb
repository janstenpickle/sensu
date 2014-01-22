module Sensu
  module Transport
    class RabbitMQ < Sensu::Transport::Base

      def setup(redis)
        self.redis = redis

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
            @parent.pause
          end
        end
        @rabbitmq.after_reconnect do
          @logger.info('reconnected to rabbitmq')
          @parent.resume
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
          parent.process_result(result)
          EM::next_tick do
            header.ack
          end
        end
      end

      def publish_check_request(check)
        payload = {
          :name => check[:name],
          :issued => Time.now.to_i
        }
        if check.has_key?(:command)
          payload[:command] = check[:command]
        end
        @logger.info('publishing check request', {
          :payload => payload,
          :subscribers => check[:subscribers]
        })
        check[:subscribers].each do |exchange_name|
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

      def publish_result(client, check)
        payload = {
          :client => client[:name],
          :check => check
        }
        @logger.debug('publishing check result', {
          :payload => payload
        })
        begin
          @amq.direct('results').publish(Oj.dump(payload))
        rescue AMQ::Client::ConnectionClosedError => error
          @logger.error('failed to publish check result', {
            :payload => payload,
            :error => error.to_s
          })
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

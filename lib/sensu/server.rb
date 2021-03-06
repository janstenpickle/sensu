require File.join(File.dirname(__FILE__), 'base')
require File.join(File.dirname(__FILE__), 'redis')
require File.join(File.dirname(__FILE__), 'socket')
require File.join(File.dirname(__FILE__), 'sandbox')

module Sensu
  class Server
    include Utilities

    attr_reader :is_master

    def self.run(options={})
      server = self.new(options)
      EM::run do
        server.start
        server.trap_signals
      end
    end

    def initialize(options={})
      base = Base.new(options)
      @logger = base.logger
      @settings = base.settings
      @extensions = base.extensions
      base.setup_process
      @extensions.load_settings(@settings.to_hash)
      @timers = Array.new
      @master_timers = Array.new
      @handlers_in_progress_count = 0
      @is_master = false
    end

    def setup_redis
      @logger.debug('connecting to redis', {
        :settings => @settings[:redis]
      })
      @redis = Redis.connect(@settings[:redis])
      @redis.on_error do |error|
        @logger.fatal('redis connection error', {
          :error => error.to_s
        })
        stop
      end
      @redis.before_reconnect do
        unless testing?
          @logger.warn('reconnecting to redis')
          pause
        end
      end
      @redis.after_reconnect do
        @logger.info('reconnected to redis')
        resume
      end
    end

    def setup_rabbitmq
      @logger.debug('connecting to rabbitmq', {
        :settings => @settings[:rabbitmq]
      })
      @rabbitmq = RabbitMQ.connect(@settings[:rabbitmq])
      @rabbitmq.on_error do |error|
        @logger.fatal('rabbitmq connection error', {
          :error => error.to_s
        })
        stop
      end
      @rabbitmq.before_reconnect do
        unless testing?
          @logger.warn('reconnecting to rabbitmq')
          pause
        end
      end
      @rabbitmq.after_reconnect do
        @logger.info('reconnected to rabbitmq')
        resume
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

    def action_subdued?(condition)
      subdued = false
      if condition.has_key?(:begin) && condition.has_key?(:end)
        begin_time = Time.parse(condition[:begin])
        end_time = Time.parse(condition[:end])
        if end_time < begin_time
          if Time.now < end_time
            begin_time = Time.parse('12:00:00 AM')
          else
            end_time = Time.parse('11:59:59 PM')
          end
        end
        if Time.now >= begin_time && Time.now <= end_time
          subdued = true
        end
      end
      if condition.has_key?(:days)
        days = condition[:days].map(&:downcase)
        if days.include?(Time.now.strftime('%A').downcase)
          subdued = true
        end
      end
      if subdued && condition.has_key?(:exceptions)
        subdued = condition[:exceptions].none? do |exception|
          Time.now >= Time.parse(exception[:begin]) && Time.now <= Time.parse(exception[:end])
        end
      end
      subdued
    end

    def handler_subdued?(handler, check)
      subdued = Array.new
      if handler[:subdue]
        subdued << action_subdued?(handler[:subdue])
      end
      if check[:subdue] && check[:subdue][:at] != 'publisher'
        subdued << action_subdued?(check[:subdue])
      end
      subdued.any?
    end

    def filter_attributes_match?(hash_one, hash_two)
      hash_one.keys.all? do |key|
        case
        when hash_one[key] == hash_two[key]
          true
        when hash_one[key].is_a?(Hash) && hash_two[key].is_a?(Hash)
          filter_attributes_match?(hash_one[key], hash_two[key])
        when hash_one[key].is_a?(String) && hash_one[key].start_with?('eval:')
          begin
            expression = hash_one[key].gsub(/^eval:(\s+)?/, '')
            !!Sandbox.eval(expression, hash_two[key])
          rescue => error
            @logger.error('filter eval error', {
              :attributes => [hash_one, hash_two],
              :error => error.to_s
            })
            false
          end
        else
          false
        end
      end
    end

    def event_filtered?(filter_name, event)
      if @settings.filter_exists?(filter_name)
        filter = @settings[:filters][filter_name]
        matched = filter_attributes_match?(filter[:attributes], event)
        filter[:negate] ? matched : !matched
      else
        @logger.error('unknown filter', {
          :filter_name => filter_name
        })
        false
      end
    end

    def derive_handlers(handler_list)
      handler_list.inject(Array.new) do |handlers, handler_name|
        if @settings.handler_exists?(handler_name)
          handler = @settings[:handlers][handler_name].merge(:name => handler_name)
          if handler[:type] == 'set'
            handlers = handlers + derive_handlers(handler[:handlers])
          else
            handlers << handler
          end
        elsif @extensions.handler_exists?(handler_name)
          handlers << @extensions[:handlers][handler_name]
        else
          @logger.error('unknown handler', {
            :handler_name => handler_name
          })
        end
        handlers.uniq
      end
    end

    def event_handlers(event)
      handler_list = Array((event[:check][:handlers] || event[:check][:handler]) || 'default')
      handlers = derive_handlers(handler_list)
      handlers.select do |handler|
        if event[:action] == :flapping && !handler[:handle_flapping]
          @logger.info('handler does not handle flapping events', {
            :event => event,
            :handler => handler
          })
          next
        end
        if handler_subdued?(handler, event[:check])
          @logger.info('handler is subdued', {
            :event => event,
            :handler => handler
          })
          next
        end
        if handler.has_key?(:severities)
          handle = case event[:action]
          when :resolve
            event[:check][:history].reverse[1..-1].any? do |status|
              if status.to_i == 0
                break
              end
              severity = SEVERITIES[status.to_i] || 'unknown'
              handler[:severities].include?(severity)
            end
          else
            severity = SEVERITIES[event[:check][:status]] || 'unknown'
            handler[:severities].include?(severity)
          end
          unless handle
            @logger.debug('handler does not handle event severity', {
              :event => event,
              :handler => handler
            })
            next
          end
        end
        if handler.has_key?(:filters) || handler.has_key?(:filter)
          filter_list = Array(handler[:filters] || handler[:filter])
          filtered = filter_list.any? do |filter_name|
            event_filtered?(filter_name, event)
          end
          if filtered
            @logger.info('event filtered for handler', {
              :event => event,
              :handler => handler
            })
            next
          end
        end
        true
      end
    end

    def mutate_event_data(mutator_name, event, &block)
      case
      when mutator_name.nil?
        block.call(Oj.dump(event))
      when @settings.mutator_exists?(mutator_name)
        mutator = @settings[:mutators][mutator_name]
        IO.async_popen(mutator[:command], Oj.dump(event), mutator[:timeout]) do |output, status|
          if status == 0
            block.call(output)
          else
            @logger.error('mutator error', {
              :event => event,
              :mutator => mutator,
              :output => output,
              :status => status
            })
            @handlers_in_progress_count -= 1
          end
        end
      when @extensions.mutator_exists?(mutator_name)
        extension = @extensions[:mutators][mutator_name]
        extension.safe_run(event) do |output, status|
          if status == 0
            block.call(output)
          else
            @logger.error('mutator extension error', {
              :event => event,
              :extension => extension.definition,
              :output => output,
              :status => status
            })
            @handlers_in_progress_count -= 1
          end
        end
      else
        @logger.error('unknown mutator', {
          :mutator_name => mutator_name
        })
        @handlers_in_progress_count -= 1
      end
    end

    def handle_event(event)
      handlers = event_handlers(event)
      handlers.each do |handler|
        log_level = event[:check][:type] == 'metric' ? :debug : :info
        @logger.send(log_level, 'handling event', {
          :event => event,
          :handler => handler.respond_to?(:definition) ? handler.definition : handler
        })
        @handlers_in_progress_count += 1
        on_error = Proc.new do |error|
          @logger.error('handler error', {
            :event => event,
            :handler => handler,
            :error => error.to_s
          })
          @handlers_in_progress_count -= 1
        end
        mutate_event_data(handler[:mutator], event) do |event_data|
          case handler[:type]
          when 'pipe'
            IO.async_popen(handler[:command], event_data, handler[:timeout]) do |output, status|
              output.each_line do |line|
                @logger.info('handler output', {
                  :handler => handler,
                  :output => line
                })
              end
              @handlers_in_progress_count -= 1
            end
          when 'tcp'
            begin
              EM::connect(handler[:socket][:host], handler[:socket][:port], SocketHandler) do |socket|
                socket.on_success = Proc.new do
                  @handlers_in_progress_count -= 1
                end
                socket.on_error = on_error
                timeout = handler[:timeout] || 10
                socket.pending_connect_timeout = timeout
                socket.comm_inactivity_timeout = timeout
                socket.send_data(event_data.to_s)
                socket.close_connection_after_writing
              end
            rescue => error
              on_error.call(error)
            end
          when 'udp'
            begin
              EM::open_datagram_socket('0.0.0.0', 0, nil) do |socket|
                socket.send_datagram(event_data.to_s, handler[:socket][:host], handler[:socket][:port])
                socket.close_connection_after_writing
                @handlers_in_progress_count -= 1
              end
            rescue => error
              on_error.call(error)
            end
          when 'amqp'
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
            @handlers_in_progress_count -= 1
          when 'extension'
            handler.safe_run(event_data) do |output, status|
              output.each_line do |line|
                @logger.info('handler extension output', {
                  :extension => handler.definition,
                  :output => line
                })
              end
              @handlers_in_progress_count -= 1
            end
          end
        end
      end
    end

    def aggregate_result(result)
      @logger.debug('adding result to aggregate', {
        :result => result
      })
      check = result[:check]
      result_set = check[:name] + ':' + check[:issued].to_s
      @redis.hset('aggregation:' + result_set, result[:client], Oj.dump(
        :output => check[:output],
        :status => check[:status]
      )) do
        SEVERITIES.each do |severity|
          @redis.hsetnx('aggregate:' + result_set, severity, 0)
        end
        severity = (SEVERITIES[check[:status]] || 'unknown')
        @redis.hincrby('aggregate:' + result_set, severity, 1) do
          @redis.hincrby('aggregate:' + result_set, 'total', 1) do
            @redis.sadd('aggregates:' + check[:name], check[:issued]) do
              @redis.sadd('aggregates', check[:name])
            end
          end
        end
      end
    end

    def process_result(result)
      @logger.debug('processing result', {
        :result => result
      })
      @redis.get('client:' + result[:client]) do |client_json|
        unless client_json.nil?
          client = Oj.load(client_json)
          check = case
          when @settings.check_exists?(result[:check][:name])
            @settings[:checks][result[:check][:name]].merge(result[:check])
          else
            result[:check]
          end
          if check[:aggregate]
            aggregate_result(result)
          end
          @redis.sadd('history:' + client[:name], check[:name])
          history_key = 'history:' + client[:name] + ':' + check[:name]
          @redis.rpush(history_key, check[:status]) do
            execution_key = 'execution:' + client[:name] + ':' + check[:name]
            @redis.set(execution_key, check[:executed])
            @redis.lrange(history_key, -21, -1) do |history|
              check[:history] = history
              total_state_change = 0
              unless history.size < 21
                state_changes = 0
                change_weight = 0.8
                previous_status = history.first
                history.each do |status|
                  unless status == previous_status
                    state_changes += change_weight
                  end
                  change_weight += 0.02
                  previous_status = status
                end
                total_state_change = (state_changes.fdiv(20) * 100).to_i
                @redis.ltrim(history_key, -21, -1)
              end
              @redis.hget('events:' + client[:name], check[:name]) do |event_json|
                previous_occurrence = event_json ? Oj.load(event_json) : false
                is_flapping = false
                if check.has_key?(:low_flap_threshold) && check.has_key?(:high_flap_threshold)
                  was_flapping = previous_occurrence ? previous_occurrence[:flapping] : false
                  is_flapping = case
                  when total_state_change >= check[:high_flap_threshold]
                    true
                  when was_flapping && total_state_change <= check[:low_flap_threshold]
                    false
                  else
                    was_flapping
                  end
                end
                event = {
                  :client => client,
                  :check => check,
                  :occurrences => 1
                }
                if check[:status] != 0 || is_flapping
                  if previous_occurrence && check[:status] == previous_occurrence[:status]
                    event[:occurrences] = previous_occurrence[:occurrences] + 1
                  end
                  @redis.hset('events:' + client[:name], check[:name], Oj.dump(
                    :output => check[:output],
                    :status => check[:status],
                    :issued => check[:issued],
                    :handlers => Array((check[:handlers] || check[:handler]) || 'default'),
                    :flapping => is_flapping,
                    :occurrences => event[:occurrences]
                  )) do
                    unless check[:handle] == false
                      event[:action] = is_flapping ? :flapping : :create
                      handle_event(event)
                    end
                  end
                elsif previous_occurrence
                  unless check[:auto_resolve] == false && !check[:force_resolve]
                    @redis.hdel('events:' + client[:name], check[:name]) do
                      unless check[:handle] == false
                        event[:occurrences] = previous_occurrence[:occurrences]
                        event[:action] = :resolve
                        handle_event(event)
                      end
                    end
                  end
                elsif check[:type] == 'metric'
                  handle_event(event)
                end
              end
            end
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
        process_result(result)
        EM::next_tick do
          header.ack
        end
      end
    end

    def check_request_subdued?(check)
      if check[:subdue] && check[:subdue][:at] == 'publisher'
        action_subdued?(check[:subdue])
      else
        false
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

    def schedule_checks(checks)
      check_count = 0
      stagger = testing? ? 0 : 2
      checks.each do |check|
        check_count += 1
        scheduling_delay = stagger * check_count % 30
        @master_timers << EM::Timer.new(scheduling_delay) do
          interval = testing? ? 0.5 : check[:interval]
          @master_timers << EM::PeriodicTimer.new(interval) do
            unless check_request_subdued?(check)
              publish_check_request(check)
            else
              @logger.info('check request was subdued', {
                :check => check
              })
            end
          end
        end
      end
    end

    def setup_publisher
      @logger.debug('scheduling check requests')
      standard_checks = @settings.checks.reject do |check|
        check[:standalone] || check[:publish] == false
      end
      extension_checks = @extensions.checks.reject do |check|
        check[:standalone] || check[:publish] == false || !check[:interval].is_a?(Integer)
      end
      schedule_checks(standard_checks + extension_checks)
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

    def determine_stale_clients
      @logger.info('determining stale clients')
      @redis.smembers('clients') do |clients|
        clients.each do |client_name|
          @redis.get('client:' + client_name) do |client_json|
            unless client_json.nil?
              client = Oj.load(client_json)
              check = {
                :thresholds => {
                  :warning => 120,
                  :critical => 180
                }
              }
              if client.has_key?(:keepalive)
                check = deep_merge(check, client[:keepalive])
              end
              check[:name] = 'keepalive'
              check[:issued] = Time.now.to_i
              check[:executed] = Time.now.to_i
              time_since_last_keepalive = Time.now.to_i - client[:timestamp]
              case
              when time_since_last_keepalive >= check[:thresholds][:critical]
                check[:output] = 'No keep-alive sent from client in over '
                check[:output] << check[:thresholds][:critical].to_s + ' seconds'
                check[:status] = 2
              when time_since_last_keepalive >= check[:thresholds][:warning]
                check[:output] = 'No keep-alive sent from client in over '
                check[:output] << check[:thresholds][:warning].to_s + ' seconds'
                check[:status] = 1
              else
                check[:output] = 'Keep-alive sent from client less than '
                check[:output] << check[:thresholds][:warning].to_s + ' seconds ago'
                check[:status] = 0
              end
              publish_result(client, check)
            end
          end
        end
      end
    end

    def setup_client_monitor
      @logger.debug('monitoring clients')
      @master_timers << EM::PeriodicTimer.new(30) do
        determine_stale_clients
      end
    end

    def prune_aggregations
      @logger.info('pruning aggregations')
      @redis.smembers('aggregates') do |checks|
        checks.each do |check_name|
          @redis.smembers('aggregates:' + check_name) do |aggregates|
            if aggregates.size > 20
              aggregates.sort!
              aggregates.take(aggregates.size - 20).each do |check_issued|
                @redis.srem('aggregates:' + check_name, check_issued) do
                  result_set = check_name + ':' + check_issued.to_s
                  @redis.del('aggregate:' + result_set) do
                    @redis.del('aggregation:' + result_set) do
                      @logger.debug('pruned aggregation', {
                        :check => {
                          :name => check_name,
                          :issued => check_issued
                        }
                      })
                    end
                  end
                end
              end
            end
          end
        end
      end
    end

    def setup_aggregation_pruner
      @logger.debug('pruning aggregations')
      @master_timers << EM::PeriodicTimer.new(20) do
        prune_aggregations
      end
    end

    def master_duties
      setup_publisher
      setup_client_monitor
      setup_aggregation_pruner
    end

    def request_master_election
      @redis.setnx('lock:master', Time.now.to_i) do |created|
        if created
          @is_master = true
          @logger.info('i am the master')
          master_duties
        else
          @redis.get('lock:master') do |timestamp|
            if Time.now.to_i - timestamp.to_i >= 60
              @redis.getset('lock:master', Time.now.to_i) do |previous|
                if previous == timestamp
                  @is_master = true
                  @logger.info('i am now the master')
                  master_duties
                end
              end
            end
          end
        end
      end
    end

    def setup_master_monitor
      request_master_election
      @timers << EM::PeriodicTimer.new(20) do
        if @is_master
          @redis.set('lock:master', Time.now.to_i) do
            @logger.debug('updated master lock timestamp')
          end
        else
          request_master_election
        end
      end
    end

    def resign_as_master(&block)
      block ||= Proc.new {}
      if @is_master
        @logger.warn('resigning as master')
        @master_timers.each do |timer|
          timer.cancel
        end
        @master_timers.clear
        if @redis.connected?
          @redis.del('lock:master') do
            @logger.info('removed master lock')
            @is_master = false
          end
        end
        timestamp = Time.now.to_i
        retry_until_true do
          if !@is_master
            block.call
            true
          elsif Time.now.to_i - timestamp >= 3
            @logger.warn('failed to remove master lock')
            @is_master = false
            block.call
            true
          end
        end
      else
        @logger.debug('not currently master')
        block.call
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

    def complete_handlers_in_progress(&block)
      @logger.info('completing handlers in progress', {
        :handlers_in_progress_count => @handlers_in_progress_count
      })
      retry_until_true do
        if @handlers_in_progress_count == 0
          block.call
          true
        end
      end
    end

    def bootstrap
      setup_keepalives
      setup_results
      setup_master_monitor
      @state = :running
    end

    def start
      setup_redis
      setup_rabbitmq
      bootstrap
    end

    def pause(&block)
      unless @state == :pausing || @state == :paused
        @state = :pausing
        @timers.each do |timer|
          timer.cancel
        end
        @timers.clear
        unsubscribe
        resign_as_master do
          @state = :paused
          if block
            block.call
          end
        end
      end
    end

    def resume
      retry_until_true(1) do
        if @state == :paused
          if @redis.connected? && @rabbitmq.connected?
            bootstrap
            true
          end
        end
      end
    end

    def stop
      @logger.warn('stopping')
      @state = :stopping
      pause do
        complete_handlers_in_progress do
          @extensions.stop_all do
            @redis.close
            @rabbitmq.close
            @logger.warn('stopping reactor')
            EM::stop_event_loop
          end
        end
      end
    end

    def trap_signals
      @signals = Array.new
      STOP_SIGNALS.each do |signal|
        Signal.trap(signal) do
          @signals << signal
        end
      end
      EM::PeriodicTimer.new(1) do
        signal = @signals.shift
        if STOP_SIGNALS.include?(signal)
          @logger.warn('received signal', {
            :signal => signal
          })
          stop
        end
      end
    end
  end
end

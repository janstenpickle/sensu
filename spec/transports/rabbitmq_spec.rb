require File.dirname(__FILE__) + '/../helpers.rb'
require File.dirname(__FILE__) + '/../../lib/sensu/base.rb'
require File.dirname(__FILE__) + '/../../lib/sensu/transport.rb'
require File.dirname(__FILE__) + '/../../lib/sensu/redis.rb'

describe 'Sensu::Transports::Rabbitmq' do
  include Helpers

  before do
    base = Sensu::Base.new(options)
    @settings = base.settings
    @transport = Sensu::Transport.create(base, nil)
  end

  it 'can connect to transport' do
    async_wrapper do
      @transport.setup
      async_done
    end
  end

  it 'can publish a message' do
    async_wrapper do
      @transport.setup
      @transport.bind('sensu_test') do |queue|
        check = result_template[:check]
        payload = {
          :client => @settings[:client][:name],
          :check => check
        }
        @transport.publish('sensu_test', payload)
        queue.subscribe do |payload|
          result = Oj.load(payload)
          result[:client].should eq('i-424242')
          result[:check][:name].should eq('foobar')
          async_done
        end
      end
    end
  end

  it 'can subscribe to a queue' do
    async_wrapper do
      @transport.setup
      @transport.bind('sensu_test')
      check = result_template[:check]
      payload = {
        :client => @settings[:client][:name],
        :check => check
      }
      @transport.publish('sensu_test', payload)
      @transport.subscribe('sensu_test') do |header, payload|
        result = Oj.load(payload)
        result[:client].should eq('i-424242')
        result[:check][:name].should eq('foobar')
        async_done
      end
    end
  end

  it 'can publish to multiple exchanages' do
    async_wrapper do
      @transport.setup
      amq.fanout('sensu_multi_test') do
        payload = {
          :name => 'foobar',
          :issued => Time.now.to_i
        }
        queue = amq.queue('', :auto_delete => true).bind('sensu_multi_test') do
          @transport.publish_multi('sensu_multi_test', payload)
        end
        queue.subscribe do |payload|
          check_request = Oj.load(payload)
          check_request[:name].should eq('foobar')
          check_request[:issued].should be_within(10).of(epoch)
          async_done
        end
      end
    end
  end

  it 'can produce info' do
    async_wrapper do
      @transport.setup
      timer(1) do
        @transport.info do | info |
          info[:keepalives][:messages].should eq(0)
          info[:keepalives][:consumers].should eq(0)
          info[:results][:messages].should eq(0)
          info[:results][:consumers].should eq(0)
          async_done
        end
      end
    end
  end

  it 'can handle an event' do
    async_wrapper do
      @transport.setup
      @transport.bind('sensu_test') do | queue |
        handler = {
          :exchange => {
            :name => 'sensu_test'
          }
        }
        @transport.handle(handler, 'test')
        queue.subscribe do | payload |
          payload.should eq('test')
          async_done
        end
      end
    end
  end

  it 'can subscribe to multiple exchanges' do
    async_wrapper do
      @transport.setup
      queue = amq.queue('', :auto_delete => true).bind('sensu_sub_test') do
      amq.fanout('sensu_sub_test').publish('test')
      end
      @transport.setup_subscriptions('sensu_sub_test') do | payload |
        payload.should eq('test')
        async_done
      end
    end
  end

  it 'can ascertain if connected' do
    async_wrapper do
      @transport.setup
      timer(1) do
        @transport.connected?.should eq(true)
        async_done
      end
    end
  end

  it 'can close a connection' do
    async_wrapper do
      @transport.setup
      timer(1) do
        @transport.close
        async_done
      end
    end
  end
end

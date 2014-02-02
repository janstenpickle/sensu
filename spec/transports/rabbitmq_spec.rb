require File.dirname(__FILE__) + '/rabbitmq_helpers.rb'
require File.dirname(__FILE__) + '/mock_parent.rb'
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
      @transport.bind('results') do |queue|
        check = result_template[:check]
        payload = {
          :client => @settings[:client][:name],
          :check => check
        }
        @transport.publish('results', payload)
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
      @transport.bind('results')
      check = result_template[:check]
      payload = {
        :client => @settings[:client][:name],
        :check => check
      }
      @transport.publish('results', payload)
      @transport.subscribe('results') do |header, payload|
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
      amq.fanout('test') do
        payload = {
          :name => 'foobar',
          :issued => Time.now.to_i
        }
        queue = amq.queue('', :auto_delete => true).bind('test') do
          @transport.publish_multi('test', payload)
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

end

require File.dirname(__FILE__) + '/rabbitmq_helpers.rb'
require File.dirname(__FILE__) + '/../../lib/sensu/base.rb'
require File.dirname(__FILE__) + '/../../lib/sensu/transport.rb'

describe 'Sensu::Transports::Rabbitmq' do
  include RabbitmqHelpers

  before do
    base = Sensu::Base.new(options)
    @transport = Sensu::Transport.create(base, nil)
    @settings = base.settings
  end

  it 'can connect to transport' do
    async_wrapper do
      @transport.setup
      async_done
    end
  end

  it 'can send a keepalive' do
    async_wrapper do
      keepalive_queue do |queue|
        @transport.setup
        @transport.publish_keepalive({:name => @settings[:client][:name]})
        queue.subscribe do |payload|
          keepalive = Oj.load(payload)
          keepalive[:name].should eq('i-424242')
          async_done
        end
      end
    end
  end

  it 'can send a check result' do
    async_wrapper do
      result_queue do |queue|
        @transport.setup
        check = result_template[:check]
        payload = {
          :client => @settings[:client][:name],
          :check => check
        }
        @transport.publish_result(payload)
        queue.subscribe do |payload|
          result = Oj.load(payload)
          result[:client].should eq('i-424242')
          result[:check][:name].should eq('foobar')
          async_done
        end
      end
    end
  end
end

require File.dirname(__FILE__) + '/../helpers.rb'

module RabbitmqHelpers
  include Helpers

  def setup_amq
    rabbitmq = AMQP.connect
    @amq = AMQP::Channel.new(rabbitmq)
    @amq
  end

  def amq
    @amq ? @amq : setup_amq
  end

  def keepalive_queue(&block)
    amq.queue('keepalives', :auto_delete => true) do |queue|
      queue.bind(amq.direct('keepalives')) do
        block.call(queue)
      end
    end
  end

  def result_queue(&block)
    amq.queue('results', :auto_delete => true) do |queue|
      queue.bind(amq.direct('results')) do
        block.call(queue)
      end
    end
  end

end

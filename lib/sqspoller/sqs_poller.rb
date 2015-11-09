require "logger"
require "concurrent"
require "net/http"
require "sqspoller/worker_task"
require "sqspoller/message_delegator"
require "sqspoller/queue_controller"

module Sqspoller

  class SqsPoller
    class << self

      def initialize_poller(filename)
        config = YAML.load(ERB.new(IO.read(filename)).result).with_indifferent_access

        fork do
          @logger = Logger.new(STDOUT)

          total_poller_threads = 0
          qcs = []
          config[:queues].keys.each { |queue|
            total_poller_threads += config[:queues][queue][:polling_threads]
          }
          message_delegator = initialize_worker config[:worker_configuration], total_poller_threads
          config[:queues].keys.each { |queue|
            @logger.info "Creating QueueController object for queue: #{queue}"
            qc = QueueController.new queue, config[:queues][queue][:polling_threads], message_delegator
            qcs << qc
          }

          qcs.each { |qc|
            qc.start
          }

          qcs.each{ |qc| qc.threads.each { |thread| thread.join } }
        end
      end

      def initialize_worker(worker_configuration, total_poller_threads)
        worker_thread_count = worker_configuration[:concurrency]
        worker_task = worker_configuration[:worker_class].constantize.new worker_configuration
        waiting_tasks_ratio = worker_configuration[:waiting_tasks_ratio]
        waiting_tasks_ratio = 1 if waiting_tasks_ratio.blank?
        if worker_thread_count.present?
          message_delegator = MessageDelegator.new worker_thread_count, waiting_tasks_ratio, worker_task
        else
          message_delegator = MessageDelegator.new total_poller_threads, waiting_tasks_ratio, worker_task
        end
        return message_delegator
      end
    end
  end

end

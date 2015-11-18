require "logger"
require "concurrent"
require "net/http"
require "yaml"
require "erb"
require "sqspoller/worker_task"
require "sqspoller/message_delegator"
require "sqspoller/queue_controller"

module Sqspoller

  class SqsPoller
    class << self


      def sym(map)
        if map.class == Hash
          map = map.inject({}){|memo,(k,v)| memo[k.to_sym] = sym(v); memo}
        end
        return map
      end

      def start_poller(filename, queue_config_name, access_key_id, secret_access_key, region)
        config = YAML.load(ERB.new(IO.read(filename)).result)
        config = sym(config)

        fork do
          @logger = Logger.new(STDOUT)

          total_poller_threads = 0
          qcs = []
          queues_config = config[queue_config_name] || config[queue_config_name.to_sym]
          queues_config.keys.each { |queue|
            total_poller_threads += queues_config[queue][:polling_threads]
          }
          message_delegator = initialize_worker config[:worker_configuration], total_poller_threads
          queues_config.keys.each { |queue|
            @logger.info "Creating QueueController object for queue: #{queue}"
            qc = QueueController.new queue, queues_config[queue][:polling_threads], message_delegator, access_key_id, secret_access_key, region
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
        worker_task = worker_configuration[:worker_class].split('::').inject(Object) {|o,c| o.const_get c}.new(worker_configuration)
        waiting_tasks_ratio = worker_configuration[:waiting_tasks_ratio]
        waiting_tasks_ratio = 1 if waiting_tasks_ratio.nil?
        if worker_thread_count.nil?
          message_delegator = MessageDelegator.new total_poller_threads, waiting_tasks_ratio, worker_task
        else
          message_delegator = MessageDelegator.new worker_thread_count, waiting_tasks_ratio, worker_task
        end
        return message_delegator
      end
    end
  end

end

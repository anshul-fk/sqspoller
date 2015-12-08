require "logger"
require "concurrent"
require "net/http"

module Sqspoller
  class MessageDelegator

    def initialize(worker_thread_pool_size, waiting_tasks_ratio, worker_task, logger_file)
      @logger = Logger.new(logger_file)
      @worker_thread_pool_size = worker_thread_pool_size
      @max_allowed_queue_size = waiting_tasks_ratio * worker_thread_pool_size
      @semaphore = Mutex.new
      @worker_task = worker_task
      @pending_schedule_tasks = 0
      initialize_connection_pool
    end

    def initialize_connection_pool
      @connection_pool = Concurrent::RubyThreadPoolExecutor.new(max_threads: @worker_thread_pool_size, min_threads: 1, max_queue: @max_allowed_queue_size)
    end

    def process(queue_controller, message, queue_name)
      @semaphore.synchronize {
        @pending_schedule_tasks +=1
        if @connection_pool.queue_length == @max_allowed_queue_size
          while @connection_pool.queue_length > @worker_thread_pool_size || @connection_pool.queue_length + @pending_schedule_tasks >= @max_allowed_queue_size
            sleep(0.01)
          end
        end
      }
      begin
        @logger.info "Scheduling worker task for message: #{message.message_id}"

        @connection_pool.post do
          begin
            @logger.info "Starting worker task for message: #{message.message_id}"
            @worker_task.process(message.body, message.message_id)
            @pending_schedule_tasks -= 1
            @logger.info "Finished worker task for message: #{message.message_id}"
            queue_controller.delete_message message.receipt_handle
          rescue Exception => e
            @pending_schedule_tasks -= 1
            @logger.info "Caught error for message: #{message}, error: #{e.message}, #{e.backtrace.join("\n")}"
          end
        end
      rescue Concurrent::RejectedExecutionError => e
        @logger.info  "Caught Concurrent::RejectedExecutionError for #{e.message}"
      end
    end
  end
end

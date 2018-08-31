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
        if @pending_schedule_tasks >= @max_allowed_queue_size
          @logger.info "Entered wait state, connection_pool size reached max threshold, pending_schedule_tasks=#{@pending_schedule_tasks}"
          while @connection_pool.queue_length > @worker_thread_pool_size || @pending_schedule_tasks >= @max_allowed_queue_size
            sleep(0.01)
          end
          @logger.info "Exiting wait state, connection_pool size reached below worker_thread_pool_size, pending_schedule_tasks=#{@pending_schedule_tasks}"
        end
      }
      @logger.info "Scheduling worker task for message: #{message.message_id}"

      begin
        @connection_pool.post do
          begin
            @logger.info "Starting worker task for message: #{message.message_id}"
            @worker_task.process(message.body, message.message_id)
            @logger.info "Finished worker task for message: #{message.message_id}"
            queue_controller.delete_message message.receipt_handle
          rescue Exception => e
            @logger.info "Caught error: #{e.message}, #{e.backtrace.join("\n")} for message id: #{message.message_id}, body: #{message.body}"
          end
          @pending_schedule_tasks -= 1
        end
      rescue Concurrent::RejectedExecutionError => e
        @pending_schedule_tasks -= 1
        @logger.info  "Caught Concurrent::RejectedExecutionError for #{e.message}"
      end
    end
  end
end

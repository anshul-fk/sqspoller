require "logger"
require "concurrent"
require "net/http"
require "aws-sdk"

module Sqspoller
  class QueueController

    def initialize(queue_name, polling_threads_count, task_delegator, access_key_id, secret_access_key, region, logger_file)
      @logger = Logger.new(logger_file)
      @queue_name = queue_name
      @polling_threads_count = polling_threads_count
      @sqs = Aws::SQS::Client.new(:access_key_id => access_key_id, :secret_access_key => secret_access_key, :region => region)
      @queue_details = @sqs.get_queue_url(queue_name: queue_name)
      @threads = []
      @task_delegator = task_delegator
    end

    def start
      queue_url = @queue_details.queue_url
      @logger.info "Going to start polling threads for queue: #{queue_url}"
      @polling_threads_count.times do
        start_thread queue_url
      end
    end

    def threads
      return @threads
    end

    def start_thread(queue_url)
      @threads << Thread.new do
        @logger.info "Poller thread started for queue: #{queue_url}"
        poller = Aws::SQS::QueuePoller.new(queue_url)

        while true
          msgs = @sqs.receive_message :queue_url => queue_url
          msgs.messages.each { |received_message|
            begin
              @logger.info "Received message #{@queue_name} : #{received_message.message_id}"
              @task_delegator.process self, received_message, @queue_name
            rescue Exception => e
              @logger.info "Encountered error #{e.message} while submitting message from queue #{queue_url}"
            end
          }
        end
      end
    end

    def delete_message(receipt_handle)
      @sqs.delete_message(
        queue_url: @queue_details.queue_url,
        receipt_handle: receipt_handle
      )
    end

  end
end

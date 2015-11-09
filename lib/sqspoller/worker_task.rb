require "logger"
require "concurrent"
require "net/http"

module Sqspoller


  class WorkerTask

    def initialize(worker_configuration)
      @logger = Logger.new(STDOUT)
      @http_method = worker_configuration[:http_method]
      @http_url = worker_configuration[:http_url]
      @uri = URI(@http_url)
    end

    def process(message, message_id)
      if @http_method.downcase == "post"
        response = Net::HTTP.post_form(@uri, JSON.parse(message))
      elsif @http_method.downcase == "get"
        uri = URI(@http_url)
        uri.query = URI.encode_www_form(JSON.parse(message))
        response = Net::HTTP.get_response(uri)
      else
        raise "Invalid http_method provided. #{http_method}"
      end
      @logger.info "Got HTTP response as #{response.code}, #{response.body}"
    end
  end

end

require "logger"
require "concurrent"
require "net/http"
require "rest-client"
require "json"

module Sqspoller


  class WorkerTask

    HEADERS = {
      'Content-Type' => 'application/json',
      'Accept' => 'application/json'
    }

    def initialize(worker_configuration, logger_file)
      @logger = Logger.new(logger_file)
      @http_method = worker_configuration[:http_method]
      @http_url = worker_configuration[:http_url]
      @timeout = worker_configuration[:timeout] ? worker_configuration[:timeout].to_i : 450
      @uri = URI(@http_url)
    end

    def process(message, message_id)
      parsed_message = JSON.parse(message)

      if @http_method.downcase == "post"
        RestClient::Request.execute(:method => :post, :url => @http_url, :payload => parsed_message.to_json, :headers => HEADERS,  :timeout => @timeout, :open_timeout => 5) do |response, request, result|
          process_http_response response
        end
      elsif @http_method.downcase == "get"
        RestClient::Request.execute(:method => :get, :url => @http_url, :payload => parsed_message.to_json, :headers => HEADERS,  :timeout => @timeout, :open_timeout => 5) do |response, request, result|
          process_http_response response
        end
      else
        raise "Invalid http_method provided. #{http_method}"
      end
    end

    def process_http_response(response)
      case response.code
      when 200
        return "OK"
      else
        raise "Service did not return 200 OK response. #{response.code}"
      end
    end
  end

end

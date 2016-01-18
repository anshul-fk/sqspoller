require "delegate"

module Seahorse
  class StringIO < SimpleDelegator
    def initialize(data = '')
      @io = ::StringIO.new(data)
      super(@io)
    end
  end
end

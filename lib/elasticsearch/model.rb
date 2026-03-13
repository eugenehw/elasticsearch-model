require_relative 'model/configuration'
require_relative 'model/client'
require_relative 'model/query'
require_relative 'model/clause_context'
require_relative 'model/filter_collector'
require_relative 'model/query_filter'
require_relative 'model/query_context'
require_relative 'model/bool_builder'
require_relative 'model/agg_builder'
require_relative 'model/response'
require_relative 'model/criteria'
require_relative 'model/searchable'

module Elasticsearch
  module Model
    extend Configuration

    class << self
      # Shared HTTP client (lazily built, reset on configure).
      def client
        @client ||= Client.new(config)
      end
    end
  end
end

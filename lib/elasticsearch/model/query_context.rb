# frozen_string_literal: true

module Elasticsearch
  module Model
    # Evaluated inside Criteria#query { } blocks.
    # Inherits all query-building methods from ClauseContext and adds:
    #   - filter { } — communicates back to the Criteria via @_criteria_ref
    #   - QueryFilter helpers (smart_match, date_range, filter_terms)
    #   - Model-specific QueryFilter helpers (MyModel::QueryFilter)
    class QueryContext < ClauseContext
      # filter_buf — an Array passed by reference from build_query_hash.
      # Non-scoring clauses (filter {}, date_range, filter_terms) are pushed here
      # so they never accumulate on the Criteria between recompilations.
      def initialize(filter_buf, model_qf_mod = nil)
        super()
        @_filter_buf    = filter_buf
        @_filter_module = model_qf_mod
        extend(QueryFilter)
        extend(model_qf_mod) if model_qf_mod
      end

      # filter {} inside a query {} block — pushes clauses into the temp buffer.
      def filter(&block)
        collector = FilterCollector.new(@_filter_module)
        collector.instance_exec(&block)
        @_filter_buf.concat(collector.clauses)
      end
    end
  end
end

# frozen_string_literal: true

module Elasticsearch
  module Model
    # Evaluated inside Criteria#query { } blocks.
    # Inherits all query-building methods from ClauseContext and adds:
    #   - filter/must/should/must_not { } — shorthand for bool { filter/... { } }
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

      # Shorthand: filter/must/should/must_not {} inside query {} wraps in bool automatically.
      def filter(&block);   bool_wrap(:filter,   &block); end
      def must(&block);     bool_wrap(:must,     &block); end
      def should(&block);   bool_wrap(:should,   &block); end
      def must_not(&block); bool_wrap(:must_not, &block); end

      # Override bool to propagate the model's QueryFilter module into BoolContext,
      # so that scope methods are available inside nested filter/must/should blocks.
      def bool(&block)
        bc = BoolContext.new(@_filter_module)
        bc.instance_exec(&block) if block_given?
        @clauses << bc.to_h
      end

      private

      def bool_wrap(clause_type, &block)
        bc = BoolContext.new(@_filter_module)
        bc.public_send(clause_type, &block)
        @clauses << bc.to_h
      end
    end
  end
end

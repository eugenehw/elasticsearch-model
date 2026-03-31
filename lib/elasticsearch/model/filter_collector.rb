# frozen_string_literal: true

module Elasticsearch
  module Model
    # Accumulates filter clauses inside filter {} / must {} / should {} blocks.
    #
    # Most clauses are stored as raw Hashes.
    # range() stores a typed Query::Range so search_index can introspect:
    #   criteria.bool.filter.find { |f| f.is_a?(Query::Range) && f.field == 'published_at' }
    #
    # Query::Range is hash-compatible (responds to [], key?, dig) so legacy
    # code like `filters.find { |f| f['range'] }` continues to work.
    class FilterCollector
      attr_reader :clauses

      def initialize(model_qf_mod = nil)
        @clauses = []
        @model_qf_mod = model_qf_mod
        extend(model_qf_mod) if model_qf_mod
      end

      def term(field, value)
        @clauses << { 'term' => { field.to_s => value } }
      end

      def terms(field, values)
        @clauses << { 'terms' => { field.to_s => Array(values) } }
      end

      # Stores a typed Query::Range for introspection while remaining hash-compatible.
      def range(field, opts = {})
        @clauses << Query::Range.new(field, opts)
      end

      def exists(field)
        @clauses << { 'exists' => { 'field' => field.to_s } }
      end

      def match(field, value)
        @clauses << { 'match' => { field.to_s => value } }
      end

      def match_phrase(field, value)
        @clauses << { 'match_phrase' => { field.to_s => value } }
      end

      def prefix(field, value)
        @clauses << { 'prefix' => { field.to_s => { 'value' => value } } }
      end

      def wildcard(field, value)
        @clauses << { 'wildcard' => { field.to_s => { 'value' => value } } }
      end

      def ids(values)
        @clauses << { 'ids' => { 'values' => Array(values) } }
      end

      # Accepts a block (creates BoolContext) or a raw hash for arbitrary bool structures.
      def bool(raw = nil, &block)
        if block_given?
          bc = BoolContext.new(@model_qf_mod)
          block.arity == 0 ? bc.instance_exec(&block) : block.call(bc)
          @clauses << bc.to_h
        else
          @clauses << { 'bool' => deep_stringify(raw || {}) }
        end
      end

      # Inject any raw clause hash directly.
      def raw(hash)
        @clauses << deep_stringify(hash)
      end

      private

      def deep_stringify(obj)
        case obj
        when Hash  then obj.each_with_object({}) { |(k, v), h| h[k.to_s] = deep_stringify(v) }
        when Array then obj.map { |v| deep_stringify(v) }
        else            obj
        end
      end
    end
  end
end

# frozen_string_literal: true

module Elasticsearch
  module Model
    # DSL for building aggregations inside Criteria#aggregate { } blocks.
    #
    # Named aggregation types are methods; unknown types fall through method_missing
    # so every ES aggregation keyword works automatically.
    #
    # Example (DSL block):
    #   .aggregate(:by_platform) { terms field: 'platform', size: 5 }
    #   .aggregate(:status) { |a| a.filters { |f| f.filter(:active) { term 'status', 'active' } } }
    #
    # Example (raw hash — still supported for complex/legacy aggregations):
    #   .aggregate(:dist, { filters: { filters: { ... } } })
    class AggBuilder
      def initialize(name, model_qf_mod = nil)
        @name         = name.to_s
        @type         = nil
        @opts         = {}
        @sub_aggs     = {}
        @model_qf_mod = model_qf_mod
      end

      # ── Common aggregation types (explicit for IDE auto-complete) ─────────────

      def terms(**opts)
        set_agg('terms', deep_stringify(opts))
      end

      def date_histogram(**opts)
        set_agg('date_histogram', deep_stringify(opts))
      end

      def histogram(**opts)
        set_agg('histogram', deep_stringify(opts))
      end

      %w[avg max min sum stats extended_stats value_count cardinality
         top_hits geo_bounds geo_centroid percentiles].each do |metric|
        define_method(metric) { |**opts| set_agg(metric, deep_stringify(opts)) }
      end

      def nested(**opts)
        set_agg('nested', deep_stringify(opts))
      end

      def reverse_nested(**opts)
        set_agg('reverse_nested', deep_stringify(opts))
      end

      # ES `filter` aggregation — takes a filter block.
      def filter(&block)
        collector = FilterCollector.new(@model_qf_mod)
        collector.instance_exec(&block) if block_given?
        clause = if collector.clauses.size == 1
          clause_h(collector.clauses.first)
        else
          { 'bool' => { 'must' => collector.clauses.map { |c| clause_h(c) } } }
        end
        set_agg('filter', clause)
      end

      # ES `filters` aggregation — groups by named filter buckets.
      #
      #   .aggregate(:status) do |a|
      #     a.filters do |f|
      #       f.filter(:active)  { term 'status', 'active' }
      #       f.filter(:deleted) { term 'status', 'deleted' }
      #     end
      #   end
      def filters(&block)
        fb = FiltersBuilder.new(@model_qf_mod)
        yield fb if block_given?
        set_agg('filters', { 'filters' => fb.to_h })
      end

      # Sub-aggregations
      def aggs(name, &block)
        ab = AggBuilder.new(name, @model_qf_mod)
        ab.instance_exec(&block) if block_given?
        @sub_aggs[name.to_s] = ab.build
        self
      end
      alias aggregate aggs

      # ── Compilation ──────────────────────────────────────────────────────────

      # Returns just the agg body (type + opts + sub-aggs), keyed by type.
      def build
        raise ArgumentError, "No aggregation type set for '#{@name}'" unless @type

        h = { @type => @opts }
        h['aggs'] = @sub_aggs unless @sub_aggs.empty?
        h
      end

      # Returns the full { name => { type => opts } } hash for top-level merging.
      def to_h
        { @name => build }
      end

      # Catch-all: any ES aggregation keyword not listed above.
      def method_missing(name, *args, **opts, &block)
        if name.to_s =~ /\A[a-z][a-z0-9_]*\z/
          payload = args.first || opts
          set_agg(name.to_s, deep_stringify(payload))
        else
          super
        end
      end

      def respond_to_missing?(name, include_private = false)
        name.to_s =~ /\A[a-z][a-z0-9_]*\z/ || super
      end

      private

      def set_agg(type, opts)
        @type = type
        @opts = opts
        self
      end

      def clause_h(c)
        c.is_a?(Hash) ? c : c.to_h
      end

      def deep_stringify(obj)
        case obj
        when Hash  then obj.each_with_object({}) { |(k, v), h| h[k.to_s] = deep_stringify(v) }
        when Array then obj.map { |v| deep_stringify(v) }
        else            obj
        end
      end
    end

    # ── FiltersBuilder ────────────────────────────────────────────────────────
    # Used inside AggBuilder#filters { } to define named filter buckets.
    class FiltersBuilder
      def initialize(model_qf_mod = nil)
        @named_filters = {}
        @model_qf_mod  = model_qf_mod
      end

      def filter(name, &block)
        collector = FilterCollector.new(@model_qf_mod)
        collector.instance_exec(&block) if block_given?
        @named_filters[name.to_s] = if collector.clauses.size == 1
          clause_h(collector.clauses.first)
        else
          { 'bool' => { 'must' => collector.clauses.map { |c| clause_h(c) } } }
        end
        self
      end

      def to_h
        @named_filters
      end

      private

      def clause_h(c)
        c.is_a?(Hash) ? c : c.to_h
      end
    end
  end
end

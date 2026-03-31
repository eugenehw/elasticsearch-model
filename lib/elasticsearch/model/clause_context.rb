# frozen_string_literal: true

module Elasticsearch
  module Model
    # Context object evaluated inside bool sub-clause blocks: filter{}, must{}, should{}, must_not{}.
    # Every method appends a raw Hash to @clauses.
    # method_missing catches any ES query keyword not explicitly listed (e.g. percolate, knn, …).
    class ClauseContext
      attr_reader :clauses

      def initialize
        @clauses = []
      end

      # ── Term-level ────────────────────────────────────────────────────────────

      def term(field, value = :__unset__, opts = {})
        if value == :__unset__
          # called with a hash: term('field' => value)
          @clauses << { 'term' => deep_stringify(field) }
        else
          @clauses << { 'term' => { field.to_s => value } }
        end
      end

      def terms(field, values = :__unset__)
        if values == :__unset__
          @clauses << { 'terms' => deep_stringify(field) }
        else
          @clauses << { 'terms' => { field.to_s => Array(values) } }
        end
      end

      def range(field, opts = {})
        @clauses << { 'range' => { field.to_s => deep_stringify(opts) } }
      end

      def exists(field = :__unset__, opts = {})
        if field == :__unset__
          @clauses << { 'exists' => deep_stringify(opts) }
        else
          @clauses << { 'exists' => { 'field' => field.to_s } }
        end
      end

      def ids(values)
        @clauses << { 'ids' => { 'values' => Array(values) } }
      end

      def prefix(field, value_or_opts)
        val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : { 'value' => value_or_opts }
        @clauses << { 'prefix' => { field.to_s => val } }
      end

      def wildcard(field, value_or_opts)
        val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : { 'value' => value_or_opts }
        @clauses << { 'wildcard' => { field.to_s => val } }
      end

      def regexp(field, value_or_opts)
        val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : { 'value' => value_or_opts }
        @clauses << { 'regexp' => { field.to_s => val } }
      end

      def fuzzy(field, value_or_opts)
        val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : { 'value' => value_or_opts }
        @clauses << { 'fuzzy' => { field.to_s => val } }
      end

      # ── Full-text ─────────────────────────────────────────────────────────────

      def match(field_or_hash, value_or_opts = :__unset__)
        if value_or_opts == :__unset__
          # called with hash: match('field' => { query: value })
          @clauses << { 'match' => normalize_field_hash(field_or_hash) }
        else
          val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : value_or_opts
          @clauses << { 'match' => { field_or_hash.to_s => val } }
        end
      end

      def match_phrase(field_or_hash, value_or_opts = :__unset__)
        if value_or_opts == :__unset__
          @clauses << { 'match_phrase' => normalize_field_hash(field_or_hash) }
        else
          val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : value_or_opts
          @clauses << { 'match_phrase' => { field_or_hash.to_s => val } }
        end
      end

      def match_phrase_prefix(field, value_or_opts = {})
        val = value_or_opts.is_a?(Hash) ? deep_stringify(value_or_opts) : value_or_opts
        @clauses << { 'match_phrase_prefix' => { field.to_s => val } }
      end

      def match_all(opts = {})
        @clauses << { 'match_all' => deep_stringify(opts) }
      end

      def match_none(opts = {})
        @clauses << { 'match_none' => deep_stringify(opts) }
      end

      def multi_match(query, fields:, **opts)
        h = { 'query' => query, 'fields' => fields }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'multi_match' => h }
      end

      def query_string(query, **opts)
        h = { 'query' => query }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'query_string' => h }
      end

      def simple_query_string(query, **opts)
        h = { 'query' => query }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'simple_query_string' => h }
      end

      # ── Compound ──────────────────────────────────────────────────────────────

      def bool(&block)
        bc = BoolContext.new
        bc.instance_exec(&block) if block_given?
        @clauses << bc.to_h
      end

      def dis_max(*queries, tie_breaker: nil)
        h = { 'queries' => queries.map { |q| q.is_a?(Hash) ? q : q.to_h } }
        h['tie_breaker'] = tie_breaker if tie_breaker
        @clauses << { 'dis_max' => h }
      end

      def constant_score(boost: nil, &block)
        cc = ClauseContext.new
        cc.instance_exec(&block) if block_given?
        filter_h = cc.clauses.size == 1 ? cc.clauses.first : { 'bool' => { 'must' => cc.clauses } }
        h = { 'filter' => filter_h }
        h['boost'] = boost if boost
        @clauses << { 'constant_score' => h }
      end

      def boosting(positive:, negative:, negative_boost:)
        @clauses << { 'boosting' => {
          'positive'       => positive.is_a?(Hash) ? positive : positive.to_h,
          'negative'       => negative.is_a?(Hash) ? negative : negative.to_h,
          'negative_boost' => negative_boost
        } }
      end

      # ── Joining ───────────────────────────────────────────────────────────────

      def nested(path, &block)
        cc = ClauseContext.new
        cc.instance_exec(&block) if block_given?
        query = cc.clauses.size == 1 ? cc.clauses.first : { 'bool' => { 'must' => cc.clauses } }
        @clauses << { 'nested' => { 'path' => path.to_s, 'query' => query } }
      end

      def has_child(type, **opts, &block)
        cc = ClauseContext.new
        cc.instance_exec(&block) if block_given?
        query = cc.clauses.size == 1 ? cc.clauses.first : { 'bool' => { 'must' => cc.clauses } }
        h = { 'type' => type.to_s, 'query' => query }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'has_child' => h }
      end

      def has_parent(parent_type, **opts, &block)
        cc = ClauseContext.new
        cc.instance_exec(&block) if block_given?
        query = cc.clauses.size == 1 ? cc.clauses.first : { 'bool' => { 'must' => cc.clauses } }
        h = { 'parent_type' => parent_type.to_s, 'query' => query }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'has_parent' => h }
      end

      # ── Geo ───────────────────────────────────────────────────────────────────

      def geo_distance(field, distance:, **location)
        h = { 'distance' => distance, field.to_s => location.transform_keys(&:to_s) }
        @clauses << { 'geo_distance' => h }
      end

      def geo_bounding_box(field, top_left:, bottom_right:)
        @clauses << { 'geo_bounding_box' => { field.to_s => {
          'top_left'     => top_left,
          'bottom_right' => bottom_right
        } } }
      end

      # ── Specialised ───────────────────────────────────────────────────────────

      def more_like_this(fields:, like:, **opts)
        h = { 'fields' => fields, 'like' => like }
        opts.each { |k, v| h[k.to_s] = v }
        @clauses << { 'more_like_this' => h }
      end

      def script(source, params: nil, lang: nil)
        s = { 'source' => source }
        s['params'] = params if params
        s['lang']   = lang   if lang
        @clauses << { 'script' => { 'script' => s } }
      end

      def knn(field, query_vector:, k:, num_candidates:, **opts)
        h = { 'field' => field.to_s, 'query_vector' => query_vector, 'k' => k, 'num_candidates' => num_candidates }
        opts.each { |k2, v| h[k2.to_s] = v }
        @clauses << { 'knn' => h }
      end

      # Inject a raw clause hash directly (escape hatch).
      def raw(hash)
        @clauses << deep_stringify(hash)
      end

      # Catch-all: any unknown ES query keyword (e.g. percolate, pinned, …)
      def method_missing(name, *args, **opts, &block)
        if name.to_s =~ /\A[a-z][a-z0-9_]*\z/
          payload = args.first || opts
          @clauses << { name.to_s => deep_stringify(payload) }
        else
          super
        end
      end

      def respond_to_missing?(name, include_private = false)
        name.to_s =~ /\A[a-z][a-z0-9_]*\z/ || super
      end

      private

      def deep_stringify(obj)
        case obj
        when Hash  then obj.each_with_object({}) { |(k, v), h| h[k.to_s] = deep_stringify(v) }
        when Array then obj.map { |v| deep_stringify(v) }
        else            obj
        end
      end

      def normalize_field_hash(h)
        h.each_with_object({}) do |(k, v), out|
          out[k.to_s] = v.is_a?(Hash) ? deep_stringify(v) : v
        end
      end
    end

    # ── BoolContext ──────────────────────────────────────────────────────────────
    # Evaluated inside bool {} blocks. Provides filter/must/should/must_not.
    # Each sub-block runs in a FilterCollector that collects individual clauses.
    class BoolContext
      def initialize(model_qf_mod = nil)
        @model_qf_mod       = model_qf_mod
        @filter             = []
        @must               = []
        @should             = []
        @must_not           = []
        @minimum_should_match = nil
        @boost              = nil
      end

      def filter(&block)
        fc = FilterCollector.new(@model_qf_mod)
        if block_given?
          block.arity == 0 ? fc.instance_exec(&block) : block.call(fc)
        end
        @filter.concat(fc.clauses)
        self
      end

      def must(&block)
        fc = FilterCollector.new(@model_qf_mod)
        if block_given?
          block.arity == 0 ? fc.instance_exec(&block) : block.call(fc)
        end
        @must.concat(fc.clauses)
        self
      end

      def should(&block)
        fc = FilterCollector.new(@model_qf_mod)
        if block_given?
          block.arity == 0 ? fc.instance_exec(&block) : block.call(fc)
        end
        @should.concat(fc.clauses)
        self
      end

      def must_not(&block)
        fc = FilterCollector.new(@model_qf_mod)
        if block_given?
          block.arity == 0 ? fc.instance_exec(&block) : block.call(fc)
        end
        @must_not.concat(fc.clauses)
        self
      end

      def minimum_should_match(val)
        @minimum_should_match = val
        self
      end

      def boost(val)
        @boost = val
        self
      end

      def to_h
        bool = {}
        bool['filter']   = @filter.map { |c| clause_h(c) }           unless @filter.empty?
        bool['must']     = unwrap(@must)                               unless @must.empty?
        bool['should']   = @should.map { |c| clause_h(c) }            unless @should.empty?
        bool['must_not'] = unwrap(@must_not)                           unless @must_not.empty?
        bool['minimum_should_match'] = @minimum_should_match           if @minimum_should_match
        bool['boost']    = @boost                                      if @boost
        { 'bool' => bool }
      end

      private

      def clause_h(c)
        c.is_a?(Hash) ? c : c.to_h
      end

      def unwrap(arr)
        clauses = arr.map { |c| clause_h(c) }
        clauses.size == 1 ? clauses.first : clauses
      end
    end
  end
end

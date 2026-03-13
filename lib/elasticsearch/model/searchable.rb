# frozen_string_literal: true

module Elasticsearch
  module Model
    # Include this module in any class to give it Elasticsearch query capabilities.
    #
    #   class Influencer
    #     include Elasticsearch::Model::Searchable
    #
    #     index_name 'influencers'
    #
    #     # Optional: custom response class (must inherit ElasticsearchResponse)
    #     class Response < Elasticsearch::Model::ElasticsearchResponse
    #       def active_records = records.select { |r| r['status'] == 'active' }
    #     end
    #
    #     # Optional: dynamic index routing
    #     def self.search_index(criteria = nil)
    #       range = criteria&.date_filter_for(:published_at)
    #       return "content_#{Date.parse(range['gte']).year}" if range
    #
    #       'influencers'
    #     end
    #   end
    module Searchable
      def self.included(base)
        base.extend(ClassMethods)
        default_index = base.name ? "#{base.name.downcase}s" : 'unknown'
        base.instance_variable_set(:@_es_index_name, default_index)
      end

      module ClassMethods
        # ── Index configuration ───────────────────────────────────────────────

        # Get or set the default index name.
        def index_name(name = nil)
          if name
            @_es_index_name = name.to_s
          else
            @_es_index_name
          end
        end

        # Declare model field metadata (used for documentation / future mapping support).
        # No-op by default — override to add behaviour.
        def field(name, type: nil, **_opts)
          @_es_fields ||= {}
          @_es_fields[name.to_sym] = { type: type }
        end

        # Returns declared field metadata hash.
        def fields
          @_es_fields || {}
        end

        # Response class used by all searches on this model.
        # Defaults to MyModel::Response if defined, else ElasticsearchResponse.
        def response_class
          return @_response_class if defined?(@_response_class) && @_response_class

          @_response_class = if const_defined?(:Response, false)
            const_get(:Response, false)
          else
            ElasticsearchResponse
          end
        end

        # Explicitly set the response class.
        def response_class=(klass)
          @_response_class = klass
        end

        # ── Query entry points ────────────────────────────────────────────────

        # Start building a lazy query. Returns a Criteria (does NOT execute).
        #
        #   Influencer.query { smart_match :name, "john" }.from(0).size(10).search
        def query(&block)
          Criteria.new(self).query(&block)
        end

        # Convenience: build a Criteria without any initial query block.
        def criteria
          Criteria.new(self)
        end

        # Start building with filter clauses.
        def filter(&block)
          Criteria.new(self).filter(&block)
        end

        # Start building with an aggregation.
        def aggregate(name, raw_hash = nil, &block)
          Criteria.new(self).aggregate(name, raw_hash, &block)
        end

        # Start building with a sort clause. Two forms:
        #
        #   # Field + direction shorthand:
        #   Model.sort(:published_at, :desc).filter { ... }.search
        #
        #   # Block returning a Hash or Array of sort clauses (full ES syntax):
        #   Model.sort { { published_at: { order: :desc }, _score: :desc } }.search
        def sort(field = nil, direction = :asc, &block)
          c = Criteria.new(self)
          if block_given?
            c.sort(&block)
          elsif field
            f = field.to_s
            d = direction.to_s
            c.sort { { f => { 'order' => d } } }
          else
            c
          end
        end

        # ── Index routing ─────────────────────────────────────────────────────

        # Override in your model for dynamic routing.
        # Receives the Criteria so you can inspect query conditions.
        #
        #   def self.search_index(criteria = nil)
        #     range = criteria&.date_filter_for(:published_at)
        #     range ? "content_#{Date.parse(range['gte']).year}" : index_name
        #   end
        #
        # @param criteria [Criteria, nil]
        # @return [String, Array<String>]
        def search_index(_criteria = nil)
          @_es_index_name
        end
      end
    end
  end
end

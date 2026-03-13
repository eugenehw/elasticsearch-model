# frozen_string_literal: true

# spec/criteria_spec.rb  (minitest)

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'elasticsearch/model'
require 'minitest/autorun'
require 'date'

class MockModel
  include Elasticsearch::Model::Searchable
  index_name 'mock_index'

  field :platform,   type: String
  field :created_at, type: DateTime
  field :status,     type: String

  def self.search_index(criteria = nil)
    range = criteria&.date_filter_for(:published_at)
    if range
      'influencers_2025'
    else
      'influencers_alias'
    end
  end

  # Model-specific QueryFilter helpers — available in both query {} and filter {} blocks
  module QueryFilter
    def platform(value)
      term 'platform', value
    end

    def active
      term 'status', 'active'
    end
  end

  # Custom response class
  class Response < Elasticsearch::Model::ElasticsearchResponse
    def active_records
      records.select { |r| r['status'] == 'active' }
    end
  end
end

class CriteriaTest < Minitest::Test
  # ── query building ──────────────────────────────────────────────────────────

  def test_query_returns_criteria
    c = MockModel.query { match_all }
    assert_instance_of Elasticsearch::Model::Criteria, c
  end

  def test_chaining_returns_criteria
    c = MockModel.query { match_all }.from(0).size(10)
    assert_instance_of Elasticsearch::Model::Criteria, c
  end

  def test_compiled_query_contains_match_all
    c = MockModel.query { match_all }
    assert_equal({ 'match_all' => {} }, c.to_query['query'])
  end

  # ── smart_match injection ────────────────────────────────────────────────────

  def test_smart_match_builds_bool_should
    c = MockModel.query { smart_match :name, 'john' }
    q = c.to_query
    bool = q.dig('query', 'bool')
    assert bool, "Expected bool query, got: #{q.inspect}"
    assert_equal 2, bool['should'].size
    types = bool['should'].map { |s| s.keys.first }
    assert_includes types, 'match_phrase'
    assert_includes types, 'match'
  end

  # ── date_range injection ─────────────────────────────────────────────────────

  def test_date_range_builds_filter_range
    c = MockModel.query { date_range :published_at, from: '2025-01-01', to: '2025-12-31' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert filters, 'Expected filter context'
    range = filters.find { |f| f['range'] }
    assert range, 'Expected a range filter'
    assert_equal '2025-01-01', range.dig('range', 'published_at', 'gte')
    assert_equal '2025-12-31', range.dig('range', 'published_at', 'lte')
  end

  # ── date_filter_for introspection ───────────────────────────────────────────

  def test_date_filter_for_returns_range_hash
    c = MockModel.query { date_range :published_at, from: '2025-01-01' }
    range = c.date_filter_for(:published_at)
    assert_equal '2025-01-01', range['gte']
  end

  def test_date_filter_for_returns_nil_when_absent
    c = MockModel.query { match_all }
    assert_nil c.date_filter_for(:published_at)
  end

  # ── from / size ──────────────────────────────────────────────────────────────

  def test_from_and_size_appear_in_query
    c = MockModel.criteria.from(10).size(5)
    q = c.to_query
    assert_equal 10, q['from']
    assert_equal 5,  q['size']
  end

  # ── default index name ───────────────────────────────────────────────────────

  def test_index_name_default
    assert_equal 'mock_index', MockModel.index_name
  end

  # ── search_index dynamic routing ─────────────────────────────────────────────

  def test_search_index_routing
    router = build_router
    c_no_date   = router.criteria
    c_with_date = router.query { date_range :published_at, from: '2025-03-01' }

    assert_equal 'alias_name',   router.search_index(c_no_date)
    assert_equal 'content_2025', router.search_index(c_with_date)
  end

  # ── filter {} block ─────────────────────────────────────────────────────────

  def test_filter_block_adds_bool_filter_clauses
    c = MockModel.filter { term 'status', 'active'; term 'platform', 'instagram' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_instance_of Array, filters
    assert_equal 2, filters.size
    assert_includes filters, { 'term' => { 'status' => 'active' } }
    assert_includes filters, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_filter_chaining_accumulates_clauses
    c = MockModel.filter { term 'status', 'active' }
                 .filter { exists 'name' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_equal 2, filters.size
    assert_includes filters, { 'term' => { 'status' => 'active' } }
    assert_includes filters, { 'exists' => { 'field' => 'name' } }
  end

  def test_filter_inside_query_block
    c = MockModel.query do
      filter { term 'platform', 'instagram' }
    end
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_equal [{ 'term' => { 'platform' => 'instagram' } }], filters
  end

  def test_filter_merges_with_query_bool_filter
    c = MockModel.query { date_range :published_at, from: '2025-01-01' }
                 .filter { term 'platform', 'instagram' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_instance_of Array, filters
    assert filters.any? { |f| f['term'] },  'expected term filter'
    assert filters.any? { |f| f['range'] }, 'expected range filter'
  end

  # ── model-specific QueryFilter helpers ──────────────────────────────────────

  def test_model_query_filter_in_filter_block
    c = MockModel.filter { platform 'instagram' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_model_query_filter_in_query_block
    c = MockModel.query { active }
    q = c.to_query
    assert_equal({ 'term' => { 'status' => 'active' } }, q['query'])
  end

  def test_model_query_filter_inside_query_filter_block
    c = MockModel.query { filter { platform 'instagram' } }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'platform' => 'instagram' } }
  end

  # ── raw hash aggregate (filters agg type) ───────────────────────────────────

  def test_raw_aggregate_hash
    c = MockModel.aggregate(:activity_organic_distribution, {
      filters: {
        filters: {
          activity: { exists: { field: 'activity_ids' } },
          organic:  { bool: { must_not: { exists: { field: 'activity_ids' } } } }
        }
      },
      aggs: {
        activity_ids: { terms: { field: 'activity_ids', size: 10 } }
      }
    })

    aggs = c.to_query['aggregations']
    assert aggs, 'expected aggregations key'

    dist = aggs['activity_organic_distribution']
    assert dist, 'expected activity_organic_distribution agg'
    assert dist['filters'], 'expected filters key'
    assert_equal 'activity_ids', dist.dig('filters', 'filters', 'activity', 'exists', 'field')
    assert dist.dig('aggs', 'activity_ids', 'terms')
  end

  def test_raw_aggregate_mixed_with_dsl_aggregate
    c = MockModel
          .aggregate(:by_platform) { terms field: 'platform', size: 5 }
          .aggregate(:organic, { filters: { filters: { organic: { bool: { must_not: { exists: { field: 'activity_ids' } } } } } } })

    aggs = c.to_query['aggregations']
    assert aggs['by_platform'], 'dsl agg missing'
    assert aggs['organic'],     'raw agg missing'
  end

  # ── filter_collector range / bool ────────────────────────────────────────────

  def test_filter_block_with_range
    c = MockModel.filter { range 'published_at', gte: '2025-01-01', lte: '2025-12-31' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    range = filters.find { |f| f['range'] }
    assert range
    assert_equal '2025-01-01', range.dig('range', 'published_at', 'gte')
  end

  def test_filter_block_with_bool
    c = MockModel.filter { bool(must_not: { exists: { field: 'deleted_at' } }) }
    filters = c.to_query.dig('query', 'bool', 'filter')
    bool_clause = filters.find { |f| f['bool'] }
    assert bool_clause
    assert bool_clause.dig('bool', 'must_not', 'exists', 'field')
  end

  # ── new API: query.bool.filter {} ──────────────────────────────────────────

  def test_bool_builder_filter
    q = MockModel.criteria
    q.bool.filter { |f| f.term('status', 'active') }
    q.bool.filter { |f| f.term('platform', 'instagram') }
    filters = q.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'status' => 'active' } }
    assert_includes filters, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_bool_builder_filter_returns_criteria_for_chaining
    q = MockModel.criteria
    result = q.bool.filter { |f| f.term('status', 'active') }
    assert_instance_of Elasticsearch::Model::Criteria, result
  end

  def test_bool_builder_filter_introspection
    q = MockModel.criteria
    q.bool.filter { |f| f.range('published_at', gte: '2025-01-01') }
    filters = q.bool.filter   # no block → returns array
    assert_equal 1, filters.size
    assert filters.first.is_a?(Elasticsearch::Model::Query::Range)
    assert_equal 'published_at', filters.first.field
  end

  # ── DSL aggregate with filters ───────────────────────────────────────────────

  def test_dsl_aggregate_filters
    c = MockModel.aggregate(:status) do |a|
      a.filters do |f|
        f.filter(:active)  { term 'status', 'active' }
        f.filter(:deleted) { term 'status', 'deleted' }
      end
    end
    aggs = c.to_query['aggregations']
    assert aggs['status']
    assert_equal({ 'term' => { 'status' => 'active' } },  aggs.dig('status', 'filters', 'filters', 'active'))
    assert_equal({ 'term' => { 'status' => 'deleted' } }, aggs.dig('status', 'filters', 'filters', 'deleted'))
  end

  # ── custom response class ────────────────────────────────────────────────────

  def test_custom_response_class_detected
    assert_equal MockModel::Response, MockModel.response_class
  end

  private

  def build_router
    Class.new do
      include Elasticsearch::Model::Searchable
      index_name 'alias_name'

      def self.search_index(criteria = nil)
        range = criteria&.date_filter_for(:published_at)
        return 'alias_name' unless range

        year = parse_year(range['gte'] || range['lte'])
        year ? "content_#{year}" : 'alias_name'
      end

      def self.parse_year(date_str)
        Date.parse(date_str).year
      rescue ArgumentError, TypeError
        nil
      end
    end
  end
end

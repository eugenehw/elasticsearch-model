# frozen_string_literal: true

# spec/criteria_spec.rb  (minitest)

$LOAD_PATH.unshift File.expand_path('./lib', __dir__)
require 'elasticsearch/model'
require 'minitest/autorun'
require 'date'

class MockModel
  include Elasticsearch::Model::Searchable
  index_name 'mock_index'

  def self.search_index(criteria = nil)
    range = criteria&.date_filter_for(:published_at)
    if range
      'influencers_2025'
    else
      'influencers_alias'
    end
  end

  # Scopes — add named filter methods to QueryFilter
  scope :active do
    term 'status', 'active'
  end

  scope :deleted do
    term 'status', 'deleted'
  end

  scope :by_platform do |platform|
    term 'platform', platform
  end

  scope :followers_gte do |n|
    range 'follower_count', gte: n
  end

  # Model-specific QueryFilter helpers — available in both query {} and filter {} blocks
  module QueryFilter
    def platform(value)
      term 'platform', value
    end
  end

  # Agg scopes
  agg_scope :group_by_status do |agg, f|
    agg.aggregate(:status) do |status_agg|
      status_agg.filters(:active)  { f.active }
      status_agg.filters(:deleted) { f.deleted }
    end
  end

  agg_scope :group_by_team do |agg|
    agg.aggregate(:teams) do
      terms field: 'team', size: 10
    end
  end

  agg_scope :timeline do |agg|
    agg.aggregate(:timeline) do
      date_histogram field: 'published_at', calendar_interval: 'month'
    end
  end

  agg_scope :team_with_status do |agg, f|
    agg.group_by_team do |teams_agg|
      teams_agg.group_by_status
    end
  end

  agg_scope :by_platform_source do |a|
    a.aggregate(:platform) { terms field: 'platform' }
  end

  agg_scope :by_status_source do |a|
    a.aggregate(:status_source) { terms field: 'status' }
  end

  agg_scope :by_field do |agg, f, field, size: 10|
    agg.aggregate(:by_field) { terms field: field.to_s, size: size }
  end

  agg_scope :top_by_field do |agg, f, field|
    agg.aggregate(:top) { top_hits size: 3 }
    agg.aggregate(:terms_agg) { terms field: field.to_s }
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

  # ── scope macro ──────────────────────────────────────────────────────────────

  def test_scope_adds_method_to_query_filter
    assert MockModel::QueryFilter.method_defined?(:active)
    assert MockModel::QueryFilter.method_defined?(:deleted)
  end

  def test_scope_usable_in_filter_block_zero_arity
    c = MockModel.filter { active }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'status' => 'active' } }
  end

  def test_scope_usable_in_filter_block_one_arity
    c = MockModel.filter { |f| f.active }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'status' => 'active' } }
  end

  def test_scope_auto_creates_query_filter_module
    model = Class.new do
      include Elasticsearch::Model::Searchable
      scope :published do
        term 'published', true
      end
    end
    assert model.const_defined?(:QueryFilter, false)
    assert model::QueryFilter.method_defined?(:published)
  end

  # ── to_h alias ───────────────────────────────────────────────────────────────

  def test_to_h_alias_for_to_query
    c = MockModel.filter { active }
    assert_equal c.to_query, c.to_h
  end

  # ── filters(:name) flat API ──────────────────────────────────────────────────

  def test_filters_named_bucket_flat_api
    c = MockModel.aggregate(:status) do |agg|
      agg.filters(:active)  { term 'status', 'active' }
      agg.filters(:deleted) { term 'status', 'deleted' }
    end
    aggs = c.to_query['aggregations']
    assert_equal({ 'term' => { 'status' => 'active' } },  aggs.dig('status', 'filters', 'filters', 'active'))
    assert_equal({ 'term' => { 'status' => 'deleted' } }, aggs.dig('status', 'filters', 'filters', 'deleted'))
  end

  def test_filters_named_bucket_with_scope
    c = MockModel.aggregate(:status) do |agg, f|
      agg.filters(:active)  { f.active }
      agg.filters(:deleted) { f.deleted }
    end
    aggs = c.to_query['aggregations']
    assert_equal({ 'term' => { 'status' => 'active' } },  aggs.dig('status', 'filters', 'filters', 'active'))
    assert_equal({ 'term' => { 'status' => 'deleted' } }, aggs.dig('status', 'filters', 'filters', 'deleted'))
  end

  # ── bool { should { } } with scopes ─────────────────────────────────────────

  def test_bool_with_should_and_scopes
    c = MockModel.filter do
      bool do
        should do
          active
          deleted
        end
        minimum_should_match 1
      end
    end
    filters = c.to_query.dig('query', 'bool', 'filter')
    bool_clause = filters.find { |f| f['bool'] }
    assert bool_clause, 'expected bool clause'
    should_clauses = bool_clause.dig('bool', 'should')
    assert_instance_of Array, should_clauses
    assert_includes should_clauses, { 'term' => { 'status' => 'active' } }
    assert_includes should_clauses, { 'term' => { 'status' => 'deleted' } }
    assert_equal 1, bool_clause.dig('bool', 'minimum_should_match')
  end

  # ── agg_scope ────────────────────────────────────────────────────────────────

  def test_agg_scope_defines_class_method
    assert MockModel.respond_to?(:group_by_status)
    assert MockModel.respond_to?(:group_by_team)
    assert MockModel.respond_to?(:timeline)
  end

  def test_agg_scope_returns_criteria
    c = MockModel.group_by_status
    assert_instance_of Elasticsearch::Model::Criteria, c
  end

  def test_agg_scope_group_by_status_builds_filters_agg
    aggs = MockModel.group_by_status.to_query['aggregations']
    assert aggs.key?('status'), "expected 'status' key"
    assert_equal({ 'term' => { 'status' => 'active' } },  aggs.dig('status', 'filters', 'filters', 'active'))
    assert_equal({ 'term' => { 'status' => 'deleted' } }, aggs.dig('status', 'filters', 'filters', 'deleted'))
  end

  def test_agg_scope_group_by_team_builds_terms_agg
    aggs = MockModel.group_by_team.to_query['aggregations']
    assert aggs.key?('teams'), "expected 'teams' key"
    assert_equal({ 'field' => 'team', 'size' => 10 }, aggs.dig('teams', 'terms'))
  end

  def test_agg_scope_timeline_builds_date_histogram
    aggs = MockModel.timeline.to_query['aggregations']
    assert aggs.key?('timeline'), "expected 'timeline' key"
    assert_equal 'published_at', aggs.dig('timeline', 'date_histogram', 'field')
    assert_equal 'month', aggs.dig('timeline', 'date_histogram', 'calendar_interval')
  end

  def test_agg_scope_returns_chainable_criteria
    c = MockModel.group_by_team
    assert_instance_of Elasticsearch::Model::Criteria, c
    assert_equal 0, c.size(0).to_query['size']
  end

  def test_agg_scope_with_call_time_sub_agg_block
    c = MockModel.group_by_status do |agg|
      agg.aggregate(:reach) { sum field: 'reach' }
    end
    aggs = c.to_query['aggregations']
    assert_equal({ 'term' => { 'status' => 'active' } }, aggs.dig('status', 'filters', 'filters', 'active'))
    assert_equal({ 'field' => 'reach' }, aggs.dig('status', 'aggs', 'reach', 'sum'))
  end

  def test_agg_scope_sibling_chaining
    aggs = MockModel.group_by_team.group_by_status.timeline.to_query['aggregations']
    assert aggs.key?('teams'),    "expected 'teams'"
    assert aggs.key?('status'),   "expected 'status'"
    assert aggs.key?('timeline'), "expected 'timeline'"
  end

  def test_agg_scope_nested_team_with_status
    aggs = MockModel.team_with_status.to_query['aggregations']
    assert aggs.key?('teams'), "expected 'teams' key"
    assert_equal({ 'field' => 'team', 'size' => 10 }, aggs.dig('teams', 'terms'))
    assert aggs.dig('teams', 'aggs', 'status'), "expected nested 'status' agg"
    assert_equal(
      { 'term' => { 'status' => 'active' } },
      aggs.dig('teams', 'aggs', 'status', 'filters', 'filters', 'active')
    )
  end

  def test_agg_scope_on_criteria_via_method_missing
    c = MockModel.criteria.group_by_status
    assert_instance_of Elasticsearch::Model::Criteria, c
    aggs = c.to_query['aggregations']
    assert aggs.key?('status')
  end

  def test_agg_scope_respond_to_on_criteria
    c = MockModel.criteria
    assert c.respond_to?(:group_by_status)
  end

  # ── custom response class ────────────────────────────────────────────────────

  def test_custom_response_class_detected
    assert_equal MockModel::Response, MockModel.response_class
  end

  # ── FilterCollector clause methods ────────────────────────────────────────────

  def test_filter_terms_clause
    c = MockModel.filter { terms 'platform', %w[instagram tiktok] }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'terms' => { 'platform' => %w[instagram tiktok] } }
  end

  def test_filter_match_clause_raises
    # match is a scoring query clause and must not be used in filter context
    assert_raises(NoMethodError) { MockModel.filter { match 'name', 'john' } }
  end

  def test_filter_match_phrase_clause_raises
    # match_phrase is a scoring query clause and must not be used in filter context
    assert_raises(NoMethodError) { MockModel.filter { match_phrase 'bio', 'content creator' } }
  end

  def test_filter_prefix_clause
    c = MockModel.filter { prefix 'name', 'joh' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'prefix' => { 'name' => { 'value' => 'joh' } } }
  end

  def test_filter_wildcard_clause
    c = MockModel.filter { wildcard 'name', 'jo*' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'wildcard' => { 'name' => { 'value' => 'jo*' } } }
  end

  def test_filter_ids_clause
    c = MockModel.filter { ids [1, 2, 3] }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'ids' => { 'values' => [1, 2, 3] } }
  end

  def test_filter_raw_clause
    raw_clause = { 'term' => { 'custom_field' => 'custom_value' } }
    c = MockModel.filter { raw(raw_clause) }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, raw_clause
  end

  # ── Criteria-level must / should / must_not ────────────────────────────────

  def test_criteria_must_single
    c = MockModel.must { term 'status', 'active' }
    assert_equal({ 'term' => { 'status' => 'active' } },
                 c.to_query.dig('query', 'bool', 'must'))
  end

  def test_criteria_must_accumulates_multiple
    c = MockModel.must { term 'status', 'active' }
                 .must { term 'platform', 'instagram' }
    must = c.to_query.dig('query', 'bool', 'must')
    assert_instance_of Array, must
    assert_equal 2, must.size
    assert_includes must, { 'term' => { 'status' => 'active' } }
    assert_includes must, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_criteria_should_multiple
    c = MockModel.should { term 'platform', 'instagram' }
                 .should { term 'platform', 'tiktok' }
    should_clauses = c.to_query.dig('query', 'bool', 'should')
    assert_instance_of Array, should_clauses
    assert_equal 2, should_clauses.size
    assert_includes should_clauses, { 'term' => { 'platform' => 'instagram' } }
    assert_includes should_clauses, { 'term' => { 'platform' => 'tiktok' } }
  end

  def test_criteria_must_not_single
    c = MockModel.must_not { term 'status', 'deleted' }
    assert_equal({ 'term' => { 'status' => 'deleted' } },
                 c.to_query.dig('query', 'bool', 'must_not'))
  end

  def test_bool_builder_minimum_should_match
    c = MockModel.criteria
    c.bool.should { |f| f.term('platform', 'instagram') }
    c.bool.should { |f| f.term('platform', 'tiktok') }
    c.bool.minimum_should_match(1)
    assert_equal 1, c.to_query.dig('query', 'bool', 'minimum_should_match')
  end

  # ── Query context clause methods ───────────────────────────────────────────

  def test_query_terms_clause
    c = MockModel.query { terms 'platform', %w[instagram tiktok] }
    assert_equal({ 'terms' => { 'platform' => %w[instagram tiktok] } },
                 c.to_query['query'])
  end

  def test_query_multi_match_clause
    c = MockModel.query { multi_match('john', fields: %w[name bio]) }
    q = c.to_query['query']
    assert_equal 'john',        q.dig('multi_match', 'query')
    assert_equal %w[name bio],  q.dig('multi_match', 'fields')
  end

  def test_query_string_clause
    c = MockModel.query { query_string('john AND smith') }
    assert_equal 'john AND smith', c.to_query.dig('query', 'query_string', 'query')
  end

  def test_query_nested_clause
    c = MockModel.query { nested('tags') { term 'tags.name', 'ruby' } }
    q = c.to_query['query']
    assert_equal 'tags', q.dig('nested', 'path')
    assert_equal({ 'term' => { 'tags.name' => 'ruby' } }, q.dig('nested', 'query'))
  end

  def test_query_knn_clause
    c = MockModel.query { knn('embedding', query_vector: [1.0, 0.5], k: 5, num_candidates: 100) }
    knn = c.to_query.dig('query', 'knn')
    assert_equal 'embedding', knn['field']
    assert_equal 5,   knn['k']
    assert_equal 100, knn['num_candidates']
  end

  # ── Aggregate metric methods ───────────────────────────────────────────────

  def test_aggregate_avg
    aggs = MockModel.aggregate(:avg_f) { avg field: 'follower_count' }.to_query['aggregations']
    assert_equal({ 'field' => 'follower_count' }, aggs.dig('avg_f', 'avg'))
  end

  def test_aggregate_sum
    aggs = MockModel.aggregate(:total) { sum field: 'reach' }.to_query['aggregations']
    assert_equal({ 'field' => 'reach' }, aggs.dig('total', 'sum'))
  end

  def test_aggregate_max
    aggs = MockModel.aggregate(:max_f) { max field: 'followers' }.to_query['aggregations']
    assert_equal({ 'field' => 'followers' }, aggs.dig('max_f', 'max'))
  end

  def test_aggregate_min
    aggs = MockModel.aggregate(:min_f) { min field: 'followers' }.to_query['aggregations']
    assert_equal({ 'field' => 'followers' }, aggs.dig('min_f', 'min'))
  end

  def test_aggregate_cardinality
    aggs = MockModel.aggregate(:uniq) { cardinality field: 'user_id' }.to_query['aggregations']
    assert_equal({ 'field' => 'user_id' }, aggs.dig('uniq', 'cardinality'))
  end

  def test_aggregate_value_count
    aggs = MockModel.aggregate(:cnt) { value_count field: 'user_id' }.to_query['aggregations']
    assert_equal({ 'field' => 'user_id' }, aggs.dig('cnt', 'value_count'))
  end

  def test_aggregate_missing
    aggs = MockModel.aggregate(:no_cat) { missing field: 'category' }.to_query['aggregations']
    assert_equal({ 'field' => 'category' }, aggs.dig('no_cat', 'missing'))
  end

  def test_aggregate_stats
    aggs = MockModel.aggregate(:stats_f) { stats field: 'score' }.to_query['aggregations']
    assert_equal({ 'field' => 'score' }, aggs.dig('stats_f', 'stats'))
  end

  def test_aggregate_percentiles
    aggs = MockModel.aggregate(:pct) { percentiles field: 'latency', percents: [50, 95, 99] }
                    .to_query['aggregations']
    pct = aggs.dig('pct', 'percentiles')
    assert_equal 'latency',      pct['field']
    assert_equal [50, 95, 99],   pct['percents']
  end

  def test_aggregate_top_hits
    aggs = MockModel.aggregate(:top) { top_hits size: 3 }.to_query['aggregations']
    assert_equal({ 'size' => 3 }, aggs.dig('top', 'top_hits'))
  end

  # ── Pipeline aggregations ──────────────────────────────────────────────────

  def test_aggregate_sum_bucket
    c = MockModel.aggregate(:total_sales) do |a|
      a.sum_bucket(buckets_path: 'by_month>sales')
    end
    aggs = c.to_query['aggregations']
    assert_equal({ 'buckets_path' => 'by_month>sales' }, aggs.dig('total_sales', 'sum_bucket'))
  end

  def test_aggregate_bucket_script
    c = MockModel.aggregate(:rate) do |a|
      a.bucket_script(
        buckets_path: { 'eng' => 'engagement>value', 'fol' => 'followers>value' },
        script: 'params.eng / params.fol'
      )
    end
    aggs = c.to_query['aggregations']
    bs = aggs.dig('rate', 'bucket_script')
    assert_equal 'params.eng / params.fol', bs['script']
    assert_equal({ 'eng' => 'engagement>value', 'fol' => 'followers>value' }, bs['buckets_path'])
  end

  # ── Composite aggregation ──────────────────────────────────────────────────

  def test_aggregate_composite_inline_sources
    c = MockModel.aggregate(:by_combo) do |a|
      a.composite do
        size 20
        sources do
          aggregate(:platform) { terms field: 'platform' }
          aggregate(:status)   { terms field: 'status' }
        end
      end
    end
    aggs  = c.to_query['aggregations']
    comp  = aggs.dig('by_combo', 'composite')
    assert_equal 20, comp['size']
    assert_equal [
      { 'platform' => { 'terms' => { 'field' => 'platform' } } },
      { 'status'   => { 'terms' => { 'field' => 'status' } } }
    ], comp['sources']
  end

  def test_aggregate_composite_with_agg_scope_sources
    c = MockModel.aggregate(:by_combo) do |a|
      a.composite do
        size 10
        sources do
          by_platform_source
          by_status_source
        end
      end
    end
    aggs    = c.to_query['aggregations']
    sources = aggs.dig('by_combo', 'composite', 'sources')
    assert_equal 2, sources.size
    assert_equal({ 'terms' => { 'field' => 'platform' } }, sources[0]['platform'])
    assert_equal({ 'terms' => { 'field' => 'status' } },   sources[1]['status_source'])
  end

  # ── KnnBuilder ────────────────────────────────────────────────────────────

  VEC = Array.new(4, 0.1)

  def test_knn_top_level_basic
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10)
    knn = c.to_query['knn']
    assert_equal 'image_embedding', knn['field']
    assert_equal VEC,               knn['query_vector']
    assert_equal 5,                 knn['k']
    assert_equal 10,                knn['num_candidates']
  end

  def test_knn_top_level_with_filter_block
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter { term 'platform', 'instagram' }
    end
    filter = c.to_query.dig('knn', 'filter')
    assert_equal({ 'term' => { 'platform' => 'instagram' } }, filter)
  end

  def test_knn_top_level_with_scope_in_filter
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter { active }
    end
    filter = c.to_query.dig('knn', 'filter')
    assert_equal({ 'term' => { 'status' => 'active' } }, filter)
  end

  def test_knn_top_level_multiple_filters_wrapped_in_bool
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter { term 'platform', 'instagram' }
      filter { active }
    end
    filter = c.to_query.dig('knn', 'filter')
    assert_equal 'bool', filter.keys.first
    assert_equal 2, filter.dig('bool', 'filter').size
  end

  def test_knn_top_level_with_similarity
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      similarity 0.8
    end
    assert_equal 0.8, c.to_query.dig('knn', 'similarity')
  end

  def test_knn_top_level_with_min_score
    c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      min_score 0.5
    end
    assert_equal 0.5, c.to_query['min_score']
    refute c.to_query['knn'].key?('min_score')
  end

  def test_knn_hybrid_with_query
    c = MockModel
          .knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10)
          .query { match :caption, 'sunset' }
    q = c.to_query
    assert q.key?('knn')
    assert q.key?('query')
    assert_equal({ 'match' => { 'caption' => 'sunset' } }, q['query'])
  end

  def test_knn_inline_in_query_block
    c = MockModel.query do
      knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
        filter { term 'platform', 'instagram' }
      end
    end
    knn = c.to_query.dig('query', 'knn')
    assert_equal 'image_embedding', knn['field']
    assert_equal({ 'term' => { 'platform' => 'instagram' } }, knn['filter'])
  end

  def test_knn_filter_rejects_match
    assert_raises(NoMethodError) do
      MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
        filter { match :caption, 'sunset' }
      end.to_query
    end
  end

  # ── BoolContext clause-array injection ────────────────────────────────────

  def test_bool_context_filter_accepts_clause_array
    # Build a criteria first, then inject its clauses into a knn filter
    c = MockModel.filter { active }.filter { term 'platform', 'instagram' }
    knn_c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter do
        bool do
          filter c.filter_clauses
        end
      end
    end
    inner_filter = knn_c.to_query.dig('knn', 'filter', 'bool', 'filter')
    assert_includes inner_filter, { 'term' => { 'status' => 'active' } }
    assert_includes inner_filter, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_bool_context_should_accepts_clause_array
    c = MockModel.should { term 'platform', 'instagram' }.should { term 'platform', 'tiktok' }
    knn_c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter do
        bool do
          should c.should_clauses
        end
      end
    end
    inner_should = knn_c.to_query.dig('knn', 'filter', 'bool', 'should')
    assert_includes inner_should, { 'term' => { 'platform' => 'instagram' } }
    assert_includes inner_should, { 'term' => { 'platform' => 'tiktok' } }
  end

  def test_bool_context_mixed_clause_arrays
    filter_criteria = MockModel.filter { active }
    should_criteria = MockModel.should { term 'type', 'video' }
    knn_c = MockModel.knn(:image_embedding, query_vector: VEC, k: 5, num_candidates: 10) do
      filter do
        bool do
          filter filter_criteria.filter_clauses
          should should_criteria.should_clauses
          minimum_should_match 1
        end
      end
    end
    bool_h = knn_c.to_query.dig('knn', 'filter', 'bool')
    assert_equal [{ 'term' => { 'status' => 'active' } }], bool_h['filter']
    assert_equal [{ 'term' => { 'type' => 'video' } }],   bool_h['should']
    assert_equal 1, bool_h['minimum_should_match']
  end

  def test_bool_context_must_and_must_not_accept_clause_arrays
    must_c     = MockModel.must     { term 'visible', true }
    must_not_c = MockModel.must_not { deleted }
    bc = Elasticsearch::Model::BoolContext.new
    bc.must(must_c.must_clauses)
    bc.must_not(must_not_c.must_not_clauses)
    h = bc.to_h['bool']
    assert_equal({ 'term' => { 'visible' => true } }, h['must'])
    assert_equal({ 'term' => { 'status' => 'deleted' } }, h['must_not'])
  end

  # ── Criteria top-level params ──────────────────────────────────────────────

  def test_track_total_hits_true
    c = MockModel.criteria.track_total_hits
    assert_equal true, c.to_query['track_total_hits']
  end

  def test_track_total_hits_false
    c = MockModel.criteria.track_total_hits(false)
    assert_equal false, c.to_query['track_total_hits']
  end

  def test_track_total_hits_integer
    c = MockModel.criteria.track_total_hits(5000)
    assert_equal 5000, c.to_query['track_total_hits']
  end

  def test_script_fields
    c = MockModel.criteria.script_fields(
      engagement: { script: { source: 'doc["likes"].value + doc["comments"].value' } }
    )
    sf = c.to_query['script_fields']
    assert sf.key?('engagement')
    assert sf.dig('engagement', 'script', 'source')
  end

  def test_sort_block
    c = MockModel.criteria.sort { { 'published_at' => { 'order' => 'desc' } } }
    assert_equal [{ 'published_at' => { 'order' => 'desc' } }], c.to_query['sort']
  end

  def test_sort_multiple_fields
    c = MockModel.criteria
                 .sort { { 'score' => 'desc' } }
                 .sort { { 'published_at' => 'asc' } }
    assert_equal 2, c.to_query['sort'].size
  end

  # ── Scope inside must / should / must_not ─────────────────────────────────

  def test_scope_in_must_block
    c = MockModel.must { active }
    assert_equal({ 'term' => { 'status' => 'active' } },
                 c.to_query.dig('query', 'bool', 'must'))
  end

  def test_scope_in_should_block
    c = MockModel.should { active }
                 .should { deleted }
    should_clauses = c.to_query.dig('query', 'bool', 'should')
    assert_includes should_clauses, { 'term' => { 'status' => 'active' } }
    assert_includes should_clauses, { 'term' => { 'status' => 'deleted' } }
  end

  def test_scope_in_must_not_block
    c = MockModel.must_not { deleted }
    assert_equal({ 'term' => { 'status' => 'deleted' } },
                 c.to_query.dig('query', 'bool', 'must_not'))
  end

  def test_scope_in_nested_bool_must
    c = MockModel.filter do
      bool do
        must { active }
        must { platform 'instagram' }
      end
    end
    filters     = c.to_query.dig('query', 'bool', 'filter')
    bool_clause = filters.find { |f| f['bool'] }
    assert bool_clause
    must = bool_clause.dig('bool', 'must')
    assert_instance_of Array, must
    assert_includes must, { 'term' => { 'status' => 'active' } }
    assert_includes must, { 'term' => { 'platform' => 'instagram' } }
  end

  # ── Parameterized scope ───────────────────────────────────────────────────

  def test_parameterized_scope_in_filter_block
    c = MockModel.filter { by_platform 'instagram' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'platform' => 'instagram' } }
  end

  def test_parameterized_scope_with_explicit_f
    c = MockModel.filter { |f| f.by_platform('tiktok') }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'platform' => 'tiktok' } }
  end

  def test_parameterized_scope_range
    c = MockModel.filter { followers_gte 5000 }
    filters = c.to_query.dig('query', 'bool', 'filter')
    range = filters.find { |f| f['range'] }
    assert range
    assert_equal 5000, range.dig('range', 'follower_count', 'gte')
  end

  def test_parameterized_scope_chained_with_non_param_scope
    c = MockModel.filter { active }.filter { by_platform 'youtube' }
    filters = c.to_query.dig('query', 'bool', 'filter')
    assert_includes filters, { 'term' => { 'status' => 'active' } }
    assert_includes filters, { 'term' => { 'platform' => 'youtube' } }
  end

  # ── Parameterized agg_scope ──────────────────────────────────────────────

  def test_parameterized_agg_scope_basic
    aggs = MockModel.by_field('platform').to_query['aggregations']
    assert aggs.key?('by_field')
    assert_equal({ 'field' => 'platform', 'size' => 10 }, aggs.dig('by_field', 'terms'))
  end

  def test_parameterized_agg_scope_keyword_arg_override
    aggs = MockModel.by_field('team', size: 20).to_query['aggregations']
    assert_equal({ 'field' => 'team', 'size' => 20 }, aggs.dig('by_field', 'terms'))
  end

  def test_parameterized_agg_scope_multiple_aggs
    aggs = MockModel.top_by_field('platform').to_query['aggregations']
    assert aggs.key?('top')
    assert aggs.key?('terms_agg')
    assert_equal({ 'field' => 'platform' }, aggs.dig('terms_agg', 'terms'))
  end

  def test_parameterized_agg_scope_chained_with_other_agg_scopes
    aggs = MockModel.by_field('status').group_by_team.to_query['aggregations']
    assert aggs.key?('by_field')
    assert aggs.key?('teams')
  end

  def test_parameterized_agg_scope_with_call_time_sub_agg
    aggs = MockModel.by_field('platform') do |ab|
      ab.aggregate(:reach) { sum field: 'reach' }
    end.to_query['aggregations']
    assert_equal({ 'field' => 'platform', 'size' => 10 }, aggs.dig('by_field', 'terms'))
    assert_equal({ 'field' => 'reach' }, aggs.dig('by_field', 'aggs', 'reach', 'sum'))
  end

  # ── Nested agg_scope (composite with scope sources) ───────────────────────

  def test_agg_scope_composite_sources_via_scope
    aggs = MockModel.aggregate(:by_combo) do |a|
      a.composite do
        size 5
        sources do
          by_platform_source
          by_status_source
        end
      end
    end.to_query['aggregations']
    sources = aggs.dig('by_combo', 'composite', 'sources')
    assert_equal 2, sources.size
    assert sources.any? { |s| s.key?('platform') }
    assert sources.any? { |s| s.key?('status_source') }
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

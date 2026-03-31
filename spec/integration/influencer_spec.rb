# frozen_string_literal: true

# spec/integration/influencer_spec.rb
#
# Integration tests against a real Elasticsearch node via Testcontainers.
# Requires Docker to be running.
#
# Run:
#   bundle exec ruby spec/integration/influencer_spec.rb
#
# Skip gracefully if Docker / testcontainers is unavailable.

$LOAD_PATH.unshift File.expand_path('../../lib', __dir__)

require 'minitest/autorun'
require 'elasticsearch/model'
require 'date'
require 'net/http'
require 'uri'
require 'json'

# ── Container bootstrap ───────────────────────────────────────────────────────

begin
  require 'testcontainers/elasticsearch'

  CONTAINER = Testcontainers::ElasticsearchContainer.new('elasticsearch:8.13.4')
                .with_env('xpack.security.enabled', 'false')
                .start
  ES_URL = CONTAINER.elasticsearch_url
  Minitest.after_run { CONTAINER.stop rescue nil }

rescue LoadError
  warn 'testcontainers-elasticsearch gem not installed — run: bundle install'
  exit 0
rescue => e
  warn "Could not start Elasticsearch container (Docker running?): #{e.message}"
  exit 0
end

# ── Client configuration ──────────────────────────────────────────────────────

Elasticsearch::Model.configure do |c|
  c.url             = ES_URL
  c.log             = false
  c.request_timeout = 30
end

# ── Admin helper (index management — not part of the ODM client) ──────────────

class ESAdmin
  def initialize(base_url)
    @base_url = base_url
  end

  def create_index(name, body)
    request(:put, "/#{name}", body)
  end

  def delete_index(name)
    request(:delete, "/#{name}", nil)
  end

  def index_doc(index, id, body)
    request(:put, "/#{index}/_doc/#{id}", body)
  end

  def refresh(index)
    request(:post, "/#{index}/_refresh", nil)
  end

  def create_alias(name, *indices)
    actions = indices.map { |idx| { add: { index: idx, alias: name } } }
    request(:post, '/_aliases', { actions: actions })
  end

  def refresh_all(*indices)
    indices.each { |idx| refresh(idx) }
  end

  private

  def request(method, path, body)
    uri = URI.parse("#{@base_url}#{path}")
    http = Net::HTTP.new(uri.host, uri.port)

    klass = { put: Net::HTTP::Put, post: Net::HTTP::Post, delete: Net::HTTP::Delete }[method]
    req = klass.new(uri.request_uri)
    req['Content-Type'] = 'application/json'
    req['Accept']       = 'application/json'
    req.body = body.to_json if body

    JSON.parse(http.request(req).body)
  end
end

ADMIN = ESAdmin.new(ES_URL)

# ── Model under test ──────────────────────────────────────────────────────────

class Influencer
  include Elasticsearch::Model::Searchable

  index_name 'test_influencers'

  scope :active do
    term 'status', 'active'
  end

  scope :on_instagram do
    term 'platform', 'instagram'
  end
end

# ── Shared fixtures ───────────────────────────────────────────────────────────

INDEX   = 'test_influencers'
MAPPING = {
  mappings: {
    properties: {
      name:           { type: 'text' },
      platform:       { type: 'keyword' },
      status:         { type: 'keyword' },
      follower_count: { type: 'integer' },
      published_at:   { type: 'date' }
    }
  }
}.freeze

DOCS = [
  { id: 1, name: 'john doe',   platform: 'instagram', status: 'active',   follower_count: 10_000, published_at: '2025-03-01' },
  { id: 2, name: 'jane smith', platform: 'tiktok',    status: 'active',   follower_count: 50_000, published_at: '2025-06-15' },
  { id: 3, name: 'bob jones',  platform: 'instagram', status: 'inactive', follower_count:  5_000, published_at: '2024-09-10' },
  { id: 4, name: 'alice wong', platform: 'youtube',   status: 'active',   follower_count: 100_000, published_at: '2025-01-20' }
].freeze

# ── Test cases ────────────────────────────────────────────────────────────────

class InfluencerIntegrationTest < Minitest::Test
  def setup
    ADMIN.create_index(INDEX, MAPPING)
    DOCS.each { |doc| ADMIN.index_doc(INDEX, doc[:id], doc.reject { |k, _| k == :id }) }
    ADMIN.refresh(INDEX)
  end

  def teardown
    ADMIN.delete_index(INDEX)
  end

  # ── Basic queries ─────────────────────────────────────────────────────────

  def test_match_all
    results = Influencer.criteria.search
    assert_equal 4, results.total
  end

  def test_filter_by_term
    results = Influencer.filter { term 'platform', 'instagram' }.search
    assert_equal 2, results.total
  end

  def test_must_clause
    results = Influencer.must { term 'platform', 'tiktok' }.search
    assert_equal 1, results.total
    assert_equal 'jane smith', results.sources.first['name']
  end

  def test_must_not_clause
    results = Influencer.must_not { term 'status', 'inactive' }.search
    assert_equal 3, results.total
  end

  def test_should_clause
    results = Influencer
      .should { term 'platform', 'instagram' }
      .should { term 'platform', 'tiktok' }
      .search
    assert results.total >= 3
  end

  # ── Scopes ────────────────────────────────────────────────────────────────

  def test_scope_active
    results = Influencer.filter { active }.search
    assert_equal 3, results.total
  end

  def test_scope_chaining
    results = Influencer.filter { active }.filter { on_instagram }.search
    assert_equal 1, results.total
    assert_equal 'john doe', results.sources.first['name']
  end

  def test_scope_inside_query_block
    results = Influencer.query { filter { active } }.search
    assert_equal 3, results.total
  end

  # ── DSL helpers ───────────────────────────────────────────────────────────

  def test_date_range
    results = Influencer
      .query { date_range :published_at, from: '2025-01-01', to: '2025-12-31' }
      .search
    assert_equal 3, results.total
  end

  def test_smart_match
    results = Influencer.query { smart_match :name, 'john' }.search
    assert results.total >= 1
    assert_includes results.sources.map { |s| s['name'] }, 'john doe'
  end

  def test_filter_terms_multi
    results = Influencer
      .query { filter_terms :platform, %w[instagram youtube] }
      .search
    assert_equal 3, results.total
  end

  # ── Aggregations ──────────────────────────────────────────────────────────

  def test_terms_aggregation
    results = Influencer.criteria.size(0)
      .aggregate(:by_platform) { terms field: 'platform' }
      .search

    buckets = results.aggregations.dig('by_platform', 'buckets')
    refute_nil buckets
    assert_includes buckets.map { |b| b['key'] }, 'instagram'
    assert_includes buckets.map { |b| b['key'] }, 'tiktok'
  end

  def test_avg_aggregation
    results = Influencer.criteria.size(0)
      .aggregate(:avg_followers) { avg field: 'follower_count' }
      .search

    avg_val = results.aggregations.dig('avg_followers', 'value')
    refute_nil avg_val
    assert avg_val > 0
  end

  # ── Pagination ────────────────────────────────────────────────────────────

  def test_pagination_no_overlap
    page1 = Influencer.criteria.from(0).size(2).search
    page2 = Influencer.criteria.from(2).size(2).search

    assert_equal 2, page1.size
    assert_equal 2, page2.size
    assert_equal 4, page1.total

    names1 = page1.sources.map { |s| s['name'] }
    names2 = page2.sources.map { |s| s['name'] }
    assert_empty names1 & names2
  end

  def test_source_fields
    results = Influencer.criteria.source('name', 'platform').search
    first = results.sources.first
    assert first.key?('name')
    assert first.key?('platform')
    refute first.key?('follower_count')
  end
end

# ── Routed model ──────────────────────────────────────────────────────────────

class RoutedInfluencer
  include Elasticsearch::Model::Searchable

  index_name 'test_routed_alias'

  def self.search_index(criteria = nil)
    range = criteria&.date_filter_for(:published_at)
    return index_name unless range

    gte_year = range['gte'] ? (Date.parse(range['gte']).year rescue nil) : nil
    lte_year = range['lte'] ? (Date.parse(range['lte']).year rescue nil) : nil
    year     = gte_year || lte_year
    return index_name unless year

    # Span multiple years → search alias (all indices)
    return index_name if gte_year && lte_year && gte_year != lte_year

    "test_routed_#{year}"
  end
end

ROUTED_MAPPING = {
  mappings: {
    properties: {
      name:         { type: 'keyword' },
      published_at: { type: 'date' }
    }
  }
}.freeze

# ── Search-index routing tests ────────────────────────────────────────────────

class InfluencerRoutingTest < Minitest::Test
  IDX_2025  = 'test_routed_2025'
  IDX_2026  = 'test_routed_2026'
  ALIAS     = 'test_routed_alias'

  def setup
    ADMIN.create_index(IDX_2025, ROUTED_MAPPING)
    ADMIN.create_index(IDX_2026, ROUTED_MAPPING)
    ADMIN.create_alias(ALIAS, IDX_2025, IDX_2026)

    ADMIN.index_doc(IDX_2025, 1, { name: 'alice', published_at: '2025-06-01' })
    ADMIN.index_doc(IDX_2025, 2, { name: 'bob',   published_at: '2025-09-15' })
    ADMIN.index_doc(IDX_2026, 3, { name: 'carol', published_at: '2026-01-10' })
    ADMIN.refresh(IDX_2025)
    ADMIN.refresh(IDX_2026)
  end

  def teardown
    ADMIN.delete_index(IDX_2025)
    ADMIN.delete_index(IDX_2026)
  end

  def test_no_date_filter_searches_alias
    results = RoutedInfluencer.criteria.search
    assert_equal 3, results.total
  end

  def test_2025_filter_routes_to_2025_index
    results = RoutedInfluencer
      .query { date_range :published_at, from: '2025-01-01', to: '2025-12-31' }
      .search
    assert_equal 2, results.total
    names = results.sources.map { |s| s['name'] }
    assert_includes names, 'alice'
    assert_includes names, 'bob'
    refute_includes names, 'carol'
  end

  def test_2026_filter_routes_to_2026_index
    results = RoutedInfluencer
      .query { date_range :published_at, from: '2026-01-01', to: '2026-12-31' }
      .search
    assert_equal 1, results.total
    assert_equal 'carol', results.sources.first['name']
  end

  def test_cross_year_filter_searches_alias
    results = RoutedInfluencer
      .query { date_range :published_at, from: '2025-01-01', to: '2026-12-31' }
      .search
    assert_equal 3, results.total
  end
end

# ── Point-in-Time pagination tests ────────────────────────────────────────────

PIT_INDEX   = 'test_pit_influencers'
PIT_MAPPING = {
  mappings: {
    properties: {
      name: { type: 'keyword' },
      seq:  { type: 'integer' }
    }
  }
}.freeze
PIT_TOTAL = 5

class InfluencerPitTest < Minitest::Test
  def setup
    ADMIN.create_index(PIT_INDEX, PIT_MAPPING)
    PIT_TOTAL.times do |i|
      ADMIN.index_doc(PIT_INDEX, i + 1, { name: "doc_#{i + 1}", seq: i + 1 })
    end
    ADMIN.refresh(PIT_INDEX)
  end

  def teardown
    ADMIN.delete_index(PIT_INDEX)
  end

  def test_search_pit_retrieves_all_docs
    all_names = []
    pages     = 0
    pit_model.criteria.sort { { 'seq' => 'asc' } }.search_pit(page_size: 2) do |response, _total|
      pages += 1
      all_names.concat(response.sources.map { |s| s['name'] })
    end

    assert_equal PIT_TOTAL, all_names.size
    assert_equal PIT_TOTAL.times.map { |i| "doc_#{i + 1}" }.sort, all_names.sort
    assert_equal 3, pages  # 2 + 2 + 1
  end

  def test_search_pit_without_block_returns_all_pages
    pages = pit_model.criteria.sort { { 'seq' => 'asc' } }.search_pit(page_size: 2)
    assert_equal 3, pages.size
    assert_equal PIT_TOTAL, pages.sum { |p| p.sources.size }
  end

  def test_search_pit_early_stop
    count = 0
    pit_model.criteria.sort { { 'seq' => 'asc' } }.search_pit(page_size: 2) do |response, _total|
      count += response.sources.size
      false  # stop after first page
    end
    assert_equal 2, count
  end

  private

  def pit_model
    @pit_model ||= Class.new do
      include Elasticsearch::Model::Searchable
      index_name PIT_INDEX
    end
  end
end

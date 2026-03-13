# frozen_string_literal: true

# example/brand_content.rb

require_relative '../lib/elasticsearch/model'

# ── Model ─────────────────────────────────────────────────────────────────────

class BrandContent
  include Elasticsearch::Model::Searchable

  index_name 'brand_contents'

  field :content_id,      type: String
  field :team_id,         type: String
  field :platform,        type: String
  field :content_type,    type: String
  field :followers_tier,  type: String
  field :published_at,    type: DateTime
  field :paid,            type: :boolean
  field :topics,          type: String
  field :sentiment_score, type: Float

  # ── Custom response ──────────────────────────────────────────────────────────

  class Response < Elasticsearch::Model::ElasticsearchResponse
    def content_ids
      sources.map { |s| s['content_id'] }
    end

    def influencer_ids_from_agg
      agg('top_influencers')&.dig('buckets')&.map { |b| b['key'] } || []
    end

    def timeline_buckets
      agg('timeline')&.fetch('buckets', []) || []
    end
  end

  # ── Dynamic routing ──────────────────────────────────────────────────────────

  def self.search_index(criteria = nil)
    range = criteria&.date_filter_for(:published_at)
    return index_name unless range

    begin
      year = Date.parse(range['gte'] || range['lte']).year
      "content_#{year}"
    rescue ArgumentError, TypeError
      index_name
    end
  end

  # ── Named filter helpers ─────────────────────────────────────────────────────
  # Available inside filter {} and query {} blocks via FilterCollector / QueryContext.
  # Also used by BrandPerformanceSearch to delegate filter logic.

  module QueryFilter
    def team_id(id)
      term 'team_id', id.to_s
    end

    def team_ids(ids)
      terms 'team_id', ids.map(&:to_s)
    end

    def content_ids(ids)
      terms 'content_id', ids
    end

    def user_ids(ids)
      terms 'user_id', ids
    end

    def influencer_ids(ids)
      terms 'influencer.id', ids.map(&:to_s)
    end

    def ambassador_types(types)
      terms 'influencer.ambassador_type', types
    end

    def platforms(values)
      terms 'platform', values
    end

    def content_types(values)
      terms 'content_type', values
    end

    def followers_tiers(values)
      terms 'followers_tier', values
    end

    def has_topics
      exists 'topics'
    end

    def topics(values)
      terms 'topics', values
    end

    def paid(value)
      term 'paid', value
    end

    def activity_ids(ids)
      terms 'activity_ids', ids
    end

    # Date range helpers — push to the non-scoring filter buffer via date_range.
    def start_date(date)
      date_range :published_at, from: date
    end

    def end_date(date)
      date_range :published_at, to: date
    end

    # Sentiment filter — builds a bool/should across positive/neutral/negative buckets.
    def sentiments(values)
      clauses = Array(values).filter_map { |s| sentiment_clause(s) }
      return if clauses.empty?

      if clauses.size == 1
        raw clauses.first
      else
        raw(bool: { should: clauses, minimum_should_match: 1 })
      end
    end

    # Country filter — terms match, with optional "other" (missing field) bucket.
    def countries(values)
      should = [{ terms: { 'influencer.country' => Array(values) } }]
      if values.any? { |v| v.nil? || v.to_s.downcase == 'other' }
        should << { bool: { must_not: { exists: { field: 'influencer.country' } } } }
      end
      raw(bool: { should: should, minimum_should_match: 1 })
    end

    private

    def sentiment_clause(s)
      case s.to_s
      when 'positive' then { range: { sentiment_score: { gt: 0 } } }
      when 'neutral'  then { term:  { sentiment_score: 0 } }
      when 'negative' then { range: { sentiment_score: { lt: 0 } } }
      end
    end
  end
end

# ── BrandPerformanceSearch ────────────────────────────────────────────────────────
# Fluent query builder. Accumulates state; builds and executes via Criteria.

class BrandPerformanceSearch
  CONTENT_SIZE        = 12
  MAX_INFLUENCERS_NUM = 10_000
  MAX_TOPICS_NUM      = 10_000
  MAX_ACTIVITY_NUM    = 10_000
  TOP_INFLUENCER_SIZE = 30
  TOP_ACTIVITY_SIZE   = 10
  TOP_TEAM_SIZE       = 10
  SHARD_SIZE_FACTOR   = 50

  class TeamRequiredError < StandardError
    def initialize
      super('Team is required for custom metrics and aggregations')
    end
  end

  def initialize
    @track_total_hits     = false
    @sort_by              = 'published_at'
    @sort_order           = 'desc'
    @from                 = 0
    @size                 = CONTENT_SIZE
    @additional_sources   = []
    @aggregations         = {}
    @influencer_size      = TOP_INFLUENCER_SIZE
    @activity_size        = TOP_ACTIVITY_SIZE
    @team_size            = TOP_TEAM_SIZE
    @activity_ids         = nil
    @organic_activity_ids = nil
    @content_scopes       = nil
    @_finalized           = false

    @criteria = BrandContent.criteria
  end

  # ── Setter API ───────────────────────────────────────────────────────────────

  def team(team)
    @criteria.team_id(team.id) if team.respond_to?(:id)
    self
  end

  def team_ids(ids)
    @criteria.team_ids(ids) if valid?(ids)
    self
  end

  def content_ids(ids)
    @criteria.content_ids(ids) if valid?(ids)
    self
  end

  def user_ids(ids)
    @criteria.user_ids(ids) if valid?(ids)
    self
  end

  def platforms(values)
    @criteria.platforms(values) if valid?(values)
    self
  end

  def content_types(values)
    @criteria.content_types(values) if valid?(values)
    self
  end

  def followers_tiers(values)
    @criteria.followers_tiers(values) if valid?(values)
    self
  end

  def start_date(date)
    @criteria.start_date(date)
    self
  end

  def end_date(date)
    @criteria.end_date(date)
    self
  end

  def paid(value)
    @criteria.paid(value) unless value.nil?
    self
  end

  def influencer_ids(ids)
    @criteria.influencer_ids(ids) if valid?(ids)
    self
  end

  def countries(values)
    @criteria.countries(values) if valid?(values)
    self
  end

  def ambassador_types(types)
    @criteria.ambassador_types(types) if valid?(types)
    self
  end

  def content_scopes(scopes, activity_ids: nil, organic_activity_ids: nil)
    if valid?(activity_ids)
      @criteria.activity_ids(activity_ids)
      @activity_ids = activity_ids   # kept for content_scope_clause logic
    end
    @organic_activity_ids = organic_activity_ids if valid?(organic_activity_ids)
    @content_scopes       = scopes               if valid?(scopes)
    self
  end

  def has_topics
    @criteria.has_topics
    self
  end

  def topics(values)
    @criteria.topics(values) if valid?(values)
    self
  end

  def sentiments(values)
    @criteria.sentiments(values) if valid?(values)
    self
  end

  def sort_by(field)
    @sort_by = field.to_s
    self
  end

  def sort_order(order)
    @sort_order = order.to_s if %w[asc desc].include?(order.to_s)
    self
  end

  def from(n)
    @from = n.to_i if n
    self
  end

  def size(n)
    @size = n
    self
  end

  def additional_sources(fields)
    @additional_sources = fields if valid?(fields)
    self
  end

  def influencer_size(n)
    @influencer_size = n.to_i if n
    self
  end

  def activity_size(n)
    @activity_size = n.to_i if n
    self
  end

  def team_size(n)
    @team_size = n.to_i if n
    self
  end

  # ── Aggregation builder methods ───────────────────────────────────────────────

  def aggregate_influencers(agg_metrics: nil, custom_aggs: {})
    aggs = { my_influencer: { terms: { field: 'influencer.my_influencer' } } }
           .merge(metrics_aggs(agg_metrics))

    @aggregations[:top_influencers] = {
      terms: {
        field:      'influencer.id',
        size:       @influencer_size,
        shard_size: @influencer_size * SHARD_SIZE_FACTOR,
        order:      { @sort_by => @sort_order }
      },
      aggs: aggs.merge(custom_aggs)
    }
    self
  end

  def aggregate_top_activities(agg_metrics: nil)
    @aggregations[:top_activities] = {
      terms: {
        script:     all_activity_ids_script,
        size:       @activity_size,
        shard_size: @activity_size * SHARD_SIZE_FACTOR,
        order:      { @sort_by => @sort_order }
      },
      aggs: metrics_aggs(agg_metrics)
    }
    self
  end

  def aggregate_teams
    @aggregations[:top_teams] = {
      terms: {
        field:      'team_id',
        size:       @team_size,
        shard_size: @team_size * SHARD_SIZE_FACTOR
      }
    }
    self
  end

  def aggregate_timeline(interval: '1d', agg_metrics: nil, custom_aggs: {})
    @aggregations[:timeline] = {
      date_histogram: {
        field:             'published_at',
        calendar_interval: interval,
        min_doc_count:     0
      },
      aggs: metrics_aggs(agg_metrics).merge(custom_aggs)
    }
    self
  end

  def aggregate_topics
    @aggregations[:topics] = {
      terms: { field: 'topics', size: MAX_TOPICS_NUM },
      aggs:  metrics_aggs
    }
    self
  end

  def aggregate_activity_organic(agg_metrics: nil)
    @aggregations[:activity_organic_distribution] = {
      filters: {
        filters: {
          activity: { exists: { field: 'activity_ids' } },
          organic:  { bool: { must_not: { exists: { field: 'activity_ids' } } } }
        }
      },
      aggs: {
        activity_ids: { terms: { field: 'activity_ids', size: @activity_size } }
      }.merge(metrics_aggs(agg_metrics))
    }
    self
  end

  def aggregate_by_custom_aggs(custom_aggs)
    @aggregations.merge!(custom_aggs)
    self
  end

  # ── Execution ─────────────────────────────────────────────────────────────────

  def search(timeout: nil)
    criteria.search(timeout: timeout)
  end

  def search_pit(page_size: 1000, keep_alive: nil, timeout: nil, &block)
    criteria.search_pit(page_size: page_size, keep_alive: keep_alive,
                        timeout: timeout, &block)
  end

  def to_query
    criteria.to_query
  end

  alias build to_query

  # ── Criteria finalizer ────────────────────────────────────────────────────────
  # Filters are applied to @criteria immediately via setters.
  # Sort / source / pagination / aggregations are applied once on first execution.

  def criteria
    unless @_finalized
      @_finalized = true

      sort_clause = { @sort_by => { 'order' => @sort_order } }
      @criteria.sort { sort_clause }
      @criteria.source('content_id', 'team_id', 'platform', *@additional_sources)
      @criteria.from(@from).size(@size)

      add_content_scope_filters(@criteria) if @content_scopes

      @aggregations.each { |name, raw| @criteria.aggregate(name, raw) }
    end

    @criteria
  end

  private

  def add_content_scope_filters(c)
    clauses = @content_scopes.filter_map { |s| content_scope_clause(s) }
    return if clauses.empty?

    c.filter { bool(should: clauses, minimum_should_match: 1) }
  end

  def content_scope_clause(scope)
    case scope.to_s
    when 'activity'
      ids_clause = @activity_ids ? { terms: { activity_ids: @activity_ids } } : { exists: { field: 'activity_ids' } }
      { bool: { filter: [ids_clause] } }
    when 'organic_by_activity'
      ids_clause = @organic_activity_ids ? { terms: { organic_activity_ids: @organic_activity_ids } } : { exists: { field: 'organic_activity_ids' } }
      { bool: { filter: [ids_clause] } }
    when 'my'
      {
        bool: {
          filter:   { term: { 'influencer.my_influencer' => true } },
          must_not: [
            { exists: { field: 'activity_ids' } },
            { exists: { field: 'organic_activity_ids' } }
          ]
        }
      }
    when 'other'
      {
        bool: {
          filter:   { term: { 'influencer.my_influencer' => false } },
          must_not: [
            { exists: { field: 'activity_ids' } },
            { exists: { field: 'organic_activity_ids' } }
          ]
        }
      }
    end
  end

  # ── Aggregation helpers ───────────────────────────────────────────────────────

  def metrics_aggs(agg_metrics = nil)
    aggs = {}
    return aggs if agg_metrics&.empty?

    want = ->(m) { agg_metrics.nil? || agg_metrics.include?(m) }

    aggs[:influencers] = { cardinality: { field: 'influencer.id' } } if want.call(:influencers)
    aggs[:reach]       = { sum: { field: 'reach' } }                  if want.call(:reach)
    aggs[:impressions] = { sum: { field: 'impressions' } }            if want.call(:impressions)
    aggs[:views]       = { sum: { field: 'views' } }                  if want.call(:views)
    aggs
  end

  def all_activity_ids_script
    { id: 'all_activity_ids' }
  end

  def valid?(arr)
    arr.is_a?(Array) && arr.any?
  end
end

# ── Quick smoke test ──────────────────────────────────────────────────────────
if $PROGRAM_NAME == __FILE__
  require 'pp'

  s = BrandPerformanceSearch.new
  s.platforms(['instagram', 'tiktok'])
   .start_date('2025-01-01')
   .end_date('2025-12-31')
   .size(12)
   .aggregate_influencers
   .aggregate_timeline(interval: '1M')

  pp s.to_query
end

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

# frozen_string_literal: true

require_relative '../../lib/elasticsearch/model'

class InfluencerModel
  include Elasticsearch::Model::Searchable
  index_name 'influencers'

  PLATFORMS = %w[instagram tiktok twitter weibo youtube facebook red twitch douyin].freeze

  # ── Filter scopes ────────────────────────────────────────────────────────────

  scope :verified_only do
    term :verified, true
  end

  scope :not_brand do
    bool do
      must_not { term :is_brand, true }
    end
  end

  scope :by_ids do |ids|
    terms :_id, ids
  end

  scope :by_team_ids do |ids|
    terms :team_ids, ids
  end

  scope :exclude_team_ids do |ids|
    bool do
      must_not { terms :team_ids, ids }
    end
  end

  scope :exclude_ids do |ids|
    bool do
      must_not { terms :_id, ids }
    end
  end

  scope :by_city do |city|
    term 'profile.current_city', city.downcase
  end

  scope :by_genders do |genders|
    terms 'profile.gender', genders
  end

  scope :by_roles do |roles|
    terms 'profile.roles', roles
  end

  scope :by_age_groups do |groups|
    terms 'profile.age_group', groups
  end

  scope :by_audience_genders do |genders|
    terms 'audience.gender', genders
  end

  scope :by_audience_interests do |interests|
    terms 'audience.interest', interests
  end

  scope :platform_exists do |platform|
    exists platform
  end

  # Matches influencers present on at least one of the given platforms.
  scope :any_platforms do |platforms|
    bool do
      platforms.each { |p| should { exists p } }
      minimum_should_match 1
    end
  end

  # Matches combined categories and sub_categories in a single OR clause.
  scope :categories do |values|
    bool do
      should { terms 'profile.categories', values }
      should { terms 'profile.sub_categories', values }
      minimum_should_match 1
    end
  end

  scope :min_reach do |platform, min|
    range "#{platform}.reach", gte: min
  end

  scope :max_reach do |platform, max|
    range "#{platform}.reach", lte: max
  end

  scope :min_engagement_rate do |platform, rate|
    range "#{platform}.engagement_rate", gte: rate
  end

  scope :max_engagement_rate do |platform, rate|
    range "#{platform}.engagement_rate", lte: rate
  end

  # At least one platform must meet the minimum reach threshold.
  scope :global_min_reach do |min|
    bool do
      PLATFORMS.each { |p| should { range "#{p}.reach", gte: min } }
      minimum_should_match 1
    end
  end

  # audience_countries: supports 'Other' to match documents without a country.
  scope :audience_countries do |countries|
    regular = countries - ['Other']
    clauses = []
    clauses << { 'terms' => { 'audience.country' => regular } } if regular.any?
    if countries.include?('Other')
      clauses << { 'bool' => { 'must_not' => { 'exists' => { 'field' => 'audience.country' } } } }
    end
    raw(bool: { should: clauses, minimum_should_match: 1 })
  end

  # Nested tags filter: matches influencers tagged with all given tag values (within a team).
  scope :by_tags do |tags, team_id: nil|
    filters = []
    filters << { 'term' => { 'tags.team_id' => team_id.to_s } } if team_id
    filters += tags.map { |t| { 'term' => { 'tags.values' => t } } }
    raw(nested: { path: 'tags', score_mode: 'none', query: { bool: { filter: filters } } })
  end

  # ── Aggregation scopes ───────────────────────────────────────────────────────

  # Builds a country terms aggregation with per-platform filter sub-aggs.
  agg_scope :group_by_country_platform do |agg|
    platform_filters = PLATFORMS.each_with_object({}) do |platform, hash|
      hash[platform] = { bool: { must: [{ exists: { field: platform } }] } }
    end

    agg.aggregate('group_by_current_country') do
      terms field: 'profile.current_country', size: 1000
      aggregate(:platforms) do |sub|
        sub.filters(*PLATFORMS.map(&:to_sym)) do |name|
          raw(bool: { must: [{ exists: { field: name.to_s } }] })
        end
      end
    end
  end
end

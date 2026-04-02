# frozen_string_literal: true

require_relative '../models/influencer'

# Fluent search object for influencer queries.
# Each method applies a filter (or sort/pagination setting) to the criteria
# and returns self, mirroring the original QueryBuilder interface.
#
# String normalisation (to_category / to_sub_category) is intentionally
# omitted here — add it in each method if needed.
class InfluencerSearch
  attr_reader :criteria

  def initialize
    @criteria = InfluencerModel.criteria
  end

  # ── Pagination / source ──────────────────────────────────────────────────────

  def from(from)
    @criteria = @criteria.from(from)
    self
  end

  def size(size)
    @criteria = @criteria.size(size)
    self
  end

  def track_total_hits(value)
    @criteria = @criteria.track_total_hits(value)
    self
  end

  # ── Identity filters ─────────────────────────────────────────────────────────

  def influencer_ids(ids)
    return self unless ids&.any?

    @criteria = @criteria.filter { by_ids ids }
    self
  end

  def excluded_influencer_ids(ids)
    return self unless ids&.any?

    @criteria = @criteria.filter { exclude_ids ids }
    self
  end

  # ── Team / scope filters ─────────────────────────────────────────────────────

  def team_ids(ids)
    return self unless ids&.any?

    @criteria = @criteria.filter { by_team_ids ids }
    self
  end

  def excluded_team_ids(ids)
    return self unless ids&.any?

    @criteria = @criteria.filter { exclude_team_ids ids }
    self
  end

  # ── Account attribute filters ────────────────────────────────────────────────

  def verified_only
    @criteria = @criteria.filter { verified_only }
    self
  end

  def exclude_brand_accounts
    @criteria = @criteria.filter { not_brand }
    self
  end

  def platform(platform)
    return self unless InfluencerModel::PLATFORMS.include?(platform)

    @criteria = @criteria.filter { platform_exists platform }
    self
  end

  def any_platforms(platforms)
    return self unless platforms&.any?

    @criteria = @criteria.filter { any_platforms platforms }
    self
  end

  # ── Profile filters ──────────────────────────────────────────────────────────

  def countries(countries)
    return self unless countries&.any?

    values = countries.compact
    # Simplified: excludes the 'Other' edge-case handling from the original.
    @criteria = @criteria.filter { terms 'profile.current_country', values }
    self
  end

  def city(city)
    return self unless city&.strip&.length&.positive?

    @criteria = @criteria.filter { by_city city }
    self
  end

  def genders(genders)
    return self unless genders&.any?

    @criteria = @criteria.filter { by_genders genders }
    self
  end

  def roles(roles)
    return self unless roles&.any?

    @criteria = @criteria.filter { by_roles roles }
    self
  end

  def age_groups(groups)
    return self unless groups&.any?

    @criteria = @criteria.filter { by_age_groups groups }
    self
  end

  # ── Category filters ─────────────────────────────────────────────────────────

  # Accepts a single category + optional sub_category.
  # String normalisation is intentionally skipped — add to_category / to_sub_category here.
  def category(category, sub_category = nil)
    combined = [category, sub_category].compact
    return self unless combined.any?

    @criteria = @criteria.filter { categories combined }
    self
  end

  # Accepts pre-built combined array of category + sub_category strings.
  def categories(values)
    return self unless values&.any?

    @criteria = @criteria.filter { categories values }
    self
  end

  # ── Reach / engagement filters ───────────────────────────────────────────────

  def min_reach(platform, min)
    return self unless min

    @criteria = @criteria.filter { min_reach platform, min }
    self
  end

  def max_reach(platform, max)
    return self unless max

    @criteria = @criteria.filter { max_reach platform, max }
    self
  end

  def global_min_reach(min)
    return self unless min

    @criteria = @criteria.filter { global_min_reach min }
    self
  end

  def min_engagement_rate(platform, rate)
    return self unless rate

    @criteria = @criteria.filter { min_engagement_rate platform, rate }
    self
  end

  def max_engagement_rate(platform, rate)
    return self unless rate

    @criteria = @criteria.filter { max_engagement_rate platform, rate }
    self
  end

  # ── Audience filters ─────────────────────────────────────────────────────────

  def audience_genders(genders)
    return self unless genders&.any?

    @criteria = @criteria.filter { by_audience_genders genders }
    self
  end

  def audience_ages(ages)
    return self unless ages&.any?

    @criteria = @criteria.filter { terms 'audience.age', ages }
    self
  end

  def audience_countries(countries)
    return self unless countries&.any?

    @criteria = @criteria.filter { audience_countries countries }
    self
  end

  def audience_interests(interests)
    return self unless interests&.any?

    @criteria = @criteria.filter { by_audience_interests interests }
    self
  end

  # ── Tag filter ───────────────────────────────────────────────────────────────

  def tags(tags, team_id: nil)
    return self unless tags&.any?

    @criteria = @criteria.filter { by_tags tags, team_id: team_id }
    self
  end

  # ── Sort ─────────────────────────────────────────────────────────────────────

  def sort_by_relevance
    @criteria = @criteria.sort { { '_score' => 'desc' } }
    self
  end

  def sort_by(platform, field)
    @criteria = @criteria.sort { { "#{platform}.#{field}" => 'desc' } }
    self
  end

  # ── Execution ────────────────────────────────────────────────────────────────

  def search
    @criteria.search
  end
end

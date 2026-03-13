# frozen_string_literal: true
# spec/influencer_example.rb
#
# Run with:   ruby spec/influencer_example.rb
# Requires a running Elasticsearch at localhost:9200

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'elasticsearch/model'

# ── 1. Global configuration ────────────────────────────────────────────────────
Elasticsearch::Model.configure do |config|
  config.url             = ENV.fetch('ES_URL', 'http://localhost:9200')
  config.log             = true
  config.request_timeout = 60          # global default (seconds)
  config.pit_keep_alive  = '2m'
end

# ── 2. Define a model ─────────────────────────────────────────────────────────
class Influencer
  include Elasticsearch::Model::Searchable

  index_name 'influencers'   # default alias / index

  # Dynamic index routing:
  # - If the query contains a date range on `published_at`, route to
  #   the year-specific index (content_2024, content_2025 …).
  # - Otherwise fall back to the alias that covers all years.
  def self.search_index(criteria = nil)
    return 'influencers' unless criteria

    range = criteria.date_filter_for(:published_at)
    return 'influencers' unless range

    year = begin
      Date.parse(range['gte'] || range['lte']).year
    rescue StandardError
      nil
    end

    year ? "content_#{year}" : 'influencers'
  end
end

# ── 3. Build a lazy criteria (nothing is sent yet) ────────────────────────────
criteria = Influencer
  .query { smart_match :name, 'john' }          # custom DSL method
  .query { date_range :published_at,
                      from: '2025-01-01',
                      to:   '2025-12-31' }       # another custom DSL method
  .from(0)
  .size(20)

puts "Compiled query:"
pp criteria.to_query
puts

puts "Resolved index: #{Influencer.search_index(criteria)}"
puts   # => "content_2025"

# ── 4. Execute with custom per-request timeout ────────────────────────────────
# response = criteria.search(timeout: 30)
# puts response.inspect
# puts "Total hits : #{response.total}"
# puts "First source: #{response.sources.first}"

# ── 5. Aggregation example ────────────────────────────────────────────────────
agg_criteria = Influencer
  .criteria
  .size(0)
  .aggregate(:followers_stats) {
    avg field: :follower_count
  }

puts "Aggregation query:"
pp agg_criteria.to_query
puts

# ── 6. PIT paginated search ───────────────────────────────────────────────────
# Uncomment when connected to a real cluster:
#
# all_responses = Influencer
#   .query { match_all }
#   .search_pit(page_size: 500) do |response, total|
#     puts "Fetched #{total} so far …"
#     total < 10_000          # keep going until we have 10k hits
#   end
#
# puts "Pages fetched : #{all_responses.size}"
# puts "Total records : #{all_responses.sum(&:size)}"

puts "All examples compiled successfully."

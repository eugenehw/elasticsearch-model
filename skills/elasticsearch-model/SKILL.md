---
name: elasticsearch-model
description: >
  Build Elasticsearch queries with the elasticsearch-model Ruby ODM gem.
  Use when the user needs to write search queries, filter clauses, aggregations,
  scopes, agg_scopes, PIT pagination, or custom response classes using this gem.
metadata:
  author: eugene
  version: 0.1.0
---

# elasticsearch-model Ruby ODM

Lightweight ODM (Object-Document Mapper) for Elasticsearch. Pure Ruby, no
`elasticsearch-dsl` dependency. Ruby 2.6 compatible.

## When to use this skill

Use when the user is building queries with this gem — filters, bool clauses,
aggregations, scopes, agg_scopes, or search execution.

---

## Setup

```ruby
class MyModel
  include Elasticsearch::Model::Searchable
  index_name 'my_index'
end
```

---

## `scope` — named filter helper

Defines a named method usable inside any `filter {}` or `bool {}` block.

```ruby
scope :active   do; term :status, 'active'; end
scope :deleted  do; term :status, 'deleted'; end
scope :instagram do; term :platform, 'instagram'; end

scope :high_engagement do
  range :engagement, gte: 10_000
end
```

Scopes are called either via `instance_exec` (0-arity block) or via an explicit
`QueryFilter` arg (block with `|f|`):

```ruby
# Both are equivalent:
MyModel.filter { active }
MyModel.filter { |f| f.active }
```

---

## `filter {}` — building filter clauses

All ES clause methods are available inside the block: `term`, `terms`, `range`,
`exists`, `bool`, `must_not`, `raw`, etc. Symbol field names are stringified
automatically.

```ruby
# Simple term filter
MyModel.filter { term :status, 'active' }.to_h

# Using a scope
MyModel.filter { active }.to_h

# Multiple clauses (ANDed as bool/filter)
MyModel.filter do
  active
  term :team, 'team-1'
  range :followers, gte: 1000
end.to_h

# Explicit QueryFilter arg — same result
MyModel.filter do |f|
  f.active
  f.term :team, 'team-1'
end.to_h
```

### `bool` inside filter

```ruby
MyModel.filter do
  bool do
    should do
      active
      deleted
    end
    minimum_should_match 1
  end
end.to_h
```

### `raw` — inject an arbitrary Hash clause

```ruby
MyModel.filter do
  raw(bool: { should: [{term: {status: 'active'}}, {term: {status: 'deleted'}}],
              minimum_should_match: 1 })
end.to_h
```

---

## `aggregate` — inline aggregation

```ruby
# terms agg
MyModel.aggregate(:teams) do |agg|
  agg.terms field: :team, size: 10
end.to_h

# metric sub-aggs
MyModel.aggregate(:teams) do |agg|
  agg.terms field: :team, size: 10
  agg.aggregate(:total_reach)     { sum field: :reach }
  agg.aggregate(:total_followers) { sum field: :followers }
end.to_h

# named-bucket filters agg
MyModel.aggregate(:status) do |agg|
  agg.filters(:active)  { term :status, 'active' }
  agg.filters(:deleted) { term :status, 'deleted' }
end.to_h

# with QueryFilter arg (f captured by closure in inner blocks)
MyModel.aggregate(:status) do |agg, f|
  agg.filters(:active)  { f.active }
  agg.filters(:deleted) { f.deleted }
end.to_h
```

---

## `agg_scope` — reusable aggregations

Defines a named aggregation that becomes:
1. A class method on the model: `MyModel.group_by_status`
2. A method on `AggBuilder` for embedding as a sub-agg: `teams_agg.group_by_status`

```ruby
agg_scope :group_by_status do |agg, f|
  agg.aggregate(:status) do |status_agg|
    status_agg.filters(:active)  { f.active }
    status_agg.filters(:deleted) { f.deleted }
  end
end

agg_scope :group_by_team do |agg|
  agg.aggregate(:teams) do
    terms field: :team, size: 10
  end
end

agg_scope :timeline do |agg|
  agg.aggregate(:timeline) do
    date_histogram field: :published_at, calendar_interval: 'month'
  end
end
```

### Calling agg_scopes

```ruby
# Standalone
MyModel.group_by_status.to_h
MyModel.timeline.to_h

# Chained as siblings (all aggs merged at top level)
MyModel.group_by_team.group_by_status.timeline.to_h

# With extra sub-aggs added at call site
MyModel.group_by_status do |agg, f|
  agg.aggregate(:reach) { sum field: :reach }
end.to_h
```

### Nested / composed agg_scopes

```ruby
# Embed one agg_scope inside another
agg_scope :team_with_status do |agg, f|
  agg.group_by_team do |teams_agg|
    teams_agg.group_by_status   # sub-agg via AggBuilder method
  end
end

# Output:
# { aggs: { teams: { terms: {...}, aggs: { status: { filters: {...} } } } } }
```

---

## Chaining filter + agg_scope + sort

```ruby
MyModel
  .filter { |f| f.instagram }
  .group_by_status
  .sort(:published_at, :desc)
  .size(0)
  .to_h

# filter + agg_scope + inline sub-agg
MyModel
  .filter { |f| f.instagram }
  .timeline do |agg, f|
    agg.group_by_status
    agg.aggregate(:total_reach) { sum field: :reach }
  end
  .size(0)
  .to_h
```

---

## Dynamic index routing

Override `search_index` on the model class. Receives the `Criteria` object so
you can inspect filters (e.g. date ranges):

```ruby
def self.search_index(criteria = nil)
  range = criteria&.date_filter_for(:published_at)
  return index_name unless range

  year = Date.parse(range['gte'] || range['lte']).year
  "content_#{year}"
rescue ArgumentError, TypeError
  index_name
end
```

---

## Custom response class

Define `MyModel::Response < ElasticsearchResponse` — it is auto-detected:

```ruby
class MyModel
  include Elasticsearch::Model::Searchable

  class Response < Elasticsearch::Model::ElasticsearchResponse
    def content_ids
      sources.map { |s| s['content_id'] }
    end

    def timeline_buckets
      agg('timeline')&.fetch('buckets', []) || []
    end
  end
end

result = MyModel.filter { active }.search
result.content_ids       # => [...]
result.timeline_buckets  # => [...]
```

---

## PIT pagination

```ruby
MyModel.filter { active }.search_pit(page_size: 500) do |response, total|
  response.sources.each { |doc| process(doc) }
  puts "Processed #{total} so far"
end
```

---

## Fluent query builder pattern

For complex search objects, accumulate state in a plain Ruby class and delegate
to `Criteria`:

```ruby
class MySearch
  def initialize
    @criteria = MyModel.criteria
  end

  def platforms(values)
    @criteria.platforms(values) if values&.any?
    self
  end

  def start_date(date)
    @criteria.start_date(date)
    self
  end

  def search
    @criteria.search
  end
end

MySearch.new.platforms(['instagram']).start_date('2025-01-01').search
```

---

## Block arity rule (for gem internals / advanced use)

All DSL blocks support two styles — the gem detects `block.arity`:

```ruby
# 0-arity → instance_exec on the context object
MyModel.filter { active }

# 1+-arity → object passed as argument
MyModel.filter { |f| f.active }
```

---

## Common gotchas

- **Ruby 2.6**: no endless method syntax (`def foo = expr` will fail)
- **`Criteria#to_h`** is the public API; `to_query` is an alias
- **`filters(:name) {}`** accumulates named buckets on the same agg node —
  multiple calls build the `filters.filters` hash incrementally
- **`must_not` inside `bool {}`** takes a block, not a plain Hash
- **`date_range`** / `filter_terms` push to the non-scoring filter buffer
  (equivalent to ES `filter` context), not the query context
- **`scope` blocks** run inside a `FilterCollector` — all clause methods work,
  but the block is re-executed fresh on each use (not cached)
- **`Date`** requires explicit `require 'date'` in scripts/specs

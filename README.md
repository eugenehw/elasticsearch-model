# elasticsearch-model

Lightweight Elasticsearch ODM (Object-Document Mapper) — pure Ruby, no elasticsearch-dsl dependency.

## Requirements

- Ruby 3.4.3+
- Bundler 4+
- Docker (for integration tests)

## Installation

```bash
bundle install
```

---

## Testing

### Unit tests (no Docker required)

```bash
bundle exec ruby spec/criteria_spec.rb
```

### Integration tests (requires Docker)

```bash
bundle exec ruby spec/integration/influencer_spec.rb
```

The first run will automatically pull the `elasticsearch:8.13.4` image (~1GB). Subsequent runs use the cached image.

---

## Docker Installation

### macOS Sonoma (14+)

```bash
brew install --cask docker
open /Applications/Docker.app
```

### macOS Ventura (13) and older

Docker Desktop 4.x does not support macOS 13. Use Colima instead:

```bash
brew install colima docker docker-compose
colima start
```

---

## Troubleshooting

### `No such file or directory - connect(2) for /var/run/docker.sock`

Docker Desktop on macOS sometimes places the socket at `~/.docker/run/docker.sock` instead of `/var/run/docker.sock`.

Verify the socket location:

```bash
ls ~/.docker/run/docker.sock
```

Run tests with the correct socket path:

```bash
DOCKER_HOST=unix://$HOME/.docker/run/docker.sock bundle exec ruby spec/integration/influencer_spec.rb
```

Or create a symlink (requires sudo):

```bash
sudo ln -sf ~/.docker/run/docker.sock /var/run/docker.sock
```

### `This software does not run on macOS versions older than Sonoma`

Docker Desktop does not support your macOS version. Use Colima instead (see installation steps above).

### `rbenv: command not found`

rbenv is not loaded in the current shell. Add the following to `~/.zshrc`:

```bash
export PATH="/opt/homebrew/bin:$PATH"
eval "$(/opt/homebrew/bin/rbenv init -)"
```

Then reload:

```bash
source ~/.zshrc
```

### Wrong Ruby version (e.g. system Ruby 2.6)

Once rbenv is set up, pin the project to Ruby 3.4.3:

```bash
rbenv local 3.4.3
ruby --version   # should print 3.4.3
gem install bundler
bundle install
```

---

## Quick Reference

```bash
# 1. Verify Ruby version
ruby --version   # 3.4.3

# 2. Install gems
bundle install

# 3. Run unit tests
bundle exec ruby spec/criteria_spec.rb

# 4. Run integration tests (Docker must be running)
DOCKER_HOST=unix://$HOME/.docker/run/docker.sock \
  bundle exec ruby spec/integration/influencer_spec.rb
```

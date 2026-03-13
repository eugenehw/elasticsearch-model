module Elasticsearch
  module Model
    # Holds all tunable settings.
    class Config
      attr_accessor :url, :log, :request_timeout, :user, :password,
                    :retry_on_failure, :headers, :pit_keep_alive

      def initialize
        @url              = ENV.fetch('ELASTICSEARCH_URL', 'http://localhost:9200')
        @log              = false
        @request_timeout  = 30
        @pit_keep_alive   = '1m'
        @retry_on_failure = false
        @user             = nil
        @password         = nil
        @headers          = {}
      end
    end

    # Mixin that provides `.configure` / `.config` on Elasticsearch::Model.
    module Configuration
      # Elasticsearch::Model.configure do |config|
      #   config.url = "http://es:9200"
      #   config.request_timeout = 60
      # end
      def configure
        yield config
        @client = nil # reset client so it's rebuilt with new config
      end

      def config
        @config ||= Elasticsearch::Model::Config.new
      end
    end
  end
end

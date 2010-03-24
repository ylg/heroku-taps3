require 'json'

class Heroku::Client
  def database_session(app_name)
    response = post("/apps/#{app_name}/database/session2", '', :x_taps_version => Taps.version, :x_ruby_version => RUBY_VERSION)
    JSON.parse(response.body)
  end
end

module Heroku::Command
  class Db < BaseWithApp
    def initialize(*args)
      super(*args)
      gem 'taps', '~> 0.3.0'
      require 'taps/operation'
      display "Loaded Taps v#{Taps.version}"
    rescue LoadError
      message  = "Taps3 Load Error: #{$!.message}\n"
      message << "You may need to install or update the taps gem to use db commands.\n"
      message << "On most systems this will be:\n\nsudo gem install --pre taps"
      error message
    end

    def push
      database_url = args.shift.strip rescue ''
      if database_url == ''
        database_url = parse_database_yml
        display "Auto-detected local database: #{database_url}" if database_url != ''
      end
      raise(CommandFailed, "Invalid database url") if database_url == ''

      # setting local timezone equal to Heroku timezone allowing TAPS to
      # correctly transfer datetime fields between databases
      ENV['TZ'] = 'America/Los_Angeles'
      taps_client(:push, database_url)
    end

    def pull
      database_url = args.shift.strip rescue ''
      if database_url == ''
        database_url = parse_database_yml
        display "Auto-detected local database: #{database_url}" if database_url != ''
      end
      raise(CommandFailed, "Invalid database url") if database_url == ''

      # setting local timezone equal to Heroku timezone allowing TAPS to
      # correctly transfer datetime fields between databases
      ENV['TZ'] = 'America/Los_Angeles'
      taps_client(:pull, database_url)
    end

    protected

    def taps_client(method, database_url)
      chunk_size = 1000
      Taps::Config.verify_database_url database_url

      info = heroku.database_session(app)

      taps = Taps::Operation.factory(method, database_url, info['url'], :default_chunksize => chunk_size)
      taps.run
    end
  end
end

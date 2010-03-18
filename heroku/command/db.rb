require 'json'

class Heroku::Client
  def database_session(app_name)
    JSON.parse(post("/apps/#{app_name}/database/session2", '', :x_taps_version => Taps.version, :x_ruby_version => RUBY_VERSION))
  end
end

module Heroku::Command
  class Db < BaseWithApp
    def initialize(*args)
      super(*args)
      gem 'taps', '~> 0.3.0'
      require 'taps/client_session'
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
      taps_client(database_url) do |client|
        client.push
      end
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
      taps_client(database_url) do |client|
        client.pull
      end
    end

    protected

    def taps_client(database_url, &block)
      chunk_size = 1000
      Taps::Config.database_url = database_url
      Taps::Config.verify_database_url

      info = heroku.database_session(app)

      Taps::ClientSession.start(database_url, info['url'], chunk_size) do |client|
        client.set_session(info['session'])
        client.verify_server
        yield client
      end
    end
  end
end

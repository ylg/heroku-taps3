require 'json'

class Heroku::Client
  def database_session(app_name)
    JSON.parse(post("/apps/#{app_name}/database/session2", '', :x_taps_version => Taps.version, :x_ruby_version => RUBY_VERSION).to_s)
  end
end

module Heroku::Command
  class Db < BaseWithApp
    def initialize(*args)
      super(*args)
      gem 'taps', '~> 0.3.0'
      require 'taps/operation'
      require 'taps/cli'
      display "Loaded Taps v#{Taps.version}"
    rescue LoadError
      message  = "Taps3 Load Error: #{$!.message}\n"
      message << "You may need to install or update the taps gem to use db commands.\n"
      message << "On most systems this will be:\n\nsudo gem install --pre taps"
      error message
    end

    def push
      opts = parse_taps_opts
      taps_client(:push, opts)
    end

    def pull
      opts = parse_taps_opts
      taps_client(:pull, opts)
    end

    protected

    def parse_taps_opts
      opts = {}
      opts[:default_chunksize] = extract_option("--chunksize", 1000)

      if extract_option("--disable-compression")
        opts[:disable_compression] = true
      end

      if resume_file = extract_option("--resume-filename")
        opts[:resume_filename] = resume_file
      end

      opts[:database_url] = args.shift.strip rescue ''
      if opts[:database_url] == ''
        opts[:database_url] = parse_database_yml
        display "Auto-detected local database: #{opts[:database_url]}" if opts[:database_url] != ''
      end
      raise(CommandFailed, "Invalid database url") if opts[:database_url] == ''

      # setting local timezone equal to Heroku timezone allowing TAPS to
      # correctly transfer datetime fields between databases
      ENV['TZ'] = 'America/Los_Angeles'
      opts
    end

    def taps_client(op, opts)
      Taps::Config.verify_database_url(opts[:database_url])
      if opts[:resume_filename]
        Taps::Cli.new([]).clientresumexfer(op, opts)
      else
        info = heroku.database_session(app)
        opts[:remote_url] = info['url']
        opts[:session_uri] = info['session']
        Taps::Cli.new([]).clientxfer(op, opts)
      end
    end
  end
end

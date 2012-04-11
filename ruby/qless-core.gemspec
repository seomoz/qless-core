# -*- encoding: utf-8 -*-
require File.expand_path('../lib/qless/core/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors     = ["Dan Lecocq"]
  gem.email       = ["dan@seomoz.org"]
  gem.homepage    = "http://github.com/seomoz/qless-core"
  gem.summary     = %q{Core lua scripts for qless, a Redis-Based Queueing System}
  gem.description = %q{This contains the core lua scripts. The qless gem provides the ruby client.}

  gem.files         = Dir.glob("ruby/lib/**/*.rb")
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})

  gem.name          = "qless-core"
  gem.require_paths = ["lib"]
  gem.version       = Qless::Core::VERSION
end

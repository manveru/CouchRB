require 'rake'
require 'rake/clean'
require 'rake/gempackagetask'
require 'time'
require 'date'

PROJECT_SPECS = Dir['spec/**/*.rb']
PROJECT_MODULE = 'CouchRB'
PROJECT_COPYRIGHT =
  '# Copyright (c) 2009 Michael Fellinger <m.fellinger@gmail.com>'

GEMSPEC = Gem::Specification.new{|s|
  s.name         = 'couchrb'
  s.author       = "Michael 'manveru' Fellinger"
  s.summary      = "Pure Ruby implementation of CouchDB."
  s.description  = "Pure Ruby implementation of CouchDB with indentical API."
  s.email        = 'm.fellinger@gmail.com'
  s.homepage     = 'http://github.com/manveru/couchrb'
  s.platform     = Gem::Platform::RUBY
  s.version      = (ENV['PROJECT_VERSION'] || Date.today.strftime("%Y.%m.%d"))
  s.files        = `git ls-files`.split("\n").sort
  s.has_rdoc     = true
  s.require_path = 'lib'
  s.executables = ['couchrb']
  s.bindir = "bin"
}

Dir['tasks/*.rake'].each{|f| import(f) }

task :default => [:bacon]

CLEAN.include('')

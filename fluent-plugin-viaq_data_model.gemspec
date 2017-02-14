# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-viaq_data_model"
  gem.version       = "0.0.2"
  gem.authors       = ["Rich Megginson"]
  gem.email         = ["rmeggins@redhat.com"]
  gem.description   = %q{Filter plugin to ensure data is in the ViaQ common data model}
  gem.summary       = %q{Filter plugin to ensure data is in the ViaQ common data model}
  gem.homepage      = "https://github.com/ViaQ/fluent-plugin-viaq_data_model"
  gem.license       = "Apache-2.0"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}) { |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.has_rdoc      = false

  gem.required_ruby_version = '>= 2.0.0'

  gem.add_runtime_dependency "fluentd", ">= 0.12.0"

  gem.add_development_dependency "bundler", "~> 1.3"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "minitest", "~> 4.0"
  gem.add_development_dependency "test-unit", "~> 3.0.2"
  gem.add_development_dependency "test-unit-rr", "~> 1.0.3"
  gem.add_development_dependency "copyright-header"
end

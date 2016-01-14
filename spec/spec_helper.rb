require 'rubygems'
require 'rspec'
require 'dotenv'
Dotenv.load

require 'carrierwave'
require 'carrierwave-azure'
require 'environment'

RSpec.configure do |config|
  config.order = :random
end

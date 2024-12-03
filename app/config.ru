require 'rack'
require 'rack/contrib'
require_relative './s3-server'

set :root, File.dirname(__FILE__)
set :views, Proc.new { File.join(root, "views") }
set :host_authorization => []

run Sinatra::Application

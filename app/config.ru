# frozen_string_literal: true

require 'rack'
require 'rack/contrib'
require_relative 's3-server'

set :root, File.dirname(__FILE__)
set :views, proc { File.join(root, 'views') }
set :host_authorization => { permitted_hosts: [] }

run Sinatra::Application

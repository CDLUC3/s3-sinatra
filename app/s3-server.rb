require 'sinatra'
require 'sinatra/base'
require 'aws-sdk-s3'
require 'aws-sdk-ssm'
require_relative 'lib/listing.rb'

MERRITT_METADATA = Keymap.metadata

helpers do

  def protected!
    @auth ||=  Rack::Auth::Basic::Request.new(request.env)
    if not basic_auth_credentials_included?
      response['WWW-Authenticate'] = 'Basic realm="Restricted Area"'
      halt 401, "Unauthorized"
    elsif not authorized?
      response['WWW-Authenticate'] = 'Basic realm="Restricted Area"'
      halt 401, "Unauthorized"
      #halt 403, "Access denied\n"
    else
      return true
    end
  end

  def basic_auth_credentials_included?
    @auth.provided? and @auth.basic? and @auth.credentials
  end

  def authorized?
    user_name = @auth.credentials[0]
    ssm_credentials = get_credentials_from_parameter_store(user_name)
    ssm_credentials and @auth.credentials == [user_name, ssm_credentials[:parameter][:value]]
  end

  def get_credentials_from_parameter_store(user_name)
    ssm_credentials_path = env.fetch('SSM_CREDENTIALS_PATH', nil)
    key = "#{ssm_credentials_path}/credentials/#{user_name}"
    ssm_client = Aws::SSM::Client.new(region: ENV.fetch('AWS_REGION', nil))
    ssm_client.get_parameter(name: key, with_decryption: true)
  rescue Aws::SSM::Errors::ParameterNotFound
    nil
  end

  def file_exists(key)
    @s3_client = Aws::S3::Client.new(region: ENV.fetch('AWS_REGION', nil))
    bucket_name = env.fetch('BUCKET_NAME', nil)
    begin
      @s3_client.head_object({bucket: bucket_name, key: key})
      @s3_client.get_object({bucket: bucket_name, key: key}).body
    rescue Aws::S3::Errors::NotFound
      return nil
    end
  end

  def get_file(key)
    @s3_client = Aws::S3::Client.new(region: ENV.fetch('AWS_REGION', nil))
    @presigner = Aws::S3::Presigner.new(client: @s3_client)
    bucket_name = env.fetch('BUCKET_NAME', nil)
    begin
      @s3_client.head_object({bucket: bucket_name, key: key})
    rescue Aws::S3::Errors::NotFound
      halt 404, "Object \"#{key}\" not found in S3 bucket \"#{bucket_name}\"\n"
    end
    url, headers = @presigner.presigned_request(:get_object, bucket: bucket_name, key: key)
    if url
      response.headers['Location'] = url
      status 303
      "success: redirecting"
    end
  end

  def generate_file(key, s)
    @s3_client = Aws::S3::Client.new(region: ENV.fetch('AWS_REGION', nil))
    @presigner = Aws::S3::Presigner.new(client: @s3_client)
    bucket_name = env.fetch('BUCKET_NAME', nil)
    begin
      @s3_client.put_object({
        body: s,
        bucket: bucket_name,
        key: key
      })
    rescue Aws::S3::Errors::NotFound => e
      halt 404, "Object Put failed for \"#{key}\"  in S3 bucket \"#{bucket_name}\": #{e}\n"
    end
    begin
      @s3_client.head_object({bucket: bucket_name, key: key})
    rescue Aws::S3::Errors::NotFound
      halt 404, "Object \"#{key}\" not found in S3 bucket \"#{bucket_name}\"\n"
    end
    url, headers = @presigner.presigned_request(
      :get_object, 
      bucket: bucket_name, 
      key: key,
      ResponseContentDisposition: "inline"
    )
    if url
      response.headers['Location'] = url
      status 303
      "success: redirecting"
    end
  end

end

def listing(prefix: '', depth: 0, credentials: nil, mode: :component)
  Listing.new(
    region: ENV.fetch('AWS_REGION', nil), 
    bucket: env.fetch('BUCKET_NAME', nil), 
    dns: env.fetch('BASE_URL', nil),
    maxobj: 30,
    maxpre: 30,
    prefix: prefix,
    depth: depth,
    credentials: credentials,
    mode: mode
  )
end

def make_auth_listing(prefix: '', depth: 0, mode: :component)
  @listing = listing(prefix: prefix, depth: depth, credentials: @auth.credentials, mode: mode)
  @listing.list_keys
end

def return_string(s, type: 'text/plain; charset=utf-8')
  s = s.encode("UTF-8")

  if s.length >= 1_000_000
    generate_file("#{Listing::GENERATED_PATH}#{request.path}", s)
  else
    content_type type
    headers 'Content-Disposition' => "inline"
    status 200
    s
  end
end


get "/" do
  @listing = listing(mode: :directory, prefix: 'ZZZ')
  @listing.list_keys

  status 200
  erb :index
end

get "/listing" do
  protected!

  make_auth_listing(mode: :directory)

  status 200
  erb :listing
end

get '/*/object.checkm' do
  protected!

  make_auth_listing(prefix: params['splat'][0], depth: 0)

  return_string(@listing.object_data)
end

get '/*/batchobject.checkm' do
  protected!

  make_auth_listing(prefix: params['splat'][0], depth: 0)

  return_string(@listing.batchobject_data(file_exists("#{params['splat'][0]}/#{MERRITT_METADATA}")))
end

get '/*/batchobject.csv' do
  protected!

  metadata = file_exists("#{params['splat'][0]}/#{MERRITT_METADATA}")
  return return_string(metadata, type: 'text/csv; charset=utf-8') if metadata

  make_auth_listing(prefix: params['splat'][0], depth: 0)

  return_string(@listing.batchobject_csv, type: 'text/csv; charset=utf-8')
end

get '/object.checkm' do
  protected!

  make_auth_listing(prefix: '', depth: 0)

  return_string(@listing.object_data)
end

get '/batchobject.checkm' do
  protected!

  make_auth_listing(prefix: '', depth: 0)

  return_string(@listing.batchobject_data(file_exists("#{MERRITT_METADATA}")))
end

get '/batchobject.csv' do
  protected!

  metadata = file_exists(MERRITT_METADATA)
  return return_string(metadata, type: 'text/csv; charset=utf-8') if metadata

  make_auth_listing(prefix: '', depth: 0)

  return_string(@listing.batchobject_csv, type: 'text/csv; charset=utf-8')
end

get %r[/(.*)/batch.depth(-?\d+).checkm] do |key, d|
  protected!

  make_auth_listing(prefix: key, depth: d.to_i)

  return_string(@listing.batch_data(file_exists("#{key}/#{MERRITT_METADATA}")))
end

get %r[/(.*)/batch.depth(-?\d+).csv] do |key, d|
  protected!

  metadata = file_exists("#{key}/#{MERRITT_METADATA}")
  return return_string(metadata, type: 'text/csv; charset=utf-8') if metadata

  make_auth_listing(prefix: key, depth: d.to_i)

  return_string(@listing.batch_csv, type: 'text/csv; charset=utf-8')
end

get %r[/(.*)/batch.depth(-?\d+)] do |key, d|
  protected!

  make_auth_listing(prefix: key, depth: d.to_i)

  status 200
  erb :listing
end

get %r[/batch.depth(-?\d+)] do |d|
  protected!

  make_auth_listing(prefix: '', depth: d.to_i)

  status 200
  erb :listing
end

get %r[/batch.depth(-?\d+).checkm] do |d|
  protected!

  make_auth_listing(prefix: '', depth: d.to_i)

  return_string(@listing.batch_data(file_exists(MERRITT_METADATA)))
end

get %r[/batch.depth(-?\d+).csv] do |d|
  protected!

  metadata = file_exists(MERRITT_METADATA)
  return return_string(metadata, type: 'text/csv; charset=utf-8') if metadata

  make_auth_listing(prefix: '', depth: d.to_i)

  return_string(@listing.batch_csv, type: 'text/csv; charset=utf-8')
end

get %r[/(.*)/batch-other.depth(-?\d+).checkm] do |key, d|
  protected!

  make_auth_listing(prefix: key, depth: d.to_i)

  return_string(@listing.other_data(file_exists("#{key}/#{MERRITT_METADATA}")))
end

get %r[/batch-other.depth(-?\d+).checkm] do |d|
  protected!

  make_auth_listing(prefix: '', depth: d.to_i)

  return_string(@listing.other_data(file_exists(MERRITT_METADATA)))
end

get '/*/' do
  protected!

  make_auth_listing(prefix: params['splat'][0], depth: 1, mode: :directory)

  status 200
  erb :listing
end

get '/*' do
  protected!
  key = params['splat'][0]
  get_file(key)
end
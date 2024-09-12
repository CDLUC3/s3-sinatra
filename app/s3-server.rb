require 'sinatra'
require 'sinatra/base'
require 'aws-sdk-s3'
require 'aws-sdk-ssm'



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

end

def list_keys(prefix: '', delimiter: nil, maxobj: 10, erbname: :listing, credentials: nil)
  @data = "List of objects"
  @s3_client = Aws::S3::Client.new(region: ENV.fetch('AWS_REGION', nil))
  keys = []
  @objlist = []
  @prefixes = []
  @data = []
  dns = env.fetch('BASE_URL', nil)
  resp = @s3_client.list_objects(bucket: env.fetch('BUCKET_NAME', nil), delimiter: delimiter, prefix: prefix, max_keys: maxobj)
  resp.to_h.fetch(:contents, []).each do |s3obj|
    k = s3obj.fetch(:key, "")
    next if k.empty?
    url = credentials.nil? ? "https://#{dns}/#{k}" : "https://#{credentials.join(':')}@#{dns}/#{k}"
    @objlist.append({
      key: k,
      url: url
    }) unless k.empty?
    @data.append(url)
  end
  resp = @s3_client.list_objects(bucket: env.fetch('BUCKET_NAME', nil), delimiter: '/', prefix: prefix, max_keys: maxobj) if delimiter.nil?
  resp.to_h.fetch(:common_prefixes, []).each do |obj|
    k = obj.fetch(:prefix, "")
    url = "https://#{dns}/#{k}"
    @prefixes.append({
      key: k,
      url: url
    }) unless k.empty?
  end

  status 200
  erb erbname
end

get "/" do
  list_keys(delimiter: '/', erbname: :index)
end

get "/listing" do
  protected!
  status 200
  @data = ""
  erb :listing
end

post "/listing" do
  protected!
  list_keys(credentials: @auth.credentials, maxobj: 500, delimiter: nil)
end

get '/*/' do
  protected!
  key = params['splat'][0]
  list_keys(credentials: @auth.credentials, prefix: "#{key}/", maxobj: 500, delimiter: nil)
end

get '/*' do
  protected!
  key = params['splat'][0]
  get_file(key)
end
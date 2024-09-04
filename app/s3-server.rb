require 'sinatra'
require 'sinatra/base'
require 'aws-sdk-s3'
require 'aws-sdk-ssm'



helpers do

  def protected!
    @auth ||=  Rack::Auth::Basic::Request.new(request.env)
    if not basic_auth_credentials_included?
      headers['WWW-Authenticate'] = 'Basic realm="Restricted Area"'
      halt 401, "Not authorized\n"
    elsif not authorized?
      headers['WWW-Authenticate'] = 'Basic realm="Restricted Area"'
      halt 403, "Access denied\n"
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

get "/" do
  @maxobj = 100
  @s3_client = Aws::S3::Client.new(region: ENV.fetch('AWS_REGION', nil))
  @top_objects = []
  @top_prefixes = []
  resp = @s3_client.list_objects(bucket: env.fetch('BUCKET_NAME', nil), delimiter: '/', max_keys: @maxobj)
  resp.to_h.fetch(:contents, []).each do |obj|
    k = obj.fetch(:key, "")
    @top_objects.append(k) unless k.empty?
  end
  resp.to_h.fetch(:common_prefixes, []).each do |obj|
    k = obj.fetch(:prefix, "")
    @top_prefixes.append(k) unless k.empty?
  end
  status 200
  erb :index
end

get '/*' do
  protected!
  key = params['splat'][0]
  get_file(key)
end

require 'aws-sdk-s3'
require_relative 'keymap.rb'

class Listing
  MAXLIST = 200_000

  def initialize(
    region: 'us-west-2', 
    bucket: 'na', 
    dns: 'http://foo.bar', 
    maxobj: 20, 
    maxpre: 20,
    prefix: '',
    depth: 0
  )
    @bucket = bucket
    @dns = dns
    @maxobj = maxobj
    @maxpre = maxpre
    @s3_client = Aws::S3::Client.new(region: region)
    @prefix = prefix
    @depth = depth
    @keymap = keymap.new(@prefix, @depth)
  end

  def list_keys(delimiter: nil, credentials: nil)
    opt = {
      bucket: @bucket, 
      delimiter: delimiter, 
      prefix: @prefix
    }
    loop do
      resp = @s3_client.list_objects_v2(opt)
      resp.to_h.fetch(:contents, []).each do |s3obj|
        @km.add_node(s3obj.fetch(:key, ''))
      end
      break unless resp.is_truncated
      opt[:continuation_token] = resp.next_continuation_token
    end
  end

  def topobjlist
    @km.topkeys[0..@maxobj-1]
  end

  def prefixes
    @km.topdirs[0..@maxpre-1]
  end

  #url = credentials.nil? ? "https://#{@dns}/#{k}" : "https://#{credentials.join(':')}@#{@dns}/#{k}"
  
end
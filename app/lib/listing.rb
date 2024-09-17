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
    depth: 0,
    credentials: nil
  )
    @bucket = bucket
    @dns = dns
    @maxobj = maxobj
    @maxpre = maxpre
    @s3_client = Aws::S3::Client.new(region: region)
    @prefix = prefix
    @depth = depth
    @keymap = Keymap.new(@prefix, @depth, dns: @dns, credentials: credentials)
  end

  def parent
    p = File.dirname(@prefix)
    p == '.' ? '' : p
  end

  def list_keys(prefix: '', delimiter: nil)
    opt = {
      bucket: @bucket, 
      delimiter: delimiter, 
      prefix: prefix
    }
    loop do
      resp = @s3_client.list_objects_v2(opt)
      resp.to_h.fetch(:contents, []).each do |s3obj|
        @keymap.add_node(s3obj.fetch(:key, ''))
      end
      break unless resp.is_truncated
      opt[:continuation_token] = resp.next_continuation_token
    end
  end

  def topobjlist
    @keymap.topkeys[0..@maxobj-1]
  end

  def prefixes
    @keymap.topdirs[0..@maxpre-1]
  end

  def allkeys
    @keymap.allkeys
  end

  def reports
    arr = []
    arr.append(
      {
        url: "/#{@keymap.report_data[:title]}",
        desc: @keymap.report_data[:title]
      }
    ) unless @keymap.empty?
    arr
  end
  
end
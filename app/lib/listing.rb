require 'aws-sdk-s3'

class Listing
  MAXLIST = 200_000

  def initialize(region: 'us-west-2', bucket: 'na', dns: 'http://foo.bar', maxobj: 20, maxpre: 20)
    @bucket = bucket
    @dns = dns
    @maxobj = maxobj
    @maxpre = maxpre
    @s3_client = Aws::S3::Client.new(region: region)
    keys = []
    @objlist = []
    @topobjlist = []
    @prefixes = {}
    @data = []
  end

  def list_keys(prefix: '', delimiter: nil, credentials: nil)
    opt = {
      bucket: @bucket, 
      delimiter: delimiter, 
      prefix: prefix
    }
    loop do
      resp = @s3_client.list_objects_v2(opt)
      resp.to_h.fetch(:contents, []).each do |s3obj|
        save_key(s3obj, credentials, prefix)
      end
      break unless resp.is_truncated
      opt[:continuation_token] = resp.next_continuation_token
    end
  end

  def objlist
    @objlist[0..@maxobj-1]
  end

  def topobjlist
    @topobjlist[0..@maxobj-1]
  end

  def prefixes
    @prefixes.values[0..@maxpre-1]
  end

  def data
    @data
  end

  def save_key(s3obj, credentials, prefix)
    k = s3obj.fetch(:key, "")
    return if k.empty?

    return if k =~ /\/$/

    url = credentials.nil? ? "https://#{@dns}/#{k}" : "https://#{credentials.join(':')}@#{@dns}/#{k}"

    rec = {
      key: k,
      url: url
    }
    unless @objlist.length > MAXLIST
      @objlist.append(rec)
      @data.append(url) 
    end

    ka = k[prefix.length..].split('/')
    
    kprefix = ka.length > 1 ? ka[0] : ''

    if kprefix.empty?
      @topobjlist.append(rec) 
      return
    end

    path = prefix.empty? ? kprefix : "#{prefix}/#{kprefix}"
  
    rec = @prefixes.fetch(
      kprefix, 
      {
        key: kprefix, 
        count: 0, 
        desc: kprefix, 
        url: "https://#{@dns}/#{path}/",
        depth: 0
      }
    )
    rec[:count] += 1
    rec[:depth] = [ka.length, rec[:depth]].max
    rec[:desc] = "#{kprefix} (#{rec[:count]}, #{rec[:depth]})"
    @prefixes[kprefix] = rec
  end
  
end
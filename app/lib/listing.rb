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
    credentials: nil,
    mode: :component
  )
    @bucket = bucket
    @dns = dns
    @maxobj = maxobj
    @maxpre = maxpre
    @s3_client = Aws::S3::Client.new(region: region)
    @prefix = prefix
    @prefixpath = prefix.empty? ? '/' : "/#{prefix}/"
    @depth = depth
    @mode = mode
    @keymap = Keymap.new(@prefix, @depth, dns: @dns, credentials: credentials)
  end

  def parent
    p = File.dirname(@prefix)
    p == '.' ? '' : p
  end

  def list_keys
    opt = {
      bucket: @bucket, 
      prefix: @prefix
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
    return [] if @mode == :component
    @keymap.topkeys[0..@maxobj-1]
  end

  def prefixes
    return [] if @mode == :component
    @keymap.topdirs[0..@maxpre-1]
  end

  def checkm_header
    %{
#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-ingest-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:mimeType
    }
  end
https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/call911.jpg | md5 | 47d321056e60944a06973793c7b5841e | 87112 |  | call911.jpg
https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/call911.txt | md5 | 77fe42b1055bbabe5164822823218f0a | 46 |  | call911.txt

  def checkm_footer
   %{
#%eof
    }
  end

  def object_data
    checkm_header + @keymap.allkeys.join("\n") + checkm_footer
  end

  def batch_data
    @keymap.batchkeys.join("\n")
  end

  def other_data
    @keymap.otherkeys.join("\n")
  end

  def component_data
    @keymap.component_data
  end

  def manifest_options
    arr = []
    return arr if @mode == :component
    %w[object.checkm].each do |k|
      arr.append({
        url: "#{@prefixpath}#{k}",
        desc: "#{k}"
      })
    end
    %w[batch.depth1 batch.depth2 batch.depth-1 batch.depth-2].each do |k|
      arr.append({
        url: "#{@prefixpath}#{k}",
        desc: "#{k}",
        download: "#{@prefixpath}#{k}.checkm"
      })
    end
    arr
  end

  def components
    arr = []
    return arr if @mode == :directory
    @keymap.component_data[:recs].each do |k,v|
      arr.append(
        {
          url: "#{k}",
          desc: "#{k} (#{v})"
        }
      )
    end
    arr
  end

end
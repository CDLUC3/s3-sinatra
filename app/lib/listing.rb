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
%{#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-ingest-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:mimeType
}
  end

  def batchobject_checkm_header
%{#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-single-file-batch-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:primaryIdentifier | mrt:localIdentifier | mrt:creator | mrt:title | mrt:date
}
  end
    
  def batch_checkm_header
%{#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-batch-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:primaryIdentifier | mrt:localIdentifier | mrt:creator | mrt:title | mrt:date
}
  end
    
  def checkm_footer
%{
#%eof
}
  end

  def manifest_urls(arr, pre)
    marr = []
    arr.each do |k|
      f = k[pre.length..]
      marr.append("#{k} | | | | | #{f}")
    end
    marr.join("\n")
  end

  def batch_manifest_urls(arr, pre, flatten: true)
    marr = []
    arr.each do |k|
      f = k[pre.length..]
      f.gsub!(/\//, '_') if flatten
      marr.append("#{k} | | | | | #{f} | | | | | ")
    end
    marr.join("\n")
  end

  def object_data
    checkm_header + manifest_urls(@keymap.allkeys, @keymap.url_prefix) + checkm_footer
  end

  def batchobject_data
    batchobject_checkm_header + batch_manifest_urls(@keymap.allkeys, @keymap.url_prefix, flatten: false) + checkm_footer
  end

  def batchobject_yaml
    arr = []
    arr.append("# Save to #{@prefix}/merritt.metadata.yaml")
    arr.append("# Not yet implemented...")
    @keymap.allkeys.each do |k|
      arr.append("#{k[@keymap.url_prefix.length..]}:")
      arr.append("  primary_id:")
      arr.append("  local_id:")
      arr.append("  erc_what:")
      arr.append("  erc_who:")
      arr.append("  erc_when:")
    end
    arr.join("\n")
  end

  def batch_data
    batch_checkm_header + batch_manifest_urls(@keymap.batchkeys, @keymap.url_prefix) + checkm_footer
  end

  def batch_yaml
    arr = []
    arr.append("# Save to #{@prefix}/merritt.metadata.yaml")
    arr.append("# Not yet implemented...")
    @keymap.batchkeys.each do |k|
      arr.append("#{k[@keymap.url_prefix.length..]}:")
      arr.append("  primary_id:")
      arr.append("  local_id:")
      arr.append("  erc_what:")
      arr.append("  erc_who:")
      arr.append("  erc_when:")
    end
    arr.join("\n")
  end

  def other_data
    checkm_header + manifest_urls(@keymap.otherkeys, @keymap.url_prefix) + checkm_footer
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
    %w[batchobject].each do |k|
      arr.append({
        url: "",
        desc: "#{k}",
        download: "#{@prefixpath}#{k}.checkm",
        yaml: "#{@prefixpath}#{k}.yaml"
      })
    end
    karr = []
    for i in 1..@keymap.maxdepth - 1
      karr.append("batch.depth#{i}")
    end
    for i in 1..@keymap.maxdepth - 1
      karr.append("batch.depth-#{i}")
    end
    karr.each do |k|
      arr.append({
        url: "#{@prefixpath}#{k}",
        desc: "#{k}",
        download: "#{@prefixpath}#{k}.checkm",
        yaml: "#{@prefixpath}#{k}.yaml"
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
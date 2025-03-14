# frozen_string_literal: true

require 'aws-sdk-s3'
require 'csv'
require_relative 'keymap'

# frozen_string_literal: true

## List items found in bucket
class Listing
  GENERATED_PATH = 'ingest-workspace-generated'
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
      puts "List #{@prefix}: #{opt}"
      resp = @s3_client.list_objects_v2(opt)
      resp.to_h.fetch(:contents, []).each do |s3obj|
        key = s3obj.fetch(:key, '')
        next if key.start_with?(GENERATED_PATH)
        next unless key.start_with?("#{@prefix}/")

        @keymap.add_node(key)
      end
      break unless resp.is_truncated

      opt[:continuation_token] = resp.next_continuation_token
    end
    puts "List #{@prefix}: #{@keymap.length} keys"
  end

  def topobjlist
    return [] if @mode == :component

    @keymap.topkeys[0..@maxobj - 1]
  end

  def prefixes
    return [] if @mode == :component

    @keymap.topdirs[0..@maxpre - 1]
  end

  def checkm_header
    %(#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-ingest-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:mimeType
)
  end

  def batchobject_checkm_header
    %(#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-single-file-batch-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:primaryIdentifier | mrt:localIdentifier | mrt:creator | mrt:title | mrt:date
)
  end

  def batch_checkm_header
    %(#%checkm_0.7
#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-batch-manifest
#%prefix | mrt: | http://merritt.cdlib.org/terms#
#%prefix | nfo: | http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#
#%fields | nfo:fileUrl | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | mrt:primaryIdentifier | mrt:localIdentifier | mrt:creator | mrt:title | mrt:date
)
  end

  def checkm_footer
    %(
#%eof
)
  end

  def manifest_urls(arr, pre)
    marr = []
    arr.each do |k|
      f = k[pre.length..]
      marr.append("#{k} | | | | | #{f}")
    end
    marr.join("\n")
  end

  def batch_manifest_urls(arr, pre, flatten: true, metadata: nil)
    mm = {}
    if metadata
      csv = CSV.parse(metadata)
      csv.shift
      csv.each do |row|
        mm[row[0]] = {
          primary_id: row[1],
          local_id: row[2],
          erc_what: row[3],
          erc_who: row[4],
          erc_when: row[5]
        }
      end
    end
    marr = []
    arr.each do |k|
      f = k[pre.length..]
      mrec = mm.fetch(f, {})
      rec = [
        k,
        '', # hash alg
        '', # hash val
        '', # file size
        '', # file mod
        flatten ? f.gsub!('/', '_') : f,
        mrec.fetch(:primary_id, ''),
        mrec.fetch(:local_id, ''),
        mrec.fetch(:erc_what, ''),
        mrec.fetch(:erc_who, ''),
        mrec.fetch(:erc_when, '')
      ]
      marr.append(rec.join('|'))
    end
    marr.join("\n")
  end

  def object_data
    checkm_header +
      manifest_urls(@keymap.allkeys, @keymap.url_prefix) +
      checkm_footer
  end

  def batchobject_data(metadata)
    batchobject_checkm_header +
      batch_manifest_urls(@keymap.allkeys, @keymap.url_prefix, flatten: false, metadata: metadata) +
      checkm_footer
  end

  def batchobject_csv
    CSV.generate do |csv|
      csv << ['§key', 'primary_id', 'local_id', 'erc_what', 'erc_who', 'erc_when']
      @keymap.allkeys.each do |k|
        csv << [k[@keymap.url_prefix.length..], '', '', '', '', '']
      end
    end
  end

  def batch_data(metadata)
    batch_checkm_header +
      batch_manifest_urls(@keymap.batchkeys, @keymap.url_prefix, metadata: metadata) +
      checkm_footer
  end

  def batch_csv
    CSV.generate do |csv|
      csv << ['§key', 'primary_id', 'local_id', 'erc_what', 'erc_who', 'erc_when']
      @keymap.batchkeys.each do |k|
        csv << [k[@keymap.url_prefix.length..], '', '', '', '', '']
      end
    end
  end

  def other_data(_metadata)
    checkm_header +
      manifest_urls(@keymap.otherkeys, @keymap.url_prefix) +
      checkm_footer
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
        desc: k.to_s
      })
    end
    %w[batchobject].each do |k|
      arr.append({
        desc: k.to_s,
        download: "#{@prefixpath}#{k}.checkm",
        csv: "#{@prefixpath}#{k}.csv"
      })
    end
    karr = []
    (1..@keymap.maxdepth - 1).each do |i|
      karr.append("batch.depth#{i}")
      karr.append("batch.depth-#{i}")
    end
    karr.each do |k|
      arr.append({
        url: "#{@prefixpath}#{k}",
        desc: k.to_s,
        download: "#{@prefixpath}#{k}.checkm",
        csv: "#{@prefixpath}#{k}.csv"
      })
    end
    arr
  end

  def components
    arr = []
    return arr if @mode == :directory

    @keymap.component_data[:recs].each do |k, v|
      arr.append(
        {
          url: k.to_s,
          desc: "#{k} (#{v})"
        }
      )
    end
    arr
  end
end

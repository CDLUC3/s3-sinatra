# frozen_string_literal: true

## class to store results of S3 search
class Keymap
  def initialize(prefix = '', depth = 0, dns: 'foo.bar', credentials: nil)
    @prefix = prefix
    @prefixpath = prefix.empty? ? '/' : "/#{prefix}/"
    @depth = depth
    @keys = {}
    @dns = dns
    @credentials = credentials
    @allkeys = []
    @other = {}
    @topkeys = []
    @topdirs = {}
  end

  def topkey
    return @prefix.to_sym if @keys.key?(@prefix.to_sym)
    return :'.' if @keys.key?(:'.')

    nil
  end

  def length
    @keys.length
  end

  def maxdepth
    topkey.nil? ? 0 : @keys[topkey][:maxdepth]
  end

  def mindepth
    topkey.nil? ? 0 : @keys[topkey][:mindepth]
  end

  def empty?
    @keys.empty?
  end

  def topkeys
    arr = []
    @topkeys.each do |k|
      rec = {
        url: "#{@prefixpath}#{k}",
        desc: k
      }
      arr.append(rec)
    end
    arr
  end

  def topdirs
    arr = []
    @topdirs.each_key do |k|
      rec = @keys[k.to_sym]
      next if rec.nil?

      rec[:url] = "#{@prefixpath}#{k}/"
      rec[:desc] = "#{rec[:key]}/ (Depth: #{rec[:mindepth]};/#{rec[:maxdepth]}; Count: #{rec[:fkeys].length})"
      arr.append(rec)
    end
    arr
  end

  def allkeys
    @allkeys.map do |k|
      "#{url_prefix}#{k}"
    end
  end

  def otherkeys
    component_data
    @other.map do |k|
      "#{url_prefix}#{k}"
    end
  end

  def url_prefix
    @credentials.nil? ? "https://#{@dns}#{@prefixpath}" : "https://#{@credentials.join(':')}@#{@dns}#{@prefixpath}"
  end

  def batchkeys
    arr = []
    component_data[:batchrecs].each_key do |k|
      url = @credentials.nil? ? "https://#{@dns}#{k}" : "https://#{@credentials.join(':')}@#{@dns}#{k}"
      arr.append(url)
    end
    arr
  end

  def load(file = File::NULL)
    File.open(file) do |filerec|
      filerec.each do |line|
        add_node(line)
      end
    end
  end

  def self.metadata
    'merritt.metadata.csv'
  end

  def add_node(k)
    k.strip!
    return if File.basename(k) == Keymap.metadata
    return if k == @prefix
    return unless k.start_with?(@prefix)

    k = k[(@prefix.length + 1)..] unless @prefix.empty?
    @topdirs[k.chop] = 1 if k =~ %r{^[^/]+/$}
    return if k =~ %r{/$}
    return if k.empty?

    kdepth = k.split('/').length
    rdepth = -1
    p = File.dirname(k)

    # special handling for top level keys
    @topkeys.append(k.to_sym) if p == '.'
    @topkeys.append(k.to_sym) if p == @prefix
    @allkeys.append(k.to_sym)

    # iterate over parent keys
    loop do
      is_top = ['.', @prefix].include?(p)
      is_top ? 0 : kdepth - p.split('/').length
      pdepth = is_top ? 0 : p.split('/').length
      @keys[p.to_sym] = @keys.fetch(
        p.to_sym,
        {
          key: p.to_sym,
          fkeys: [],
          mindepth: kdepth,
          maxdepth: kdepth,
          depth: pdepth,
          rdepth: is_top ? 0 : rdepth
        }
      )
      rec = @keys[p.to_sym]
      rec[:fkeys].append(k[(p.length + 1)..])
      rec[:mindepth] = [rec[:mindepth], kdepth].min
      rec[:maxdepth] = [rec[:maxdepth], kdepth].max
      break if ['.', @prefix].include?(p)

      pp = File.dirname(p)
      @topdirs[p] = 1 if pp == '.'
      p = pp
      rdepth -= 1
    end
  end

  def report
    rpt = component_data
    puts rpt[:title]
    puts '--------------'
    count = 0
    rpt[:recs].each do |k, c|
      count += c
      puts format('%6d %s', c, k)
    end
    puts format('%6d %s', count, 'TOTAL')
  end

  def component_data
    rpt = {
      prefix: @prefix,
      depth: @depth,
      title: '',
      recs: {},
      batchrecs: {}
    }

    @other = {}
    skipOther = @allkeys.length > 40_000
    if @depth.zero?
      rpt[:title] = "#{@prefixpath}object.checkm"
      rpt[:recs]['object.checkm'] = @allkeys.length
      rpt[:batchrecs]["#{@prefixpath}object.checkm"] = @allkeys.length
    else
      rpt[:title] = "#{@prefix}/batch.depth#{@depth}.checkm"
      @other = @allkeys.clone unless skipOther
      @keys.keys.sort.each do |k|
        rec = @keys[k]
        next unless rec[:depth] == @depth || rec[:rdepth] == @depth

        rpt[:recs]["#{k}/object.checkm"] = rec[:fkeys].length
        rpt[:batchrecs]["#{@prefixpath}#{k}/object.checkm"] = rec[:fkeys].length
        rec[:fkeys].each do |dk|
          @other.delete(:"#{k}/#{dk}") unless skipOther
        end
      end
    end
    if skipOther
      rpt[:recs]["batch-other.depth#{@depth}.checkm"] = 'tbd'
      rpt[:batchrecs]["#{@prefixpath}batch-other.depth#{@depth}.checkm"] = 'tbd'
    elsif !@other.empty?
      rpt[:recs]["batch-other.depth#{@depth}.checkm"] = @other.length
      rpt[:batchrecs]["#{@prefixpath}batch-other.depth#{@depth}.checkm"] = @other.length
    end
    rpt
  end
end

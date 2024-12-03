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
    return @prefix if @keys.key?(@prefix)
    return '.' if @keys.key?('.')
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
    @topdirs.keys.each do |k|
      rec = @keys[k]
      next if rec.nil?
      rec[:url] = "#{@prefixpath}#{k}/"
      rec[:desc] = "#{rec[:key]}/ (Depth: #{rec[:mindepth]};/#{rec[:maxdepth]}; Count: #{rec[:fkeys].length})"
      arr.append(rec)
    end
    arr
  end

  def allkeys
    arr = []
    @allkeys.each do |k|
        arr.append("#{url_prefix}#{k}")
    end
    arr
  end

  def otherkeys
    arr = []
    component_data
    @other.each do |k|
        arr.append("#{url_prefix}#{k}")
    end
    arr
  end

  def url_prefix
    @credentials.nil? ? "https://#{@dns}#{@prefixpath}" : "https://#{@credentials.join(':')}@#{@dns}#{@prefixpath}"
  end

  def batchkeys
    arr = []
    component_data[:batchrecs].keys.each do |k|
        url = @credentials.nil? ? "https://#{@dns}#{k}" : "https://#{@credentials.join(':')}@#{@dns}#{k}"
        arr.append(url)
    end
    arr
  end

  def load(file = '/dev/null')
    File.open(file) do |filerec|
      filerec.each do |line|
        add_node(line)
      end
    end
  end

  def self.metadata
    "merritt.metadata.csv"
  end

  def add_node(k)
    k.strip!
    return if File.basename(k) == Keymap.metadata
    return if k == @prefix
    return unless k.start_with?(@prefix)

    k = @prefix.empty? ? k : k[@prefix.length+1..]
    @topdirs[k.chop] = 1 if k =~ /^[^\/]+\/$/
    return if k =~ /\/$/
    return if k.empty?

    kdepth = k.split('/').length
    rdepth = -1
    p = File.dirname(k)

    # special handling for top level keys
    if p == '.'
      @topkeys.append(k)
    end
    if p == @prefix
      @topkeys.append(k)
    end
    @allkeys.append(k)

    # iterate over parent keys
    loop do
      is_top = p == '.' || p == @prefix
      pdepth = is_top ? 0 : kdepth - p.split('/').length
      pdepth = is_top ? 0 : p.split('/').length
      @keys[p] = @keys.fetch(
        p, 
        {
          key: p,
          fkeys: [], 
          mindepth: kdepth, 
          maxdepth: kdepth, 
          depth: pdepth, 
          rdepth: is_top ? 0 : rdepth
        }
      )
      rec = @keys[p]
      rec[:fkeys].append(k[p.length+1..])
      rec[:mindepth] = [rec[:mindepth], kdepth].min
      rec[:maxdepth] = [rec[:maxdepth], kdepth].max
      break if p == '.' || p == @prefix
      pp = File.dirname(p)
      @topdirs[p] = 1 if pp == '.'
      p = pp
      rdepth -= 1
      break if rdepth < -20 # temp debug
    end
  end

  def report
    rpt = component_data
    puts rpt[:title]
    puts '--------------'
    count = 0
    rpt[:recs].each do |k, c|
      count += c
      puts sprintf("%6d %s", c, k)
    end
    puts sprintf("%6d %s", count, 'TOTAL')
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
    if @depth == 0
      rpt[:title] = "#{@prefixpath}object.checkm"
      rpt[:recs]["object.checkm"] = @allkeys.length
      rpt[:batchrecs]["#{@prefixpath}object.checkm"] = @allkeys.length
    else 
      rpt[:title] = "#{@prefix}/batch.depth#{@depth}.checkm"
      @other = @allkeys.clone
      @keys.keys.sort.each do |k|
        rec = @keys[k]
        next unless rec[:depth] == @depth || rec[:rdepth] == @depth
        rpt[:recs]["#{k}/object.checkm"] = rec[:fkeys].length
        rpt[:batchrecs]["#{@prefixpath}#{k}/object.checkm"] = rec[:fkeys].length
        rec[:fkeys].each do |dk|
          @other.delete("#{k}/#{dk}")
        end
      end
    end
    unless @other.empty?
      rpt[:recs]["batch-other.depth#{@depth}.checkm"] = @other.length
      rpt[:batchrecs]["#{@prefixpath}batch-other.depth#{@depth}.checkm"] = @other.length
    end
    rpt
  end
end

class Keymap
  def initialize(prefix = '', depth = 0)
    @prefix = prefix
    @depth = depth
    @keys = {}
    @allkeys = []
    @topkeys = []
    @topdirs = []
  end

  def topkeys
    @topkeys
  end

  def topdirs
    @topdirs
  end

  def load(file = '/dev/null')
    File.open(file) do |filerec|
      filerec.each do |line|
        add_node(line)
      end
    end
  end

  def add_node(k)
    k.strip!
    return unless k.start_with?(@prefix)
    if k =~ /\/$/
      @topdirs.append(k) if k.split('/').length == 1
      return
    end

    kdepth = k.split('/').length
    rdepth = -1
    p = File.dirname(k)

    # special handling for top level keys
    if p == '.'
      @topkeys.append(k)
    end
    if p == @prefix
      @topkeys.append(k[@prefix.length+1..])
    end
    @allkeys.append(k)

    # iterate over parent keys
    loop do
      is_top = p == '.' || p == @prefix
      pdepth = is_top ? 0 : kdepth - p.split('/').length
      pdepth = is_top ? 0 : p.split('/').length
      @keys[p] = @keys.fetch(p, {fkeys: [], mindepth: kdepth, maxdepth: kdepth, depth: pdepth, rdepth: is_top ? 0 : rdepth})
      rec = @keys[p]
      rec[:fkeys].append(k[p.length+1..])
      rec[:mindepth] = [rec[:mindepth], kdepth].min
      rec[:maxdepth] = [rec[:maxdepth], kdepth].max
      break if p == '.' || p == @prefix
      p = File.dirname(p)
      rdepth -= 1
    end
  end

  def report
    rpt = report_data
    puts rpt[:title]
    puts '--------------'
    count = 0
    rpt[:recs].each do |k, c|
      count += c
      puts sprintf("%6d %s", c, k)
    end
    puts sprintf("%6d %s", count, 'TOTAL')
  end

  def report_data
    rpt = {
      title: '', 
      recs: {}
    }

    other = {}
    if @depth == 0
      rpt[:title] = "#{@prefix}/object.checkm"
      rpt[:recs]["#{@prefix}/object.checkm"] = @allkeys.length
    else 
      rpt[:title] = "#{@prefix}/batch.depth#{@depth}.checkm"
      other = @allkeys.clone
      @keys.keys.sort.each do |k|
        rec = @keys[k]
        next unless rec[:depth] == @depth || rec[:rdepth] == @depth
        rpt[:recs]["#{k}/object.checkm"] = rec[:fkeys].length
        rec[:fkeys].each do |dk|
          other.delete("#{k}/#{dk}")
        end
      end
    end
    unless other.empty?
      rpt[:recs]["#{@prefix}/batch-other.depth#{@depth}.checkm"] = other.length
    end
    rpt
  end
end

class Keymap
  def initialize
    @keys = {}
    @allkeys = []
    @topkeys = []
  end

  def self.args(argv)
    {
      file: argv.length > 0 ? argv[0] : 'testdata2.txt',
      prefix: argv.length > 1 ? argv[1]: '',
      depth: argv.length > 2 ? argv[2].to_i : 0
    }
  end

  def load(file: '/dev/null', prefix: '')
    File.open(file) do |filerec|
      filerec.each do |line|
        line.strip!
        next if line =~ /\/$/
        next unless line.start_with?(prefix)
        add_node(prefix, line)
      end
    end
  end

  def add_node(prefix, k)
    depth = k.split('/').length
    rdepth = -1
    p = File.dirname(k)

    # special handling for top level keys
    if p == '.'
      @topkeys.append(k)
    end
    if p == prefix
      @topkeys.append(k[prefix.length+1..])
    end
    @allkeys.append(k)

    # iterate over parent keys
    loop do
      is_top = p == '.' || p == prefix
      pdepth = is_top ? 0 : depth - p.split('/').length
      pdepth = is_top ? 0 : p.split('/').length
      @keys[p] = @keys.fetch(p, {fkeys: [], mindepth: depth, maxdepth: depth, depth: pdepth, rdepth: is_top ? 0 : rdepth})
      rec = @keys[p]
      rec[:fkeys].append(k[p.length+1..])
      rec[:mindepth] = [rec[:mindepth], depth].min
      rec[:maxdepth] = [rec[:maxdepth], depth].max
      break if p == '.' || p == prefix
      p = File.dirname(p)
      rdepth -= 1
    end

    @keys.keys.sort.each do |k|
      #puts "#{k} #{@keys[k][:depth]}"
    end
  end

  def report(prefix, depth)
    rpt = report_data(prefix, depth)
    puts rpt[:title]
    puts '--------------'
    count = 0
    rpt[:recs].each do |k, c|
      count += c
      puts sprintf("%6d %s", c, k)
    end
    puts sprintf("%6d %s", count, 'TOTAL')
  end

  def report_data(prefix, depth)
    rpt = {
      title: '', 
      recs: {}
    }

    other = {}
    if depth == 0
      rpt[:title] = "#{prefix}/object.checkm"
      rpt[:recs]["#{prefix}/object.checkm"] = @allkeys.length
    else 
      rpt[:title] = "#{prefix}/batch.depth#{depth}.checkm"
      other = @allkeys.clone
      @keys.keys.sort.each do |k|
        rec = @keys[k]
        next unless rec[:depth] == depth || rec[:rdepth] == depth
        rpt[:recs]["#{k}/object.checkm"] = rec[:fkeys].length
        rec[:fkeys].each do |dk|
          other.delete("#{k}/#{dk}")
        end
      end
    end
    unless other.empty?
      rpt[:recs]["#{prefix}/batch-other.depth#{depth}.checkm"] = other.length
    end
    rpt
  end
end

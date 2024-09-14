require_relative 'app/lib/listing.rb'

@listing = Listing.new(region: 'us-west-2', bucket: ARGV[0], maxpre: 30)
@listing.list_keys

puts 'DIRS'
@listing.prefixes.each do |k|
  puts "\t#{k[:key]}/ depth:#{k[:depth]}; count:#{k[:count]}"
end
puts
puts 'OBJS'
@listing.topobjlist.each do |k|
  puts "\t#{k[:key]}"
end
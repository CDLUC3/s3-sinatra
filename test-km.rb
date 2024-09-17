require_relative 'app/lib/keymap.rb'

file = ARGV.length > 0 ? ARGV[0] : 'testdata2.txt'
prefix = ARGV.length > 1 ? ARGV[1]: ''
depth = ARGV.length > 2 ? ARGV[2].to_i : 0
km = Keymap.new(prefix, depth)
km.load(file)
km.report
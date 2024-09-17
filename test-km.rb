require_relative 'app/lib/keymap.rb'

km = Keymap.new
args = Keymap.args(ARGV)
km.load(file: args[:file], prefix: args[:prefix])
km.report(args[:prefix], args[:depth])
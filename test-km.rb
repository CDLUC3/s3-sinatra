# frozen_string_literal: true

require_relative 'app/lib/keymap'

file = ARGV.length.positive? ? ARGV[0] : 'testdata2.txt'
prefix = ARGV.length > 1 ? ARGV[1] : ''
depth = ARGV.length > 2 ? ARGV[2].to_i : 0
km = Keymap.new(prefix, depth)
km.load(file)
km.report

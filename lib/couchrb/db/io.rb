module CouchRB
  module Db
    class IO
      attr_reader :path, :options

      def initialize(path, options = {})
        @path, @options = path, options
      end

      def read_binary(pos, length)
        open do |io|
          io.pos = pos
          io.read(length)
        end
      end

      def write_binary(pos, binary)
        open do |io|
          io.pos = pos
          io.write(binary.to_binary)
        end
      end

      def expand(length)
        open do |io|
          io.pos = io.lstat.size
          io.write("\0" * length)
        end
      end

      def append_term(term)
        open do |io|
          io.pos = io.lstat.size
          io.write(term.to_binary)
        end
      end

      def append_binary(binary)
        open do |io|
          io.pos = io.lstat.size
          io.write(binary)
        end
      end

      def read_term(pos)
        open do |io|
          io.pos = io
          return Term.new(io).next
        end
      end

      def size
        open do |io|
          return io.lstat.size
        end
      end

      if //.respond_to?(:kcode) # 1.8
        def open(mode = 'r', &block)
          ::File.open(@path, mode, &block)
        end
      else # 1.9
        def open(mode = 'r', &block)
          ::File.open(@path, "#{mode}:ASCII-8BIT", &block)
        end
      end
    end
  end
end

require 'digest/md5'
require 'couchrb/db/io'
require 'couchrb/db/term'
require 'couchrb/db/header'

module CouchRB
  module Db
    class File < Db::IO
      attr_reader :header, :docs

      def parse
        open do |io|
          read_headers(io)
          read_docs(io)
        end
      end

      private

      def write_header(header)
        # write_binary(0, header)
      end

      def read_headers(io)
        header1 = Header.parse(io)
        header2 = Header.parse(io)

        if header1.ok?
          if header2.ok?
            return @header = header1
          else
            return @header = header1
          end
        elsif header2.ok?
          return @header = header2
        else
          raise "Headers corrupt"
        end
      end

      def read_docs(io)
        Term.new(io).each do |term|
          # p term
        end
      end

      class Document
        def self.parse(io)
          new(Term.new(io).next)
        end

        def initialize(*args)
          @args = args
        end
      end
    end
  end
end

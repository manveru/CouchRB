require 'digest/md5'
require 'couchrb/couch/io'
require 'couchrb/couch/term'
require 'couchrb/couch/header'

module CouchRB
  class CouchFile < CouchIO
    def write_header(header)
      write_binary(0, header)
    end

    def read_header
      open do |io|
        return CouchHeader.parse(io)
      end
    end
  end
end

__END__

=begin

  # Attempts to read (and eventually write) the CouchDB files directly.
  #
  # The format of CouchDB files consists of two headers for redundancy.
  # Headers are described at {CouchRB::CouchFile::Header}.
  #
  # All valuable information is stored as Erlang Terms except for the header
  # prefix and checksums.
  class CouchFile
    attr_reader :header1, :header2

    def initialize(path)
      @path = path
    end

    def parse(&block)
      File.open(@path, 'r:ASCII-8BIT'){|io|
        header = parse_headers(io)
        parse_documents(io, &block)
      }
    end

    # unimplemented
    def parse_documents(io, &block)
      raise(ArgumentError, "IO is at EOF") if io.eof?

      @documents = []

      ErlangTerm.new(io).parse(&block)
    end

    def parse_headers(io)
      @header1 = parse_header(io)
      @header2 = parse_header(io)

      if @header1.ok?
        if @header2.ok?
          if @header1 == @header2
            return @header1
          else # headers differ, log it and take the first
            puts("Header version differences.")
            puts("Primary Header: %p" % @header1)
            puts("Secondary Header: %p" % @header2)
            return @header1
          end
        else # header2 failed, fall back to header1
          puts("Secondary header corruption. Using primary header.")
          return @header1
        end
      elsif @header2.ok?
        puts("Primary header corruption. Using secondary header.")
        return @header2
      else
        puts("Fatal failure, both headers corrupted.")
      end
    end

    def parse_header(io)
      Header.parse(io)
    end
    end
    end
=end

    # A header consists of prefix, body, and a MD5 checksum.
    #
    # Every CouchDB file contains two headers for redundancy at the start of
    # the file, the size of each header is 2048 bytes, the prefix, body, and
    # checksum are each separated by a null byte.
    #
    # The prefix is "gmk", that should help us identify potential CouchDB files.
    #
    # The body of the header is usually much smaller than the available header
    # space, and it simply pads with null bytes until the checksum.
    #
    # The checksum is calculated only for the binary header body with padding
    # nulls, not for the complete file.
    #
    # On every update, the writing sequence is:
    # * Write first header
    # * Write data
    # * Write second header
    #
    # In case of a mismatch of the headers, the one that is not corrupt will
    # take priority, there is a fatal failure if both checksums don't match.
    #
    # According to the CouchDB source, a header body consists of:
    #
    #   update_seq
    #   summary_stream_state
    #   docinfo_by_seq_btree_state
    #   fulldocinfo_by_id_btree_state
    #   local_docs_btree_state
    #   admins_ptr
    #   revs_limit
    #
    # I'll need to check Erlang source to decode that information.

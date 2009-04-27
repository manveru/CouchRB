module CouchRB
  # A header consists of prefix, body, and a MD5 checksum.
  #
  # Every CouchDB file contains two headers for redundancy at
  # the start of the file, the size of each header is 2048
  # bytes, the prefix, body, and checksum are each separated
  # by a null byte.
  #
  # The prefix is "gmk", that should help us identify
  # potential CouchDB files.
  #
  # The body of the header is usually much smaller than the
  # available header space, and it simply pads with null bytes
  # until the checksum.
  #
  # The checksum is calculated only for the binary header body
  # with padding nulls, not for the complete file.
  #
  # On every update, the writing sequence is:
  # * Write first header
  # * Write data
  # * Write second header
  #
  # In case of a mismatch of the headers, the one that is not
  # corrupt will take priority, there is a fatal failure if
  # both checksums don't match.
  #
  # According to CouchDB source, a header body consists of:
  #
  #   update_seq
  #   summary_stream_state
  #   docinfo_by_seq_btree_state
  #   fulldocinfo_by_id_btree_state
  #   local_docs_btree_state
  #   admins_ptr
  #   revs_limit
  #
  # Right now we don't have all this data, but we're damn
  # close.
  class CouchHeader
    SIZE = 2048
    PREFIX = "gmk\0"
    PREFIX_SIZE = PREFIX.bytesize
    MD5_SIZE = 16
    BODY_SIZE = SIZE - PREFIX_SIZE - MD5_SIZE

    def self.parse(io)
      instance = new
      instance.parse(io)
      instance
    end

    attr_reader :prefix, :body, :md5

    def parse(io)
      parse_prefix(io)
      parse_body(io)
      parse_md5(io)
    end

    def parse_prefix(io)
      @prefix = io.read(PREFIX_SIZE).unpack('a*')[0]
      raise "Invalid header prefix" unless @prefix == PREFIX
    end

    def parse_body(io)
      @body = Body.parse(io)
    end

    def parse_md5(io)
      @md5 = io.read(MD5_SIZE).unpack('H*')[0]
      @real_md5 = @body.checksum
      raise "Invalid checksum" unless ok?
    end

    def ok?
      @real_md5 == @md5
    end

    def ==(header)
      @md5 == header.md5
    end

    def write_headers(io)
      io.open do |file|
        file.write binary_prefix
        file.write binary_body
        file.write binary_checksum
      end
    end

    # Body consists of meta-information needed to
    # keep the db sane.
    #
    # Parsing it almost drove me insane, but it is done!
    class Body
      def self.parse(io)
        terms = ErlangTerm.new(io)

        all = [
          term_name                     = terms.next,
          disk_version                  = terms.next,
          update_seq                    = terms.next,
          summary_stream_state          = terms.next,
          fulldocinfo_by_id_btree_state = terms.next,
          docinfo_by_seq_btree_state    = terms.next,
          local_docs_btree_state        = terms.next,
          purge_seq                     = terms.next,
          purged_docs                   = terms.next,
          admins_ptr                    = terms.next,
          revs_limit                    = terms.next,
        ]

        new(*all)
      end

      def initialize(*terms)
        @terms = terms
      end

      def checksum
        Digest::MD5.hexdigest('123')
      end
    end
  end
end
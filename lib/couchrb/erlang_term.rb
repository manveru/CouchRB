module CouchRB
  # External Term Format
  #
  # The external term format is mainly used in the distribution mechanism of
  # Erlang.
  # Since Erlang has a fixed number of types, there is no need for a programmer
  # to define a specification for the external format used within some
  # application.
  # All Erlang terms has an external representation and the interpretation of
  # the different terms are application specific.
  # In Erlang the BIF term_to_binary/1,2 is used to convert a term into the
  # external format.
  # To convert binary data encoding a term the BIF binary_to_term/1 is used.
  # The distribution does this implicitly when sending messages across node
  # boundaries.
  #
  # The overall format of the term format is:
  # | 1   | 1   | N
  # | 131 | Tag | Data
  #
  # A compressed term looks like this:
  # | 1   | 1  | 4                | N
  # | 131 | 80 | UncompressedSize | Zlib-compressedData
  #
  # Uncompressed Size (unsigned 32 bit integer in big-endian byte order) is the
  # size of the data before it was compressed.
  #
  # The compressed data has the following format when it has been expanded:
  # | 1   | Uncompressed Size
  # | Tag | Data
  #
  # Headache, here we come!
  #
  # Alright, this works... somewhat, there are some things that the spec didn't
  # tell us expclicitly, and thank god that CouchDB doesn't use the compressed
  # serialization (guess for performance reasons?).
  module ErlangTerm
    TYPE_MAP = {
       67 => :cached_atom_ext,
       70 => :new_float_ext,
       77 => :bit_binary_ext,
       78 => :new_cache_ext,
       97 => :small_integer_ext,
       98 => :integer_ext,
      100 => :atom_ext,
      101 => :reference_ext,
      102 => :port_ext,
      103 => :pid_ext,
      104 => :small_tuple_ext,
      105 => :large_tuple_ext,
      107 => :string_ext,
      108 => :list_ext,
      109 => :binary_ext,
      110 => :small_big_ext,
      111 => :large_big_ext,
      112 => :new_fun_ext,
      113 => :export_ext,
      114 => :new_reference_ext,
      117 => :fun_ext,
      131 => :term_ext,
    }
    TYPE_IDS = TYPE_MAP.keys

    def self.parse(io)
      while result = step(io)
        case result
        when :null, :skip, :nil
        else
          yield(result) if block_given?
        end
      end
    end

    def self.step(io)
      return unless tag = io.read(1)

      id = tag.unpack('H*')[0].to_i(16)

      case id
      when 0, "\000", /0/; :null
      when 106; :nil
      else
        if handler = TYPE_MAP[id]
          send(handler, io)
        else
          :skip
        end
      end
    end

    # | 1  | 1
    # | 67 | index
    #
    # When the atom cache is in use, index is the slot number in which the
    # atom MUST be located.
    def self.cached_atom_ext(io)
      index = io.read(1).unpack('H*')[0].to_i(16)

      log "Atom (cached)", :index => index

      index
    end

    # 1  | 8
    # 70 | IEEE float
    #
    # A float is stored as 8 bytes in big-endian IEEE format. This term is used
    # in minor version 1 of the external format.
    def self.new_float_ext(io)
      float = io.read(8).unpack('g')[0]

      log "Float (new)", :float => float

      float
    end

    # | 1  | 4      | 1    | Length
    # | 77 | Length | Bits | Data
    #
    # This term represents a bitstring whose length in bits is not a multiple
    # of 8 (created using the bit syntax in R12B and later).
    # The Length field is an unsigned 4 byte integer (big endian).
    # The Bits field is the number of bits that are used in the last byte in
    # the data field, counting from the most significant bit towards the least
    # significant.
    def self.bit_binary_ext(io)
      length = io.read(4).unpack('N')[0]
      bits   = io.read(1).unpack('H*')[0].to_i(16)
      data   = io.read(length)

      log "Binary (bit)", :length => length, :bits => bits, :data => data

      data
    end

    # | 1  | 1     | 2      | Length
    # | 78 | index | Length | Atom name
    #
    # NEW_CACHE works just like ATOM_EXT, but it must also cache the atom in
    # the atom cache in the location given by index.
    # The atom cache is currently only used between real Erlang nodes (not
    # between Erlang nodes and C or Java nodes).
    def self.new_cache_ext(io)
      index     = io.read(1).unpack('H*')[0]
      length    = io.read(2).unpack('n')[0]
      atom_name = io.read(length)

      log "Cache (new)", :index => index, :length => length, :atom_name => atom_name

      [index, atom_name]
    end

    # |  1 | 4
    # | 98 | Int
    #
    # Signed 32 bit integer in big-endian format (i.e. MSB first)
    def self.small_integer_ext(io)
      int = io.read(4).unpack('n')[0]

      log "Integer (small)", :int => int

      int
    end

    # | 1  | 4
    # | 98 | Int
    #
    # Signed 32 bit integer in big-endian format (i.e. MSB first)
    def self.integer_ext(io)
      int = io.read(4).unpack('N')[0]

      log "Integer", :int => int

      int
    end

    # | 1   | 2      | Length
    # | 100 | Length | AtomName
    #
    # An atom is stored with a 2 byte unsigned length in big-endian order,
    # followed by Length numbers of 8 bit characters that forms the AtomName.
    # Note: The maximum allowed value for Length is 255.
    def self.atom_ext(io)
      length    = io.read(2).unpack('n')[0]
      atom_name = io.read(length)

      log "Atom", :length => length, :atom_name => atom_name

      atom_name
    end

    # | 1   | N    | 4  | 1
    # | 101 | Node | ID | Creation
    #
    # Encode a reference object (an object generated with make_ref/0).
    # The Node term is an encoded atom, i.e. ATOM_EXT, NEW_CACHE or
    # CACHED_ATOM.
    # The ID field contains a big-endian unsigned integer, but should be
    # regarded as uninterpreted data since this field is node specific.
    # Creation is a byte containing a node serial number that makes it possible
    # to separate old (crashed) nodes from a new one.
    # In ID, only 18 bits are significant; the rest should be 0.
    # In Creation, only 2 bits are significant; the rest should be 0.
    # See NEW_REFERENCE_EXT.
    def self.reference_ext(io)
      node     = step(io)
      id       = io.read(4).unpack('N')[0]
      creation = io.read(1).unpack('H*')[0].to_i(16)

      log "Reference (old)", :node => node, :id => id, :creation => creation

      [node, id, creation]
    end

    # | 1   | N    | 4  | 1
    # | 102 | Node | ID | Creation
    #
    # Encode a port object (obtained form open_port/2).
    # The ID is a node specific identifier for a local port.
    # Port operations are not allowed across node boundaries.
    # The Creation works just like in reference_ext.
    #
    # This seems to be useless for us, only used in Erlang internals?
    def self.port_ext(io)
      node     = step(io)
      id       = io.read(4).unpack('N')[0]
      creation = io.read(1).unpack('H*')[0].to_i(16)

      log "Port", :node => node, :id => id, :creation => creation

      [node, id, creation]
    end

    # | 1   | N    | 4  | 4      | 1
    # | 103 | Node | ID | Serial | Creation
    #
    # Encode a process identifier object (obtained from spawn/3 or friends).
    # The ID and Creation fields works just like in REFERENCE_EXT, while the
    # Serial field is used to improve safety.
    # In ID, only 15 bits are significant; the rest should be 0.
    def self.pid_ext(io)
      node     = step(io)
      id       = io.read(4).unpack('N')[0]
      serial   = io.read(4).unpack('N')[0]
      creation = io.read(1).unpack('H*')[0].to_i(16)

      log "PID", :node => node, :id => id, :serial => serial, :creation => creation

      [node, id, serial, creation]
    end

    # 1   | 1     | N
    # 104 | Arity | Elements
    #
    # SMALL_TUPLE_EXT encodes a tuple.
    # The Arity field is an unsigned byte that determines how many element that
    # follows in the Elements section.
    def self.small_tuple_ext(io)
      arity = io.read(1).unpack('H*')[0].to_i(16)
      tuple = Array.new(arity){ step(io) }

      log "Tuple (small)", :arity => arity, :tuple => tuple

      tuple
    end

    # | 1   | 4     | N
    # | 105 | Arity | Elements
    #
    # Same as SMALL_TUPLE_EXT with the exception that Arity is an unsigned 4
    # byte integer in big endian format.
    def self.large_tuple_ext(io)
      arity = io.read(4).unpack('N')[0]
      tuple = Array.new(arity){ step(io) }

      log "Tuple (large)", :arity => arity, :tuple => tuple

      tuple
    end

    # | 1   | 2      | Length
    # | 107 | Length | Characters
    #
    # String does NOT have a corresponding Erlang representation, but is an
    # optimization for sending lists of bytes (integer in the range 0-255) more
    # efficiently over the distribution.
    # Since the Length field is an unsigned 2 byte integer (big endian),
    # implementations must make sure that lists longer than 65535 elements are
    # encoded as LIST_EXT.
    def self.string_ext(io)
      length = io.read(2).unpack('n')[0]
      chars  = io.read(length)
      log "String", :length => length, :chars => chars
      chars
    end

    # | 1   | 4      | Â        | Â 
    # | 108 | Length | Elements | Tail
    #
    # Length is the number of elements that follows in the Elements section.
    # Tail is the final tail of the list; it is NIL_EXT for a proper list, but
    # may be anything type if the list is improper (for instance [a|b]).
    def self.list_ext(io)
      length = io.read(4).unpack('N')[0]
      list   = Array.new(length){ step(io) }
      log "List", :length => length, :list => list
      list
    end

    # | 1   | 4      | Length
    # | 109 | Length | Data
    #
    # Binaries are generated with bit syntax expression or with
    # list_to_binary/1, term_to_binary/1, or as input from binary ports.
    # The Length length field is an unsigned 4 byte integer (big endian).
    def self.binary_ext(io)
      length = io.read(4).unpack('N')[0]
      data   = io.read(length)
      log "Binary", :length => length, :data => data
      data
    end

    # | 1   | 1 | 1    | n
    # | 110 | n | Sign | d(0) ... d(n-1)
    #
    # Bignums are stored in unary form with a Sign byte that is 0 if the binum
    # is positive and 1 if is negative.
    # The digits are stored with the LSB byte stored first.
    #
    # To calculate the integer the following formula can be used:
    # B = 256
    # (d0*B^0 + d1*B^1 + d2*B^2 + ... d(N-1)*B^(n-1))
    #
    # FIXME: Whatever this is supposed to mean
    def self.small_big_ext(io)
      n    = io.read(1).unpack('H*')[0].to_i(16)
      sign = io.read(1)
      d    = io.read(n)
      log "Big (small)", :n => n, :sign => sign, :d => d
      d
    end

    # | 1   | 4 | 1    | n
    # | 111 | n | Sign | d(0) ... d(n-1)
    #
    # Same as SMALL_BIG_EXT with the difference that the length field is an
    # unsigned 4 byte integer.
    def self.large_big_ext(io)
      n    = io.read(4).unpack('N')[0]
      sign = io.read(1)
      d    = io.read(n)
      log "Big (small)", :n => n, :sign => sign, :d => d
      d
    end

    # | 1   | 4    | 1     | 16   | 4     | 4       | N1     | N2       | N3      | N4  | N5
    # | 112 | Size | Arity | Uniq | Index | NumFree | Module | OldIndex | OldUniq | Pid | Free Vars
    #
    # This is the new encoding of internal funs: fun F/A and fun(Arg1,..) -> ... end.
    #
    # Size
    #   Total number of bytes, including the Size field.
    # Arity
    #   Arity of the function implementing the fun.
    # Uniq
    #   16 bytes MD5 of the significant parts of the Beam file.
    # Index
    #   Each fun within a module has an unique index.
    # Index
    #   Stored in big-endian byte order.
    # NumFree
    #   Number of free variables.
    # Module  
    #   Encoded as an atom, using ATOM_EXT, NEW_CACHE or CACHED_ATOM.
    #   This is the module that the fun is implemented in.
    # OldIndex
    #   An integer encoded using SMALL_INTEGER_EXT or INTEGER_EXT.
    #   It is typically a small index into the module's fun table.
    # OldUniq
    #   An integer encoded using SMALL_INTEGER_EXT or INTEGER_EXT.
    # Uniq
    #   The hash value of the parse tree for the fun.
    # Pid
    #   Process identifier as in PID_EXT.
    #   It represents the process in which the fun was created.
    # Free vars
    #   NumFree number of terms, each one encoded according to its type.
    def self.new_fun_ext(io)
      size     = io.read(4).unpack('N')[0]
      arity    = io.read(1).unpack('H*')[0].to_i(16)
      uniq     = io.read(16).unpack('H*')[0]
      index    = io.read(4).unpack('N')[0]
      num_free = io.read(4).unpack('N')[0]
    end

    # | 1   | N1     | N2       | N3
    # | 113 | Module | Function | Arity
    #
    # This term is the encoding for external funs: fun M:F/A.
    # Module and Function are atoms (encoded using ATOM_EXT, NEW_CACHE or
    # CACHED_ATOM). Arity is an integer encoded using SMALL_INTEGER_EXT.
    def self.export_ext(io)
    end

    # |  1 | 31
    # | 99 | Float String
    #
    # A float is stored in string format.
    # The format used in sprintf to format the float is "%.20e" (there are more
    # bytes allocated than necessary). To unpack the float use sscanf with
    # format "%lf".
    #
    # This term is used in minor version 0 of the external format; it has been
    # superseded by NEW_FLOAT_EXT .
    def self.float_ext(io)
      float = io.read(31)
      log "Float (old)", :float => float
      float
    end

    # The overall format of the term format is:
    # | 1   | 1   | N
    # | 131 | Tag | Data
    #
    # A compressed term looks like this:
    # | 1   | 1  | 4                | N
    # | 131 | 80 | UncompressedSize | Zlib-compressedData
    def self.term(io)
      tag = io.read(1).unpack('H')[0].to_i(16)

      log "Term", :tag => tag

      tag
    end

    def self.method_missing(*args)
      p args
    end

    def self.log(msg, hash)
      puts("%-20s: %p" % [msg, hash])
    end
  end
end

module CouchRB
  module Db
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
    class Term
      TYPE_MAP = {
        67 => :cached_atom_ext,
        70 => :new_float_ext,
        77 => :bit_binary_ext,
        78 => :new_cache_ext,
        97 => :small_integer_ext,
        98 => :integer_ext,
        99 => :float_ext,
        100 => :atom_ext,
        101 => :reference_ext,
        102 => :port_ext,
        103 => :pid_ext,
        104 => :small_tuple_ext,
        105 => :large_tuple_ext,
        106 => :nil_ext,
        107 => :string_ext,
        108 => :list_ext,
        109 => :binary_ext,
        110 => :small_big_ext,
        111 => :large_big_ext,
        112 => :new_fun_ext,
        113 => :export_ext,
        114 => :new_reference_ext,
        117 => :fun_ext,
        131 => :term,
      }

      attr_reader :io

      def initialize(io)
        @io = io
      end

      def each
        if block_given?
          while (ok, term = self.next; ok)
            yield term
          end
        else
          Enumerable::Enumerator.new(self, :each)
        end
      end

      def next
        return if io.eof?
        ok, term = step
        return term if ok
      end
      alias walking next # don't wanna type self.next

      # the maximum values for numeric
      MIN_INTEGER_SMALL = 0
      MAX_INTEGER_SMALL = ((1 << 8)    - 1)
      MIN_INTEGER_LARGE = MAX_INTEGER_SMALL + 1
      MAX_INTEGER_LARGE = ((1 << 31)   - 1)
      MIN_BIG_SMALL     = MAX_INTEGER_LARGE + 1
      MAX_BIG_SMALL     = ((1 << 2040) - 1)
      MIN_BIG_LARGE     = MAX_BIG_SMALL     + 1
      MAX_BIG_LARGE     = ((1 << 8160) - 1)

      def self.dump(obj)
        case obj
        when Integer
          abs = obj.abs

          case abs
          when (MIN_INTEGER_SMALL..MAX_INTEGER_SMALL)
            binary_join(97, [obj].pack('C'))
          when (MIN_INTEGER_LARGE..MAX_INTEGER_LARGE)
            binary_join(98, [obj].pack('l'))
          when (MIN_BIG_SMALL..MAX_BIG_SMALL)
            sign   = [abs == obj ? 0 : 1].pack('C')
            data   = [abs.to_s(16)      ].pack('h*')
            length = [data.bytesize     ].pack('C')

            binary_join(110, length, sign, data)
          when (MIN_BIG_LARGE..MAX_BIG_LARGE)
            sign   = [abs == obj ? 0 : 1].pack('C')
            data   = [abs.to_s(16)      ].pack('h*')
            length = [data.bytesize     ].pack('N')

            binary_join(111, length, sign, data)
          else
            raise("Cannot serialize numbers that large: %p" % obj)
          end
        when Float
          binary_join(70, [obj].pack('G'))
        when Symbol # We use that as our Atom
          value  = obj.to_s
          length = [value.bytesize].pack('n')
          data   = value

          binary_join(100, length, data)
        when Tuple
          binary_elements = obj.map{|e| dump(e) }
          size = binary_elements.size

          if size < 256
            binary_size = [size].pack('C')
            binary_join(104, binary_size, binary_elements)
          else
            binary_size = [size].pack('N')
            binary_join(105, binary_size, binary_elements)
          end
        when nil
          binary_join(106)
        when List, Array
          binary_elements = []
          obj.each{|l| binary_elements << dump(l) }

          size = binary_elements.size
          binary_length = [size].pack('N')

          binary_join(108, binary_length, *binary_elements)
        else
          raise("Cannot dump: %p" % [obj])
        end
      end

      private

      def self.binary_join(id, *rest)
        binary_id = [id].pack('C')
        [binary_id, *rest].join
      end

      def step
        id = read(1)

        if handler = TYPE_MAP[id]
          got = send(handler)
          return :ok, got
        elsif id == 0
          return
        else
          raise "No handler for %p" % [id]
        end
      end

      # | 1  | 1
      # | 67 | index
      #
      # When the atom cache is in use, index is the slot number
      # in which the atom MUST be located.
      #
      # NOTE: No idea about the encoding, and no idea how to
      #       check, this needs clarification. We simply assume
      #       that every length one is encoded as 8 bit unsigned
      #       int. This would limit the number of cached atoms to
      #       255... is this realistic?
      def cached_atom_ext
        read(1)
      end

      # 1  | 8
      # 70 | IEEE float
      #
      # A float is stored as 8 bytes in big-endian IEEE format.
      # This term is used in minor version 1 of the external
      # format.
      def new_float_ext
        read(8, 'G')
      end

      # | 1  | 4      | 1    | Length
      # | 77 | Length | Bits | Data
      #
      # This term represents a bitstring whose length in bits is
      # not a multiple of 8 (created using the bit syntax in R12B
      # and later).
      # The Length field is an unsigned 4 byte integer (big
      # endian).
      # The Bits field is the number of bits that are used in the
      # last byte in the data field, counting from the most
      # significant bit towards the least significant.
      def bit_binary_ext
        length, bits = io.read(5).unpack('NC')
        data = io.read(length)
      end

      # | 1  | 1     | 2      | Length
      # | 78 | index | Length | Atom name
      #
      # NEW_CACHE works just like ATOM_EXT, but it must also
      # cache the atom in the atom cache in the location given by
      # index.
      # The atom cache is currently only used between real Erlang
      # nodes (not between Erlang nodes and C or Java nodes).
      def new_cache_ext
        index, length = io.read(3).unpack('Cn')
        atom_name = io.read(length)

        [index, atom_name]
      end

      # |  1 | 1
      # | 97 | Int
      #
      # Unsigned 8 bit integer.
      def small_integer_ext
        read(1)
      end

      # | 1  | 4
      # | 98 | Int
      #
      # Signed 32 bit integer in big-endian format.
      def integer_ext
        read(4, 'l')
      end

      # | 1   | 2      | Length
      # | 100 | Length | AtomName
      #
      # An atom is stored with a 2 byte unsigned length in
      # big-endian order,
      # followed by Length numbers of 8 bit characters that forms
      # the AtomName.
      #
      # Note: The maximum allowed value for Length is 255.
      #
      # Note: Some atoms, like 'db_header' actually have a
      #       defined data structure that is bound to them.
      #       Other atoms like 'nil', 'true', and 'false' are
      #       only themselves.
      #
      # Note: We simply use Symbol here, as it maps well with the Ruby symbol
      def atom_ext
        length = read(2, 'n')
        io.read(length).to_sym
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
      # In ID, only 18 bits are significant; the rest should be 0.  In Creation,
      # only 2 bits are significant; the rest should be 0.
      # See NEW_REFERENCE_EXT.
      def reference_ext
        node = walking
        id, creation = io.read(5).unpack('NC')

        Reference.new(node, id, creation)
      end

      # | 1   | N    | 4  | 1
      # | 102 | Node | ID | Creation
      #
      # Encode a port object (obtained form open_port/2).
      # The ID is a node specific identifier for a local port.
      # Port operations are not allowed across node boundaries.
      # The Creation works just like in reference_ext.
      #
      # This seems to be useless for us, only used in Erlang
      # internals?
      def port_ext
        node = walking
        id, creation = io.read(5).unpack('NC')

        [node, id, creation]
      end

      # | 1   | N    | 4  | 4      | 1
      # | 103 | Node | ID | Serial | Creation
      #
      # Encode a process identifier object (obtained from spawn/3 or friends).
      # The ID and Creation fields works just like in REFERENCE_EXT, while the
      # Serial field is used to improve safety.
      # In ID, only 15 bits are significant; the rest should be 0.
      def pid_ext
        node = walking
        id, serial, creation = io.read(9).unpack('NNC')
        PID.new(node, id, serial, creation)
      end

      # 1   | 1     | N
      # 104 | Arity | Elements
      #
      # SMALL_TUPLE_EXT encodes a tuple.
      # The Arity field is an unsigned byte that determines how
      # many element that follows in the Elements section.
      def small_tuple_ext
        arity = read(1)
        elements = Array.new(arity){ walking }
        Tuple.new(elements)
      end

      def nil_ext
        nil
      end

      # | 1   | 4     | N
      # | 105 | Arity | Elements
      #
      # Same as SMALL_TUPLE_EXT with the exception that Arity is
      # an unsigned 4 byte integer in big endian format.
      #
      # Please note that in Ruby, Array.new argument is restricted to the size of
      # 'long'
      def large_tuple_ext
        arity = read(4, 'N')
        elements = []
        arity.times{
          break if io.eof?
          elements << walking
        }
        Tuple.new(elements)
      end

      # | 1   | 2      | Length
      # | 107 | Length | Characters
      #
      # String does NOT have a corresponding Erlang
      # representation, but is an optimization for sending lists
      # of bytes (integer in the range 0-255) more efficiently
      # over the distribution.
      # Since the Length field is an unsigned 2 byte integer (big
      # endian), implementations must make sure that lists longer
      # than 65535 elements are encoded as LIST_EXT.
      def string_ext
        length = read(2, 'n')
        chars  = io.read(length)
      end

      # | 1   | 4      | Â        | Â 
      # | 108 | Length | Elements | Tail
      #
      # Length is the number of elements that follows in the
      # Elements section.
      # Tail is the final tail of the list; it is NIL_EXT for a
      # proper list, but may be anything type if the list is
      # improper (for instance [a|b]).
      def list_ext
        length = read(4, 'N')
        list = Array.new(length){ walking }
        list
      end

      # | 1   | 4      | Length
      # | 109 | Length | Data
      #
      # Binaries are generated with bit syntax expression or with
      # list_to_binary/1, term_to_binary/1, or as input from binary ports.
      # The Length length field is an unsigned 4 byte integer (big endian).
      def binary_ext
        length = read(4, 'N')
        data   = io.read(length)
      end

      # | 1   | 1      | 1    | Length
      # | 110 | Length | Sign | d(0) ... d(n-1)
      #
      # Bignums are stored in unary form with a Sign byte that is
      # 0 if the binum is positive and 1 if is negative.
      # The digits are stored in little-endian.
      #
      # To calculate the integer the following formula can be
      # used:
      # B = 256
      # (d0*B^0 + d1*B^1 + d2*B^2 + ... d(N-1)*B^(n-1))
      #
      # FIXME: Whatever this is supposed to mean
      def small_big_ext
        length, sign = io.read(2).unpack('CC')

        data = io.read(length).unpack('C*')
        b = 256
        sum = 0

        data.each_with_index do |digit, index|
          sum += (digit * (b ** index))
        end

        sign == 0 ? sum : -sum
      end

      # | 1   | 4      | 1    | Length
      # | 111 | Length | Sign | d(0) ... d(n-1)
      #
      # Same as SMALL_BIG_EXT with the difference that the length
      # field is an unsigned 4 byte integer.
      def large_big_ext
        length, sign = io.read(5).unpack('NC')

        data = io.read(length).unpack('C*')
        b = 256
        sum = 0

        data.each_with_index do |digit, index|
          sum += (digit * (b ** index))
        end

        sign == 0 ? sum : -sum
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
      #
      # NOTE: We don't spend much effort here, there's nothing we can do with
      #       this information other than write it back again.
      def new_fun_ext
        size = read(4, 'N')
        NewFunction.new(io.read(size))
      end

      # | 1   | N1     | N2       | N3
      # | 113 | Module | Function | Arity
      #
      # This term is the encoding for external funs: fun M:F/A.
      # Module and Function are atoms (encoded using ATOM_EXT, NEW_CACHE or
      # CACHED_ATOM). Arity is an integer encoded using SMALL_INTEGER_EXT.
      def export_ext
        mod      = walking
        function = walking
        arity    = walking

        Export.new(mod, function, arity)
      end

      # | 1   | 2      | N    | 1        | N'
      # | 114 | Length | Node | Creation | ID ...
      #
      # Node and Creation are as in REFERENCE_EXT.
      # ID contains a sequence of big-endian unsigned integers (4 bytes each, so
      # N' is a multiple of 4), but should be regarded as uninterpreted data.
      # N' = 4 * Length.
      # In the first word (four bytes) of ID, only 18 bits are significant, the
      # rest should be 0.
      # In Creation, only 2 bits are significant, the rest should be 0.
      # NEW_REFERENCE_EXT was introduced with distribution version 4.
      # In version 4, N' should be at most 12.
      def new_reference_ext
        length   = read(2, 'n')
        node     = walking
        creation = read(1)
        n_prime  = read(length * 4)

        NewReference.new(length, node, creation, n_prime)
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
      #
      # For some reason, erlang is still using this instead of NEW_FLOAT_EXT.
      def float_ext
        raw = io.read(31)
        raw.to_f # Yay! To whoever came up with that binary representation.
      end

      # The overall format of the term format is:
      # | 1   | 1   | N
      # | 131 | Tag | Data
      #
      # A compressed term looks like this:
      # | 1   | 1  | 4                | N
      # | 131 | 80 | UncompressedSize | Zlib-compressedData
      def term
        walking
      end

      def read(n, format = 'C')
        io.read(n).unpack(format)[0]
      end

      def log(msg, hash)
        puts("%-20s: %p" % [msg, hash])
      end

      module ArrayLike
        def each(&block) elements.each(&block) end
        def inspect; elements.inspect; end
        def pretty_print(q); elements.pretty_print(q); end

        def <=>(other)
          if other.respond_to?(:elements)
            elements <=> other.elements
          else
            elements <=> other
          end
        end

        def [](idx)
          case idx
          when Fixnum
            elements[idx]
          else
            super
          end
        end

        def to_a
          elements.dup
        end
      end

      module PrettyTerm
        def inspect
          old = super
          full_name = self.class.name
          name = full_name.split('::').last
          old.sub("struct #{full_name}", name)
        end

        def pretty_print(q)
          name = self.class.name.split('::').last

          q.group(1, "#<#{name}", '>') {
            q.seplist(self.class.members, lambda { q.text "," }) {|member|
            q.breakable
            q.text member.to_s
            q.text '='
            q.group(1) {
              q.breakable ''
              q.pp self[member]
            }
          }
          }
        end

        def pretty_print_cycle(q)
          q.text sprintf("#<struct %s:...>", PP.mcall(self, Kernel, :class).name)
        end
      end

      # Classes to hold Erlang structures so we don't lose them

      class Export < Struct.new(:module, :function, :arity)
        include PrettyTerm
      end
      class NewFunction < Struct.new(:binary)
        include PrettyTerm
      end
      class NewReference < Struct.new(:length, :node, :creation, :n_prime)
        include PrettyTerm
      end
      class PID < Struct.new(:node, :id, :serial, :creation)
        include PrettyTerm
      end
      class Reference < Struct.new(:node, :id, :creation)
        include PrettyTerm
      end

      class Tuple < Struct.new(:elements);
        include Comparable, Enumerable, ArrayLike
      end

      class List < Struct.new(:elements)
        include Comparable, Enumerable, ArrayLike
      end
    end
  end
end

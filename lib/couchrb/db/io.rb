require 'stringio'

module CouchRB
  module Db

    # these are the calls to gen_server, apparently they should be asynchron,
    # but we'll just do it one at a time.
    #
    # This one, just like BTreeFunctions, needs a nasty argument to make sure
    # we implement it 1:1
    module GenServer
      module_function

      # handle_call({pread, Pos, Bytes}, _From, Fd) ->
      #     {reply, file:pread(Fd, Pos, Bytes), Fd};
      def pread(fd, pos, bytes)
        fd.seek(pos, ::IO::SEEK_SET)
        fd.read(bytes)
      end

      # handle_call({pwrite, Pos, Bin}, _From, Fd) ->
      #     {reply, file:pwrite(Fd, Pos, Bin), Fd};
      def pwrite(fd, pos, binary)
        fd.seek(pos, ::IO::SEEK_SET)
        fd.write(binary)
      end

      # handle_call({expand, Num}, _From, Fd) ->
      #     {ok, Pos} = file:position(Fd, eof),
      #     {reply, {file:pwrite(Fd, Pos + Num - 1, <<0>>), Pos}, Fd};
      def expand(fd, num)
        fd.seek(-1, ::IO::SEEK_END)
        pos = fd.pos
        fd.write("\0" * num)
        pos
      end

      # handle_call(bytes, _From, Fd) ->
      #     {reply, file:position(Fd, eof), Fd};
      def bytes(fd)
        fd.seek(-1, ::IO::SEEK_END)
        fd.pos
      end

      # handle_call(sync, _From, Fd) ->
      #     {reply, file:sync(Fd), Fd};
      def sync(fd)
        fd.sync
      end

      # handle_call({truncate, Pos}, _From, Fd) ->
      #     {ok, Pos} = file:position(Fd, Pos),
      #     {reply, file:truncate(Fd), Fd};
      def truncate(fd, pos)
        fd.truncate(pos)
      end

      # handle_call({append_bin, Bin}, _From, Fd) ->
      #     Len = size(Bin),
      #     Bin2 = <<Len:32, Bin/binary>>,
      #     {ok, Pos} = file:position(Fd, eof),
      #     {reply, {file:pwrite(Fd, Pos, Bin2), Pos}, Fd};
      def append_bin(fd, bin)
        len = bin.bytesize
        packed_len = [len].pack('N')
        bin2 = packed_len << bin

        pos = fd.lstat.size
        fd.pos = pos

        fd.write(bin2)

        fd.pos = pos
      ensure
        fd.sync
      end

      # handle_call({pread_bin, Pos}, _From, Fd) ->
      #     {ok, <<TermLen:32>>} = file:pread(Fd, Pos, 4),
      #     {ok, Bin} = file:pread(Fd, Pos + 4, TermLen),
      #     {reply, {ok, Bin}, Fd}.
      def pread_bin(fd, pos)
        # p :pread_bin => {:pos => pos}
        fd.pos = pos
        term_len_bytes = fd.read(4)
        term_len = term_len_bytes.unpack('N')[0]
        fd.read(term_len)
      end
    end

    class IO
      attr_reader :mode, :path, :options, :fd

      def initialize(path, mode, options = {})
        @path, @mode, @options = path, mode, options
        @fd = open
      end

      # %%----------------------------------------------------------------------
      # %% Args:    Pos is the offset from the beginning of the file, Bytes is
      # %%  is the number of bytes to read.
      # %% Returns: {ok, Binary} where Binary is a binary data from disk
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # pread(Fd, Pos, Bytes) when Bytes > 0 ->
      #     gen_server:call(Fd, {pread, Pos, Bytes}, infinity).
      def pread(pos, bytes)
        GenServer.pread(fd, pos, bytes)
      end

      # %%----------------------------------------------------------------------
      # %% Args:    Pos is the offset from the beginning of the file, Bin is
      # %%  is the binary to write
      # %% Returns: ok
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # pwrite(Fd, Pos, Bin) ->
      #     gen_server:call(Fd, {pwrite, Pos, Bin}, infinity).
      def pwrite(pos, bin)
        GenServer.pwrite(fd, pos, bin)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: To append a segment of zeros to the end of the file.
      # %% Args:    Bytes is the number of bytes to append to the file.
      # %% Returns: {ok, Pos} where Pos is the file offset to the beginning of
      # %%  the new segments.
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # expand(Fd, Bytes) when Bytes > 0 ->
      #     gen_server:call(Fd, {expand, Bytes}, infinity).
      def expand(bytes)
        GenServer.expand(fd, bytes)
      end


      # %%----------------------------------------------------------------------
      # %% Purpose: To append an Erlang term to the end of the file.
      # %% Args:    Erlang term to serialize and append to the file.
      # %% Returns: {ok, Pos} where Pos is the file offset to the beginning the
      # %%  serialized  term. Use pread_term to read the term back.
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # append_term(Fd, Term) ->
      #     append_binary(Fd, term_to_binary(Term, [compressed])).
      #
      # FIXME: Term needs compression support
      def append_term(term)
        pp :append_term => term
        # p caller
        append_binary(Term.dump(term))
      end


      # %%----------------------------------------------------------------------
      # %% Purpose: To append an Erlang binary to the end of the file.
      # %% Args:    Erlang term to serialize and append to the file.
      # %% Returns: {ok, Pos} where Pos is the file offset to the beginning the
      # %%  serialized  term. Use pread_term to read the term back.
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # append_binary(Fd, Bin) ->
      #     gen_server:call(Fd, {append_bin, Bin}, infinity).
      def append_binary(bin)
        GenServer.append_bin(fd, bin)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: Reads a term from a file that was written with append_term
      # %% Args:    Pos, the offset into the file where the term is serialized.
      # %% Returns: {ok, Term}
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # pread_term(Fd, Pos) ->
      #     {ok, Bin} = pread_binary(Fd, Pos),
      #     {ok, binary_to_term(Bin)}.
      def pread_term(pos)
        # p :pread_term => {:pos => pos}
        # p caller
        # p caller
        bin = pread_binary(pos)
        # p :bin => bin
        term = Term.new(StringIO.new(bin)).next
        # p :term => term
        term
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: Reads a binrary from a file that was written with append_binary
      # %% Args:    Pos, the offset into the file where the term is serialized.
      # %% Returns: {ok, Term}
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # pread_binary(Fd, Pos) ->
      #     gen_server:call(Fd, {pread_bin, Pos}, infinity).
      def pread_binary(pos)
        GenServer.pread_bin(fd, pos)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: The length of a file, in bytes.
      # %% Returns: {ok, Bytes}
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # % length in bytes
      # bytes(Fd) ->
      #     gen_server:call(Fd, bytes, infinity).
      def bytes
        GenServer.bytes(fd)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: Truncate a file to the number of bytes.
      # %% Returns: ok
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # truncate(Fd, Pos) ->
      #     gen_server:call(Fd, {truncate, Pos}, infinity).
      def truncate(pos)
        GenServer.truncate(fd, pos)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: Ensure all bytes written to the file are flushed to disk.
      # %% Returns: ok
      # %%  or {error, Reason}.
      # %%----------------------------------------------------------------------
      #
      # sync(Fd) ->
      #     gen_server:call(Fd, sync, infinity).
      def sync
        GenServer.sync(fd)
      end

      # %%----------------------------------------------------------------------
      # %% Purpose: Close the file. Is performed asynchronously.
      # %% Returns: ok
      # %%----------------------------------------------------------------------
      # close(Fd) ->
      #     Result = gen_server:cast(Fd, close),
      #     catch unlink(Fd),
      #     Result.
      def close
        GenServer.close(fd)
      end

      def open(mode = self.mode)
        io = ::File.open(path, mode).binmode
        io.sync = true
        io
      end
    end
  end
end

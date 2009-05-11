require 'digest/md5'
require 'couchrb/db/io'
require 'couchrb/db/term'
require 'couchrb/db/header'

module CouchRB
  module Db
    class File < Db::IO
      # size of each segment of the doubly written header
      HEADER_SIZE = 2048

      HeaderCorruption = Class.new(RuntimeError)

      # write_header(Fd, Prefix, Data) ->
      #     TermBin = term_to_binary(Data),
      #     % the size of all the bytes written to the header, including the md5 signature (16 bytes)
      #     FilledSize = size(Prefix) + size(TermBin) + 16,
      #     {TermBin2, FilledSize2} =
      #     case FilledSize > ?HEADER_SIZE of
      #     true ->
      #         % too big!
      #         {ok, Pos} = append_binary(Fd, TermBin),
      #         PtrBin = term_to_binary({pointer_to_header_data, Pos}),
      #         {PtrBin, size(Prefix) + size(PtrBin) + 16};
      #     false ->
      #         {TermBin, FilledSize}
      #     end,
      #     ok = sync(Fd),
      #     % pad out the header with zeros, then take the md5 hash
      #     PadZeros = <<0:(8*(?HEADER_SIZE - FilledSize2))>>,
      #     Sig = erlang:md5([TermBin2, PadZeros]),
      #     % now we assemble the final header binary and write to disk
      #     WriteBin = <<Prefix/binary, TermBin2/binary, PadZeros/binary, Sig/binary>>,
      #     ?HEADER_SIZE = size(WriteBin), % sanity check
      #     DblWriteBin = [WriteBin, WriteBin],
      #     ok = pwrite(Fd, 0, DblWriteBin),
      #     ok = sync(Fd).
      def write_header(fd, prefix, data)
        term_bin = Term.dump(data)
        # size of all bytes written to header, including md5 (16 bytes)
        filled_size = prefix.bytesize + term_bin.bytesize + 16

        if filled_size > HEADER_SIZE # too big!
          pos = append_binary(fd, term_bin)
          term_bin = Term.dump([:pointer_to_header_data, pos])
          filled_size = prefix.bytesize, ptr_bin.bytesize + 16
        end

        sync(fd)

        # pad out the header with zeros, then take the md5 hash
        pad_zeros = "\0" * (HEADER_SIZE - filled_size)
        sig = Digest::MD5.hexdigest(term_bin + pad_zeros)

        # now we assemble the final header binary and write to disk
        write_bin = [prefix, term_bin, pad_zeros, sig].join
        fail("Header size mismatch") unless HEADER_SIZE == write_bin.bytesize

        dbl_write_bin = [write_bin, write_bin].join
        pwrite(fd, 0, dbl_write_bin)
        sync(fd)
      end

      # read_header(Fd, Prefix) ->
      #     {ok, Bin} = couch_file:pread(Fd, 0, 2*(?HEADER_SIZE)),
      #     <<Bin1:(?HEADER_SIZE)/binary, Bin2:(?HEADER_SIZE)/binary>> = Bin,
      #     Result =
      #     % read the first header
      #     case extract_header(Prefix, Bin1) of
      #     {ok, Header1} ->
      #         case extract_header(Prefix, Bin2) of
      #         {ok, Header2} ->
      #             case Header1 == Header2 of
      #             true ->
      #                 % Everything is completely normal!
      #                 {ok, Header1};
      #             false ->
      #                 % To get here we must have two different header versions with signatures intact.
      #                 % It's weird but possible (a commit failure right at the 2k boundary). Log it and take the first.
      #                 ?LOG_INFO("Header version differences.~nPrimary Header: ~p~nSecondary Header: ~p", [Header1, Header2]),
      #                 {ok, Header1}
      #             end;
      #         Error ->
      #             % error reading second header. It's ok, but log it.
      #             ?LOG_INFO("Secondary header corruption (error: ~p). Using primary header.", [Error]),
      #             {ok, Header1}
      #         end;
      #     Error ->
      #         % error reading primary header
      #         case extract_header(Prefix, Bin2) of
      #         {ok, Header2} ->
      #             % log corrupt primary header. It's ok since the secondary is still good.
      #             ?LOG_INFO("Primary header corruption (error: ~p). Using secondary header.", [Error]),
      #             {ok, Header2};
      #         _ ->
      #             % error reading secondary header too
      #             % return the error, no need to log anything as the caller will be responsible for dealing with the error.
      #             Error
      #         end
      #     end,
      #     case Result of
      #     {ok, {pointer_to_header_data, Ptr}} ->
      #         pread_term(Fd, Ptr);
      #     _ ->
      #         Result
      #     end.
      def read_header(fd, prefix)
        bin = pread(fd, 0, 2 * HEADER_SIZE)
        bin1, bin2 = bin.slice!(0..HEADER_SIZE), bin.slice!(0..HEADER_SIZE)

        begin
          header1 = extract_header(bin1)

          begin
            header2 = extract_header(bin2)

            if header1 == header2
              # everything is completely normal
              return header1
            else
              # to get here we must have two different header versions with
              # signatures intact.
              # It's weird but possible (a commit failure right at the 2k
              # boundary). Log it and take the first.
              Log.info('Header version differences.')
              Log.info('Primary Header: %p' % header1)
              Log.info('Secondary Header: %p' % header2)

              return header1
            end
          rescue HeaderCorruption => error2
            # error reading second header. It's ok, but log it.
            Log.info('Secondary header corruption. Using primary header')
            Log.info('Error was: %p' % [error2])

            return header1
          end
        rescue HeaderCorruption => error1
          header2 = extract_header(bin2)

          # Log corrupt primary header.
          # It's ok since the secondary is still good.
          Log.info('Primary header corruption. Using secondary header')
          Log.info('Error was: %p' % [error1])

          return header2
        end
      end

      # extract_header(Prefix, Bin) ->
      #     SizeOfPrefix = size(Prefix),
      #     SizeOfTermBin = ?HEADER_SIZE -
      #                     SizeOfPrefix -
      #                     16,     % md5 sig
      #
      #     <<HeaderPrefix:SizeOfPrefix/binary, TermBin:SizeOfTermBin/binary, Sig:16/binary>> = Bin,
      #
      #     % check the header prefix
      #     case HeaderPrefix of
      #     Prefix ->
      #         % check the integrity signature
      #         case erlang:md5(TermBin) == Sig of
      #         true ->
      #             Header = binary_to_term(TermBin),
      #             {ok, Header};
      #         false ->
      #             header_corrupt
      #         end;
      #     _ ->
      #         unknown_header_type
      #     end.
      def extract_header(prefix, bin)
        size_of_prefix = prefix.bytesize
        size_of_term_bin = HEADER_SIZE - size_of_prefix - 16

        header_prefix = bin.slice!(0..size_of_prefix)
        term_bin = bin.slice!(0..size_of_term_bin)
        sig = bin.slice!(0..16)

        if header_prefix == prefix
          # check integrity signature
          if Digest::MD5.hexdigest(term_bin) == sig
            header = Term.new(StringIO.new(term_bin)).next
            return header
          else
            raise HeaderCorruption, "signature doesn't match"
          end
        else
          raise HeaderCorruption, "unknown header type"
        end
      end
    end
  end
end

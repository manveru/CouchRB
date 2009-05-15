require 'spec/helper'
require 'open3'

ET = CouchRB::Db::Term

TERM_TO_BINARY = <<-ERLANG.strip
io:format("~n~n"),
Bin = term_to_binary(%s),
io:format("~p~n~n", [Bin]).
ERLANG

shared :serializer do
  # I built small scripts that help us passing data from and to escript, that's
  # much better than piping into erl.
  #
  # Still struggling with the lack of an easy-to-use eval that would let us
  # convert terms to binary, but that's not as important since what we need to
  # do is parse and generate binary term format.
  # For now we still pipe into erl for that.
  def pipe_binary_to_term(binary)
    Open3.popen3('binary_to_term'){|sin,sout,serr|
      sin.print(%(<<"#{binary}">>.\n))
      sout.read.chomp.scan(/\S/).join
    }
  end

  # this script doesn't work as we would like it to, figure out how to create
  # erlang terms from a string (erl_eval or erl_parse... but how?).
  def pipe_term_to_binary(term)
    Open3.popen3('term_to_binary'){|sin,sout,serr|
      sin.print(%(<<"#{binary}">>.\n))
      sout.read.chomp
    }
  end

  # The shelling out to erlang is very expensive, it would be better to talk
  # with it over network.
  def term_to_binary(term)
    input = TERM_TO_BINARY % [term]
    # puts input
    output = `echo -e '#{input}' | erl`
    # puts output
    out = []
    output[/\n\n(.*)\n\n/m, 1].scan(/\d+/){|d| out << [d.to_i].pack('C') }
    out
  end

=begin
  def parse(prefix, suffix)
    id = [prefix.to_s(16)].pack('H*')
    binary = "#{id}#{suffix}"
    ET.new(StringIO.new(binary)).next
  end

  def dump(obj)
    binary = ET.dump(obj)
    id = binary[0,1].unpack('H*')[0].to_i(16)
    rest = binary[1..-1]
    [id, rest]
  end

  def roundtrip(obj)
    parse(*dump(obj))
  end
=end
end

describe CouchRB::Db::Term do
  describe 'deserializing' do
    behaves_like :serializer

    def check(term, expected)
      binary = term_to_binary(term).join
      parsed = ET.new(StringIO.new(binary)).next
      parsed.should == expected
    end

    # TODO
    # CACHED_ATOM_EXT
    # NEW_FLOAT_EXT
    # BIT_BINARY_EXT
    # NEW_CACHE_EXT

    should 'parse SMALL_INTEGER_EXT' do
      check 123, 123
    end

    should 'parse INTEGER_EXT' do
      values = [1 << 8, 1 << 15, 1 << 30]
      values.each{|value| check(value, value) }
    end

    should 'parse FLOAT_EXT' do
      check 1.23456, 1.23456
    end

    should 'parse ATOM_EXT' do
      check 'kv_node', :kv_node
    end

    # TODO
    # REFERENCE_EXT
    # PORT_EXT
    # PID_EXT

    should 'parse SMALL_TUPLE_EXT' do
      check '{a,b,c}', ET::Tuple[:a, :b, :c]
    end

    should 'parse LARGE_TUPLE_EXT' do
      values = [0,1,2,3,4,5,6,7,8,9] * 30
      term = '{' << values.join(',') << '}'
      check term, ET::Tuple[*values]
    end

    should 'parse NIL_EXT' do
      check '[]', nil
    end

    # TODO
    # STRING_EXT

    should 'parse LIST_EXT' do
      check '[a,b,c]', ET::List[:a, :b, :c]
    end

    # TODO
    # BINARY_EXT

    should 'parse SMALL_BIG_EXT' do
      values = [1 << 32, 1 << 2039]
      values.each{|value| check(value, value) }
    end

    should 'parse LARGE_BIG_EXT' do
      values = [1 << 2040, 1 << 8159]
      values.each{|value| check(value, value) }
    end

    # TODO
    # NEW_FUN_EXT
    # EXPORT_EXT
    # NEW_REFERENCE_EXT
    # FUN_EXT
    # TERM
  end

  describe 'serializing' do
    behaves_like :serializer

    def check(value, expected)
      pipe_binary_to_term(ET.dump_term(value)).
        should == expected
    end

    # TODO
    # CACHED_ATOM_EXT
    # NEW_FLOAT_EXT
    # BIT_BINARY_EXT
    # NEW_CACHE_EXT

    should 'dump as SMALL_INTEGER_EXT' do
      check 123, '123'
    end

    should 'dump as INTEGER_EXT' do
      values = [1 << 8, 1 << 15, 1 << 30]
      values.each{|value| check(value, value.to_s) }
    end

    should 'dump as FLOAT_EXT' do
      check 1.23456, '1.23456'
    end

    should 'dump as ATOM_EXT' do
      check :kv_node, 'kv_node'
    end

    # TODO
    # REFERENCE_EXT
    # PORT_EXT
    # PID_EXT

    should 'dump as SMALL_TUPLE_EXT' do
      check ET::Tuple[1,2,3], '{1,2,3}'
    end

    should 'dump as LARGE_TUPLE_EXT' do
      values = [0,1,2,3,4,5,6,7,8,9] * 30
      expected = '{' << values.join(',') << '}'
      check ET::Tuple[*values], expected
    end

    should 'dump as NIL_EXT' do
      check nil, '[]'
    end

    # TODO
    # STRING_EXT

    should 'dump as LIST_EXT' do
      check ET::List[1,2,3], '[1,2,3]'
    end

    # TODO
    # BINARY_EXT

    should 'dump as SMALL_BIG_EXT' do
      values = [1 << 32, 1 << 2039]
      values.each{|value| check(value, value.to_s) }
    end

    should 'dump as LARGE_BIG_EXT' do
      values = [1 << 2040, 1 << 8159]
      values.each{|value| check(value, value.to_s) }
    end

    # TODO
    # NEW_FUN_EXT
    # EXPORT_EXT
    # NEW_REFERENCE_EXT
    # FUN_EXT
    # TERM
  end
end

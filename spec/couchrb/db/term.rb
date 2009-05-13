require 'spec/helper'

shared :serializer do
  # The shelling out to erlang is very expensive, it would be better to talk
  # with it over network.

  TERM_TO_BINARY = <<-ERLANG.strip
io:format("~n~n"),
Bin = term_to_binary(%s),
io:format("~p~n~n", [Bin]).
  ERLANG

  BINARY_TO_TERM = <<-ERLANG.strip
io:format("~n~n"),
Bin = binary_to_term(%s),
io:format("~p~n~n", [Bin]).
  ERLANG

  def term_to_binary(term)
    input = TERM_TO_BINARY % [term]
    # puts input
    output = `echo -e '#{input}' | erl`
    puts output
    out = []
    output[/\n\n(.*)\n\n/m, 1].scan(/\d+/){|d| out << [d.to_i].pack('C') }
    out
  end

  def binary_to_term(id, bytes)
    binary = '<<' << [131, id, *bytes].join(',') << '>>'
    input = BINARY_TO_TERM % [binary]
    # puts input
    output = `echo -e '#{input}' | erl`
    # puts output
    output[/\n\n(.*)\n\n/, 1].gsub(/\s+/, '')
  end

  def binary_to_erl_binary(binary)
    '<<' << binary.scan(/./).map{|c| [c].pack('C') }.join(',') << '>>'
  end

  def parse(prefix, suffix)
    id = [prefix.to_s(16)].pack('H*')
    binary = "#{id}#{suffix}"
    CouchRB::Db::Term.new(StringIO.new(binary)).next
  end

  def dump(obj)
    binary = CouchRB::Db::Term.dump(obj)
    id = binary[0,1].unpack('H*')[0].to_i(16)
    rest = binary[1..-1]
    [id, rest]
  end

  def roundtrip(obj)
    parse(*dump(obj))
  end
end

ET = CouchRB::Db::Term

describe CouchRB::Db::Term do
  # Deserialize some objects that have equivalents in Ruby
  describe 'deserializing' do
    behaves_like :serializer

    should 'handle cached atom' do
      value = 12
      atom = parse(67, [value].pack('C'))
      atom.should == value
    end

    should 'handle Float (new)' do
      value = 123123.123123
      float = parse(70, [value].pack('G'))
      float.should == value
    end

    # should 'handle Bit Binary' do
    #   parse(77, 'something').should.
    #     flunk("Haven't got a real-world example yet")
    # end

    # should 'handle new cache' do
    #   parse(78, "something").should.
    #     flunk("Used only Erlang internally")
    # end

    should 'handle Integer (small)' do
      value = 255
      int = parse(97, [value].pack('C'))
      int.should == value
    end

    should 'handle Integer (large)' do
      value = 1078199889
      int = parse(98, [value].pack('l'))
      int.should == value
    end

    should 'handle Atom' do
      value = 'db_header'
      length = [value.bytesize].pack('n')
      atom = parse(100, "#{length}#{value}")
      atom.should == value.to_sym
    end

    # should 'handle Reference' do
    #   parse(101, 'something').should.
    #     flunk("Haven't got a real-world example yet")
    # end

    # should 'handle Port' do
    #   parse(102, "something").should.
    #     flunk("Used only Erlang internally")
    # end

    # should 'handle PID' do
    #   parse(103, "something").should.
    #     flunk("Used only Erlang internally")
    # end
  end

  # Serializing some simple objects that have equivalents in
  # Erlang
  describe 'serializing' do
    behaves_like :serializer

    should 'handle Float' do
      value = 123123.123123
      binary = dump(value)
      binary.should == [70, [value].pack('G')]
    end

    should 'handle Integer (small)' do
      value = 255
      binary = dump(value)
      binary.should == [97, [value].pack('C')]
    end

    should 'handle Integer (large)' do
      value = 1078199889
      binary = dump(value)
      binary.should == [98, [value].pack('l')]
    end
  end

  # This seems most important
  describe 'roundtrip' do
    behaves_like :serializer

    should 'handle Float' do
      value = 123123.123123
      roundtrip(value).should == value
    end

    should 'handle positive Integer (small)' do
      value = 255
      roundtrip(value).should == value
    end

    should 'handle positive Integer (large)' do
      value = ((1 << 31) - 1)
      roundtrip(value).should == value
    end

    should 'handle negative Integer (large)' do
      value = -((1 << 31) - 1)
      roundtrip(value).should == value
    end

    should 'handle positive Big (small)' do
      value = ((1 << 2040) - 1)
      roundtrip(value).should == value
    end

    should 'handle negative Big (small)' do
      value = -((1 << 2040) - 1)
      roundtrip(value).should == value
    end

    should 'handle positive Big (large)' do
      value = ((1 << 8160) - 1)
      roundtrip(value).should == value
    end

    should 'handle negative Big (large)' do
      value = -((1 << 8160) - 1)
      roundtrip(value).should == value
    end

    should 'handle Atom' do
      value = :db_header
      roundtrip(value).should == value
    end

    should 'handle Tuple (small)' do
      value = ET::Tuple.new([100, 200, 300])
      roundtrip(value).should == value
    end

    should 'handle Tuple (small)' do
      value = ET::Tuple.new([100, 200, 300] * 200)
      roundtrip(value).should == value
    end

    should 'handle List' do
      value = [100, 200, 300]
      roundtrip(value).should == value
      roundtrip(roundtrip(value)).should == value
    end

    should 'handle List with nil values' do
      value = [[100, nil], [200, nil], [300, nil]]
      roundtrip(value).should == value
      roundtrip(roundtrip(value)).should == value
    end
  end

  describe 'we can parse original erlang terms' do
    behaves_like :serializer

    def et_bounce(term)
      bin = term_to_binary(term).join
      p bin
      ET.new(StringIO.new(bin)).next
    end

    should 'handle Integer (small)' do
      value = 255
      et_bounce(value).should == value
    end

    should 'handle Big (small)' do
      value = 1078199889
      et_bounce(value).should == value
      et_bounce(-value).should == -value
    end
  end
end

__END__
  describe 'roundtrip over erlang' do
    behaves_like :serializer

    should 'serialize a term to binary' do
      bin = term_to_binary('1.23456')
      bin[2..-1].join.should =~ /^1\.23456/
    end

    should 'deserialize binary to a term' do
      binary = ('%.20f' % 1.23456).unpack('C*')
      binary << 0 until binary.size == 31
      binary_to_term(99, binary).should == '1.23456'
    end

    should 'handle Integer (small)' do
      value = 255
      binary = dump(value)
      binary.should == [97, [value].pack('C')]
      term_to_binary('255').should == binary
    end
  end
end

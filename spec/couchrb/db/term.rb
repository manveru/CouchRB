require 'spec/helper'

shared :serializer do
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


describe CouchRB::Db::Term do
  ET = CouchRB::Db::Term

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
      atom.value.should == value
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
      value = ET::Atom.new('db_header')
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

    # I'm not sure whether this is kosher, but seems so...
    # There must be some lisp buried underneath all that erlang
    should 'handle List' do
      value = ET::List.new([100, 200, 300, ET::List.new([nil])])
      roundtrip(value).should == value
      roundtrip(roundtrip(value)).should == value
    end
  end
end

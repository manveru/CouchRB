require 'spec/helper'

describe CouchRB::Db::Header do
  fixture = File.expand_path('../../../fixture', __FILE__)
  empty   = File.join(fixture, 'empty.couch')
  tiny    = File.join(fixture, 'tiny.couch')
  medium  = File.join(fixture, 'medium.couch')

  describe 'parsing headers' do
    it 'parses headers of an empty db' do
      file = CouchRB::Db::File.new(empty)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "2309e1153e6794f26f9a6504f4a2d1f9"
    end

    it 'parses headers of a tiny db' do
      file = CouchRB::Db::File.new(tiny)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "8d7e70dd9959f3fe8fbbc33ddb021f40"
    end

    it 'parses headers of a medium db' do
      file = CouchRB::Db::File.new(medium)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "c8fd19c870a8dc24ce5ca1d26342dbd0"
    end
  end
end

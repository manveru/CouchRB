require 'spec/helper'

describe CouchRB::CouchFile do
  fixture = File.expand_path('../../fixture', __FILE__)
  empty = File.join(fixture, 'empty.couch')
  tiny  = File.join(fixture, 'tiny.couch')
  medium = File.join(fixture, 'medium.couch')

  it 'should parse headers of a file' do
    db = CouchRB::CouchFile.new(empty)
    db.read_header

#     db.header1.should.be.ok
#     db.header1.md5.should == "2309e1153e6794f26f9a6504f4a2d1f9"
#
#     db.header2.should.be.ok
#     db.header2.md5.should == db.header1.md5
  end

#   it 'should parse more!' do
#     db = CouchRB::CouchFile.new(tiny)
#     puts
#     db.parse do |exp|
#       p exp
#     end
#   end

#   it 'should parse even more!' do
#     db = CouchRB::CouchFile.new(medium)
#     puts
#     db.parse do |term|
#       p term
#     end
#   end
end

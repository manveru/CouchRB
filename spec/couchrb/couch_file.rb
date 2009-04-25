require 'spec/helper'

# Pretty sad, all we can do right now is read the headers and check whether
# they are valid.

describe CouchRB::CouchFile do
  path = File.expand_path('../../fixture/test.couch', __FILE__)

  it 'should parse headers of a file' do
    db = CouchRB::CouchFile.new(path)
    db.parse

    db.header1.should.be.ok
    db.header1.md5.should == "2309e1153e6794f26f9a6504f4a2d1f9"

    db.header2.should.be.ok
    db.header2.md5.should == db.header1.md5
  end
end

require 'spec/helper'

describe '.couch docs' do
  fixture = File.expand_path('../../../fixture', __FILE__)

  empty   = File.join(fixture, 'empty.couch')
  tiny    = File.join(fixture, 'tiny.couch')
  medium  = File.join(fixture, 'medium.couch')

  it 'parses docs of a tiny db' do
    file = CouchRB::Db::File.new(medium)
    file.parse
    # pp file.docs
    # file.docs.size.should == 6

#     pp file.docs
  end
end

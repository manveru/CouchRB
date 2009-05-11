require 'spec/helper'

describe 'recreate doc' do
  behaves_like :couch_client

  @db = DB.new('/test_suite_db')
  @db.delete_db
  @db.create_db

  it 'creates a new document and deletes it' do
    doc = {'_id' => 'foo', 'a' => 'bar', 'b' => 42}

    @db.save(doc)['ok'].should == true
    @db.open(doc['_id'])['a'].should == 'bar'

    @db.delete_doc(doc)['ok'].should == true
    @db.open('foo').should == nil
  end

  it 'creates an identical document and deletes it again' do
    doc = {'_id' => 'foo', 'a' => 'bar', 'b' => 42}

    @db.save(doc)['ok'].should == true
    @db.open(doc['_id'])['a'].should == 'bar'

    @db.delete_doc(doc)['ok'].should == true
    @db.open('foo').should == nil
  end

=begin
  it 'creates a new document with same ID, saves and modifies it' do
    10.times do
      doc = {'_id' => 'foo'}
      @db.save(doc)['ok'].should == true
      doc = @db.open('foo')
      doc['a'] = 'baz'
      @db.save(doc)['ok'].should == true
      @db.delete_doc(doc)['rev'].should == nil
    end
  end
=end
end

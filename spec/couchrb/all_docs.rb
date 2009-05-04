require 'spec/helper'

describe 'all_docs' do
  behaves_like :couch_client

  @db = DB.new('test_suite_db')
  @db.delete_db
  @db.create_db

  should 'create some more documents' do
    @db.save '_id' => '0', :a => 1, :b => 1
    @db.check_ok
    @db.save '_id' => '3', :a => 4, :b => 16
    @db.check_ok
    @db.save '_id' => '1', :a => 2, :b => 4
    @db.check_ok
    @db.save '_id' => '2', :a => 3, :b => 9
    @db.check_ok
  end

  should 'check all docs' do
    results = @db.all_docs
    @db.check_ok
    rows = results['rows']

    results['total_rows'].should == rows.size
    rows.map{|row| row['id'] }.sort.should == %w[0 1 2 3]
  end

  should 'check _all_docs with descending=true' do

    results = @db.all_docs(:descending => true)
    @db.check_ok
    results['total_rows'].should == results['rows'].size
  end

  should 'check _all_docs offset' do
    results = @db.all_docs(:startkey => '2')
    results['offset'].should == 2
  end

  should 'collate sanely' do
    @db.save("_id" => "Z", "foo" => "Z")
    @db.save("_id" => "a", "foo" => "a")

    rows = @db.all_docs(:startkey => 'Z', :endkey => 'Z')
    rows['rows'].size.should == 1
  end

  # TODO: All the sequence tests don't work currently until i have an idea
  #       how this is handled in CouchDB.
end

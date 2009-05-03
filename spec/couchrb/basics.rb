require 'spec/helper'

describe 'basics' do
  behaves_like :mock, :couch_client

  it 'shows version on /' do
    get('/').status.should == 200
    JSON.parse(last_response.body).
      should == { 'couchdb' => 'Welcome', 'version' => '0.9.0'}
  end

  it 'creates and deletes a db' do
    @db = DB.new('/test_suite_db')
    @db.create_db.status.should == 200
    @db.json_body['ok'].should == true

    @db.delete_db.status.should == 200
    @db.json_body['ok'].should == true
  end

  should "tell us if we try to delete a db that doesn't exist" do
    @db.delete_db.status.should == 404
    @db.json_body.should == {'error' => 'not_found', 'reason' => 'Missing'}
  end

  it 'returns 412 when trying to PUT existing DB' do
    @db.create_db.status.should == 200
    @db.json_body['ok'].should == true

    @db.create_db.status.should == 412
    @db.json_body.should == {
      'error' => 'file_exists',
      'reason' => 'The database could not be created, the file already exists.'
    }
  end

  should 'get the database info, check the db_name' do
    @db.info['db_name'].should == 'test_suite_db'
  end

  should 'create a document and save it to the database' do
    @doc_1 = {'_id' => '0', 'a' => 1, 'b' => 1}

    @db.save(@doc_1).status.should == 200

    doc = @db.json_body
    doc['ok'].should == true
    doc['id'].should.not.be.nil
    doc['rev'].should.not.be.nil

    @doc_1['_id'].should == doc['id']
    @doc_1['_rev'].should == doc['rev']
  end

  should 'make sure the revs_info status is good' do
    @db.open(@doc_1['_id'], :revs_info => true)
    @db.json_body['_revs_info'].first['status'].should == 'available'
  end

  should 'create some more documents' do
    @db.save('_id' => '1', 'a' => 2, 'b' => 4)
    @db.json_body['ok'].should == true

    @db.save('_id' => '2', 'a' => 3, 'b' => 9)
    @db.json_body['ok'].should == true

    @db.save('_id' => '3', 'a' => 4, 'b' => 16)
    @db.json_body['ok'].should == true
  end

  should 'have correct count of docs' do
    @db.info['doc_count'].should == 4
  end

  should 'process a simple map function' do
    @map = 'lambda{|doc| emit(nil, doc["b"]) if doc["a"] == 4 }'

    results = @db.query(:map => @map)
    results['total_rows'].should == 1
    results['rows'].first['value'].should == 16
  end

  should 'reopen, modify, and save a document' do
    doc = @db.open(@doc_1['_id'])

    doc['a'].should == 1
    doc['a'] = 4

    @db.save(doc)
  end

  should 'still have correct count of docs' do
    @db.info['doc_count'].should == 4
  end

  should 'show the modified doc in the new map results' do
    @db.query(:map => @map)['total_rows'].should == 2
  end

  should 'write 2 more documents' do
    @db.save('_id' => '4', 'a' => 3, 'b' => 9)
    @db.save('_id' => '5', 'a' => 4, 'b' => 16)

    # 1 more document should now be in the result.
    @db.query(:map => @map)['total_rows'].should == 3

    @db.info['doc_count'].should == 6
  end

  it 'processes a map/reduce query' do
    @reduce = 'lambda{|keys, values| sum(values) }'

    results = @db.query(:map => @map, :reduce => @reduce)
    results['rows'].first['value'].should == 33
  end

  it 'deletes a document' do
    @db.delete_doc(@doc_1)['ok'].should == true
  end

  it 'makes sure we cannot open the document' do
    @db.open(@doc_1['_id']).should == nil
  end

  it 'shows 1 less document in the results' do
    @db.query(:map => @map)['total_rows'].should == 2
  end

  it 'counts one less document in the db' do
    @db.info['doc_count'].should == 5
  end

  should 'still be able open the old rev of the deleted doc' do
    @db.open(@doc_1['_id'], :rev => @doc_1['_rev'])
    @db.last_response.status.should == 200
  end

  it 'should set a Location header on POST' do
    @db.save('foo' => 'bar')
    doc = @db.json_body

    location = @db.last_response['Location']
    location.should.not.be.nil?

    atoms = location.split('/')
    atoms[1].should == 'test_suite_db'
    atoms[2].should == doc['id']
  end

  should 'return 404 when deleting a non-existent document' do
    @db.delete_doc('_id' => 'doc-does-not-exist')
    @db.last_response.status.should == 404
  end
end

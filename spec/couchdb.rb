require 'spec/helper'

describe 'basics' do
  behaves_like :couch_client

  it 'shows version on /' do
    get('/').status.should == 200
    json_body['couchdb'].should == 'Welcome'
  end

  it 'creates and deletes a db' do
    put('/test_suite_db').status.should == 200
    json_body['ok'].should == true

    delete('/test_suite_db').status.should == 200
    json_body['ok'].should == true
  end

  should "tell us if we try to delete a db that doesn't exist" do
    delete('/test_couch_db').status.should == 404
    json_body.should == {'error' => 'not_found', 'reason' => 'Missing'}
  end

  it 'returns 412 when trying to PUT existing DB' do
    put('/test_suite_db').status.should == 200
    json_body['ok'].should == true

    put('/test_suite_db').status.should == 412
    json_body.should == {
      'error' => 'file_exists',
      'reason' => 'The database could not be created, the file already exists.'
    }
  end

  should 'get the database info, check the db_name' do
    get('/test_suite_db')
    json_body['db_name'].should == 'test_suite_db'
  end

  should 'create a document and save it to the database' do
    put_doc('/test_suite_db/0', 'a' => 1, 'b' => 1)
    last_response.status.should == 200

    body = json_body
    body['ok'].should == true
    body['id'].should == '0'
    body['rev'].should.not.be.nil
  end

  should 'make sure the revs_info status is good' do
    get('/test_suite_db/1', :revs_info => true).status.should == 200
    json_body['_revs_info'].first['status'].should == 'available'
  end

  should 'create some more documents' do
    put_doc('/test_suite_db/1', '_id' => '1', 'a' => 2, 'b' => 4)
    json_body['ok'].should == true
    put_doc('/test_suite_db/2', '_id' => '2', 'a' => 3, 'b' => 9)
    json_body['ok'].should == true
    put_doc('/test_suite_db/3', '_id' => '3', 'a' => 4, 'b' => 16)
    json_body['ok'].should == true
  end

  should 'have correct count of docs' do
    get('/test_suite_db')
    json_body['doc_count'].should == 4
  end

  should 'process a simple map function' do
    @map = 'lambda{|doc| emit(nil, doc["b"]) if doc["a"] == 4 }'

    query('/test_suite_db/_temp_view', :map => @map)
    body = json_body
    body['total_rows'].should == 1
    body['rows'].first['value'].should == 16
  end

  should 'reopen document we saved earlier' do
    get('/test_suite_db/0').status.should == 200
    json_body['a'].should == 1
  end

  should 'modify and save a document' do
    get('/test_suite_db/0').status.should == 200
    body = json_body
    body['a'] = 4
    put_doc('/test_suite_db/0', body).status.should == 200
    json_body['ok'].should == true
    json_body['rev'].should.not == body['_rev']

    get('/test_suite_db/0')
    @first_doc = json_body # keep it around for later reference
    @first_doc['a'].should == 4
  end

  should 'still have correct count of docs' do
    get('/test_suite_db')
    json_body['doc_count'].should == 4
  end

  should 'show the modified doc in the new map results' do
    query('/test_suite_db/_temp_view', :map => @map)
    body = json_body
    body['total_rows'].should == 2
  end

  should 'write 2 more documents' do
    put_doc('/test_suite_db/4', 'a' => 3, 'b' => 9)
    put_doc('/test_suite_db/5', 'a' => 4, 'b' => 16)

    # 1 more document should now be in the result.
    query('/test_suite_db/_temp_view', :map => @map)
    body = json_body
    body['total_rows'].should == 3

    get('/test_suite_db')
    json_body['doc_count'].should == 6
  end

  it 'processes a map/reduce query' do
    @reduce = 'lambda{|keys, values| sum(values) }'

    query('/test_suite_db/_temp_view', :map => @map, :reduce => @reduce)
    body = json_body
    body['rows'].first['value'].should == 33
  end

  it 'deletes a document' do
    @id = @first_doc['_id']
    @rev = @first_doc['_rev']
    delete("/test_suite_db/#@id", :rev => @rev)
    json_body['ok'].should == true
  end

  it 'makes sure we cannot open the document' do
    get("/test_suite_db/#@id")
    json_body['_id'].should == nil
  end

  it 'shows 1 less document in the results' do
    query('/test_suite_db/_temp_view', :map => @map)
    json_body['total_rows'].should == 2
  end

  it 'counts one less document in the db' do
    get('/test_suite_db')
    json_body['doc_count'].should == 5
  end
end

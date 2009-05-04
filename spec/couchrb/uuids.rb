require 'spec/helper'

describe '/_uuids' do
  behaves_like :couch_client

  def ensure_hash_busting_headers
    res = @db.last_response

    res['Cache-Control'].should =~ /no-cache/
    res['Pragma'].should =~ /no-cache/

    current_time   = Time.now
    expires_header = Time.parse(res['Expires'])
    date_header    = Time.parse(res['Date'])

    expires_header.should < current_time
    (current_time - date_header).should < 30000
  end

  @db = DB.new('/test_suite_db')

  it 'returns a single UUID without an explicit count' do
    @db.get('/_uuids')
    body = @db.json_body
    body['uuids'].size.should == 1
    @first = body['uuids'][0]

    ensure_hash_busting_headers
  end

  it 'returns a single UUID with an explicit count' do
    @db.get('/_uuids', :count => 1)
    body = @db.json_body
    body['uuids'].size.should == 1
    @second = body['uuids'][0]

    @first.should.not == @second
  end

  should 'have no collisions with 1,000 UUIDs' do
    @db.get('/_uuids', :count => 1000)
    body = @db.json_body
    body['uuids'].size.should == 1000
    body['uuids'].uniq.size.should == 1000
  end

  should 'return a 405 on POST' do
    @db.post('/_uuids', :count => 1000).status.should == 405
  end
end

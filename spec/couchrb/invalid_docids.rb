require 'spec/helper'

describe 'Valid doc ID' do
  behaves_like :couch_client

  @db = DB.new('/test_suite_db')
  @db.delete_db
  @db.create_db

  it 'is a string' do
    @db.save('_id' => 1)['error'].should == 'bad_request'
    @db.last_response.status.should == 400
  end

  it 'has no _prefix' do
    @db.save('_id' => '_invalid')['error'].should == 'bad_request'
    @db.last_response.status.should == 400
  end

  # TODO: find out what _local is
  # TODO: implement bulk save
end

__END__

// Test _local explicitly first.
T(db.save({"_id": "_local/foo"}).ok);
T(db.open("_local/foo")._id == "_local/foo");

// Test _bulk_docs explicitly.
var docs = [{"_id": "_design/foo"}, {"_id": "_local/bar"}];
db.bulkSave(docs);
docs.forEach(function(d) {T(db.open(d._id)._id == d._id);});

docs = [{"_id": "_invalid"}];
try {
  db.bulkSave(docs);
  T(1 == 0, "doc id may not start with underscore, even in bulk docs");
} catch(e) {
    T(db.last_req.status == 400);
    T(e.error == "bad_request");
}

require 'spec/helper'

describe 'all_docs' do
  behaves_like :couch_client

  @db = DB.new('test_suite_db')
  @db.delete_db
  @db.create_db

  should 'pass the original all_docs tests' do
    # create some more documents.
    @db.save '_id' => '0', :a => 1, :b => 1
    @db.check_ok
    @db.save '_id' => '3', :a => 4, :b => 16
    @db.check_ok
    @db.save '_id' => '1', :a => 2, :b => 4
    @db.check_ok
    @db.save '_id' => '2', :a => 3, :b => 9
    @db.check_ok

    # check all docs
    results = @db.all_docs
    @db.check_ok
    rows = results['rows']

    results['total_rows'].should == rows.size
    rows.map{|row| row['id'] }.sort.should == %w[0 1 2 3]

    # check _all_docs with descending=true

    results = @db.all_docs(:descending => true)
    @db.check_ok
    pp results
    results['total_rows'].should == results['rows'].size

    # check _all_docs offset
    results = @db.all_docs(:startkey => '2')
    results['offset'].should == 2

    # check that the docs show up in the seq view in the order they were created
    results = @db.all_docs_by_seq
    results['rows'].map{|r| r['id'] }.should == %w[0 3 1 2]
  end
end

__END__
  // check that the docs show up in the seq view in the order they were created
  var all_seq = db.allDocsBySeq();
  var ids = ["0","3","1","2"];
  for (var i=0; i < all_seq.rows.length; i++) {
    var row = all_seq.rows[i];
    T(row.id == ids[i]);
  };

  // it should work in reverse as well
  all_seq = db.allDocsBySeq({descending:true});
  ids = ["2","1","3","0"];
  for (var i=0; i < all_seq.rows.length; i++) {
    var row = all_seq.rows[i];
    T(row.id == ids[i]);
  };

  // check that deletions also show up right
  var doc1 = db.open("1");
  var deleted = db.deleteDoc(doc1);
  T(deleted.ok);
  all_seq = db.allDocsBySeq();

  // the deletion should make doc id 1 have the last seq num
  T(all_seq.rows.length == 4);
  T(all_seq.rows[3].id == "1");
  T(all_seq.rows[3].value.deleted);

  // is this a bug?
  // T(all_seq.rows.length == all_seq.total_rows);

  // do an update
  var doc2 = db.open("3");
  doc2.updated = "totally";
  db.save(doc2);
  all_seq = db.allDocsBySeq();

  // the update should make doc id 3 have the last seq num
  T(all_seq.rows.length == 4);
  T(all_seq.rows[3].id == "3");

  // ok now lets see what happens with include docs
  all_seq = db.allDocsBySeq({include_docs: true});
  T(all_seq.rows.length == 4);
  T(all_seq.rows[3].id == "3");
  T(all_seq.rows[3].doc.updated == "totally");

  // and on the deleted one, no doc
  T(all_seq.rows[2].value.deleted);
  T(!all_seq.rows[2].doc);

  // test the all docs collates sanely
  db.save({_id: "Z", foo: "Z"});
  db.save({_id: "a", foo: "a"});

  var rows = db.allDocs({startkey: "Z", endkey: "Z"}).rows;
  T(rows.length == 1);
end

require 'lib/couchdb/rbtree'
require 'bacon'
Bacon.summary_at_exit

describe CouchDB::RedBlackTree do
  it 'inserts and iterates' do
    values = [11,4,8,14,17,6,9,7,16,2,15,13,5,19,18,12,10,3,20,1]
    tree = CouchDB::RedBlackTree[*values]

    all = []
    tree.each{|node| all << node.value }
    all.should == values.sort
  end

  it 'inserts and searches' do
    values = [11,4,8,14,17,6,9,7,16,15,13,5,19,18,12,10,3,20,1]

    tree = CouchDB::RedBlackTree[*values]
    tree.find(16).should == CouchDB::RedBlackTree.new(16)
  end
end

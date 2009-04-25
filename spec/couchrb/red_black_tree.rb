require 'spec/helper'
require 'bacon'
Bacon.summary_at_exit

class Doc < Struct.new(:id, :rev)
  def self.[](*args)
    new(*args)
  end

  include Comparable

  def <=>(other)
    raise(TypeError, "Not a Doc") unless other.is_a?(self.class)

    if rev
      other_rev, other_id = other.rev, other.id

      if id == other_id
        if other_rev == :max
          -1
        elsif rev == :max
          1
        else
          rev <=> other_rev # will raise if both are symbols (on 1.8)
        end
      else
        [id, rev] <=> [other_id, other_rev]
      end
    else
      id <=> other.id
    end
  end
end

describe CouchRB::RedBlackTree do
  it 'inserts and iterates' do
    values = [11,4,8,14,17,6,9,7,16,2,15,13,5,19,18,12,10,3,20,1]
    tree = CouchRB::RedBlackTree[*values]

    all = []
    tree.each{|node| all << node.value }
    all.should == values.sort
  end

  it 'inserts and searches' do
    values = [11,4,8,14,17,6,9,7,16,15,13,5,19,18,12,10,3,20,1]

    tree = CouchRB::RedBlackTree[*values]
    tree.find(16).should == CouchRB::RedBlackTree.new(16)
  end

  it 'inserts multiple revisions and finds the latest' do
    ten_dogs = Array.new(10){|i| Doc['dog', i] }

    tree = CouchRB::RedBlackTree[*ten_dogs]
    puts
    tree.find_latest(Doc['dog', :max]).value.should == Doc['dog', 9]
  end
end

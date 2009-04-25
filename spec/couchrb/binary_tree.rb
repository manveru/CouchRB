require 'spec/helper'
require 'bacon'
Bacon.summary_at_exit

describe CouchRB::BinaryTree do
  it 'initializes' do
    @tree = CouchRB::BinaryTree.new(10)
    @tree.value.should == 10
  end

  it 'can add nodes' do
    @tree << 11
    @tree.right.value.should == 11
    @tree << 9
    @tree.left.value.should == 9
  end

  it 'iterates with #each' do
    full = []
    @tree.each{|node| full << node }
    full.should == [9, 10, 11]
  end

  it 'iterates with #each_preorder' do
    full = []
    @tree.each_preorder{|node| full << node }
    full.should == [10, 9, 11]
  end

  it 'iterates with #each_postorder' do
    full = []
    @tree.each_postorder{|node| full << node }
    full.should == [9, 11, 10]
  end

  it 'generates larger trees' do
    ascending_tree = CouchRB::BinaryTree.new(0)
    (1..100).each{|n| ascending_tree << n }

    descending_tree = CouchRB::BinaryTree.new(100)
    (0..99).each{|n| descending_tree << n }

    ascending, descending = [], []
    ascending_tree.each{|node| ascending << node }
    descending_tree.each{|node| descending << node }

    ascending.should == (0..100).to_a
    ascending.should == descending
  end
end

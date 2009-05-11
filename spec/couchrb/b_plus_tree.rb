module CouchRB
  # Wiki: B+tree
  # Implementation
  #
  # The leaves (the bottom-most index blocks) of the B+tree are often linked to
  # one another in a linked list; this makes range queries simpler and more
  # efficient (though the aforementioned upper bound can be achieved even
  # without this addition). This does not substantially increase space
  # consumption or maintenance of the tree.
  #
  # If a storage system has a block size of B bytes, and the keys to be stored
  # have a size of k, arguably the most efficient B+tree is one where
  # 'b=(B/k)-1'. Although theoretically the one-off is unnecessary, in practice
  # there is often a little extra space taken up by the index blocks (for
  # example, the linked list references in the leaf blocks). Having an index
  # block which is slightly larger than the storage system's actual block
  # represents a significant performance decrease; therefore erring on the side
  # of caution is preferable.
  #
  # If nodes of the B+tree are organized as arrays of elements, then it may
  # take a considerable time to insert or delete an element as half of the
  # array well need to be shifted on average. To overcome this problem elements
  # inside a node can be organized in a binary tree or B+tree instead of an
  # array.
  #
  # B+tree can also be used for data stored in RAM. In this case a reasonable
  # choice for block size would be the size of processor's cache line. However
  # some studies have proved that a block size few times larger than
  # processor's cache can deliver better performance if cache prefetching is
  # used.
  #
  # Space efficiency of B+tree can be provided by using some compression
  # techniques.
  class BTree
    def initialize(io, root)
      @io = io
      @root = root
    end
  end
end

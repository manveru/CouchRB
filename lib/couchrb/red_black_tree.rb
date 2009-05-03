module CouchRB
  RED_BLACK_TREE_MEMBERS = [ :value, :color, :left, :right, :deleted ]

  class RedBlackTree < Struct.new(*RED_BLACK_TREE_MEMBERS)
    include Comparable
    include Enumerable

    def self.[](*values)
      values.flatten!
      root = RedBlackTree.new(values.shift)
      values.each{|value| root.insert(value) }
      root
    end

    def initialize(value, color = :red, deleted = false, left = nil, right = nil)
      self.value = value
      self.color = color
      self.deleted = deleted
      self.left = left
      self.right = right
    end

    def to_s
      "[#{value},#{color}]"
    end

    # Walk into the tree and find an exact match
    def find(key)
      return self if key == value
      return left.find(key) if left and key < value
      return right.find(key) if right
    end

    # Walk into the tree until we cannot find higher revisions, then return
    # The tricky part is to have a revision that is higher than any other, but
    # that's handled by the value, we only care about the id.
    # If the current document is deleted and a child node differs only in the
    # revision, we consider the child as deleted as well, that allows us to
    # delete an entire history of documents by marking only the parent as
    # deleted.
    def find_latest(key)
      if deleted
        if left
          if key < value
            unless left.value.id == value.id
              return left.find_latest(key)
            end
          end
        end
        if right
          unless right.value.id == value.id
            return right.find_latest(key)
          end
        end
      else
        return self if key == value
        return left.find_latest(key) if left and key < value
        return right.find_latest(key) if right
        return self if key.id == value.id
      end
    end

    def find_value(key)
      found = find(key)
      found.value if found
    end

    def each(&block)
      if deleted
        left.each(&block) if left and left.value.id != value.id
        right.each(&block) if right and right.value.id != value.id
      else
        left.each(&block) if left
        yield self
        right.each(&block) if right
      end
    end

    def <=>(other)
      raise(TypeError, "Not an RedBlackTree") unless other.is_a?(self.class)
      value <=> other.value
    end

    def insert(val)
      rb_insert(val)
      self.color = :black
    end

    def inspect
      "(%p:%p:%p)" % [left, value, right]
    end

    def to_dot(io, parent = nil)
      left.to_dot(io, value) if left
      if parent
        io.puts("%p [color=%s];" % [value, color])
        io.puts("%p -> %p;" % [parent, value])
      else
        # io.puts("%p;" % value)
      end
      right.to_dot(io, value) if right
    end

    def to_json(all = [])
      left.to_json(all) if left
      all << value.to_json
      right.to_json(all) if right
      all
    end

    def all_ascending(all = [])
      left.all_ascending(all) if left
      all << value if value.id != '_'
      right.all_ascending(all) if right
      all
    end

    def all_descending(all = [])
      right.all_descending(all) if right
      all << value if value.id != '_'
      left.all_descending(all) if left
      all
    end

    def all_by_seq(all = [])
    end

    def delete(key)
      return destroy if key == value
      return left.delete(key) if left and key < value
      return right.delete(key) if right
    end

    protected

    def destroy
      self.deleted = true
    end

    def copy(node)
      RED_BLACK_TREE_MEMBERS.each do |member|
        self[member] = node[member]
      end
    end

    # OK
    def rb_insert_left(val, sw)
      if left
        left.rb_insert(val, sw)
      else
        self.left = RedBlackTree.new(val)
      end
    end

    # OK
    def rb_insert_right(val, sw)
      if right
        right.rb_insert(val, sw)
      else
        self.right = RedBlackTree.new(val)
      end
    end

    # OK
    #             self
    #            /   \
    #        left     right
    #
    # becomes
    #
    #            right
    #            /   \
    #        self    left
    def rotate_left
      return unless right

      me = RedBlackTree.new(value, color, deleted, left, right)

      x        = me.right
      me.right = x.left
      x.left   = me

      copy x
    end

    # OK
    #
    #             self
    #            /   \
    #        left     right
    #
    # becomes
    #             left
    #             /  \
    #        right    self
    def rotate_right
      return unless left

      me = RedBlackTree.new(value, color, deleted, left, right)

      x       = me.left
      me.left = x.right
      x.right = me

      copy x
    end

    def rb_insert(val, sw = 0)
      # if current node is a 4 node, split it by flipping its colors
      # if both children of this node are red, change this one to red
      # and the children to black
      l = left
      r = right
      if l and l.color == :red and r and r.color == :red
        self.color = :red
        l.color    = :black
        r.color    = :black
      end

      # go left or right depending on key relationship
      if val < value
        # recursively insert
        rb_insert_left(val, 0)

        # on the way back up check if a rotation is needed
        # if search path has two red links with same orientation
        # do a single rotation and flip the color bits
        if color == :red and l = left and l.color == :red and sw == 1
          rotate_right
        end

        # flip the color bits
        if l = left and l.color == :red
          if ll = l.left and ll.color == :red
            rotate_right

            self.color = :black
            r = right
            r.color = :red if r
          end
        end
      else
        rb_insert_right(val, 1)

        # on the way back up check if a rotation is needed
        # if search path has two red links with same orientation
        # do a single rotation and flip the color bits
        if color == :red and r = right and r.color == :red and sw == 0
          rotate_left
        end

        # flip the color bits
        if r = right and r.color == :red
          if rr = r.right and rr.color == :red
            rotate_left

            self.color = :black
            l = left
            l.color = :red if l
          end
        end
      end
    end
  end
end

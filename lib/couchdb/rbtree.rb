module CouchDB
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

    def find(key)
      return self if key == value
      return left.find(key) if left and key < value
      return right.find(key) if right
    end

    def each(&block)
      left.each(&block) if left
      yield self
      right.each(&block) if right
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

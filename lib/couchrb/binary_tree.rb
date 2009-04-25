class BinaryTree < Struct.new(:value, :left, :right)
  include Enumerable

  def add(value)
    if value < self.value
      if left.nil?
        self.left = BinaryTree.new(value)
      else
        left.add(value)
      end
    else
      if right.nil?
        self.right = BinaryTree.new(value)
      else
        right.add(value)
      end
    end
  end
  alias << add

  def each(&block)
    left.each(&block) if left
    yield(value)
    right.each(&block) if right
  end

  def each_preorder(&block)
    yield(value)
    left.each(&block) if left
    right.each(&block) if right
  end

  def each_postorder(&block)
    left.each(&block) if left
    right.each(&block) if right
    yield(value)
  end
end

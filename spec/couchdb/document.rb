require 'spec/helper'

describe CouchDB::Document do
  D = CouchDB::Document

  it 'should be comparable by id' do
    dog = D.new('dog', '0', 'dog' => true)
    cat = D.new('cat', '0', 'cat' => true)

    dog.should > cat
  end

  it 'should be comparable by id/rev' do
    first_dog  = D.new('dog', '0', 'dog' => true)
    second_dog = D.new('dog', '1', 'dog' => true)

    first_dog.should < second_dog
  end

  it 'should compare special revisions correctly' do
    dog = D.new('dog', 0.to_s(36), 'dog' => true)
    dog_max = D::Max.new('dog', '_max', 'dog' => true)
    dog_min = D::Min.new('dog', '_min', 'dog' => true)

    dog.should < dog_max
    dog.should > dog_min
    dog_max.should > dog
    dog_min.should < dog
  end
end

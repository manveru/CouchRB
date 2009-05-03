# * Encoding: UTF-8
$KCODE = 'u' if /regexp/.respond_to?(:kcode)

require 'spec/helper'

describe 'UTF8 handling' do
  behaves_like :couch_client

  @db = DB.new('/test_suite_db')
  @db.delete_db
  @db.create_db

  @texts = [
    "1. Ascii: hello",
    "2. Russian: На берегу пустынных волн",
    "3. Math: ∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i),",
    "4. Geek: STARGΛ̊TE SG-1",
    "5. Braille: ⡌⠁⠧⠑ ⠼⠁⠒  ⡍⠜⠇⠑⠹⠰⠎ ⡣⠕⠌",
  ]

  it 'stores documents with UTF8 in the db' do
    @texts.each_with_index do |text, idx|
      @db.save('_id' => idx.to_s, 'text' => text)['ok'].should == true
    end
  end

  it 'opens the documents, and they have identical content' do
    1.should == 1

    @texts.each_with_index do |text, idx|
      @db.open(idx)['text'].should == text
    end
  end

  it "doesn't blog up on key collation" do
    map = "proc{|doc| pp doc['text']; emit(nil, doc['text']) }"
    rows = @db.query(:map => map)['rows']
    rows.map{|row| row['value'] }.should == @texts
  end
end

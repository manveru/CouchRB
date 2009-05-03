require 'spec/helper'

describe CouchRB::Db::Header do
  fixture = File.expand_path('../../../fixture', __FILE__)
  empty   = File.join(fixture, 'empty.couch')
  tiny    = File.join(fixture, 'tiny.couch')
  medium  = File.join(fixture, 'medium.couch')

  describe 'parsing headers' do
    it 'parses headers of an empty db' do
      file = CouchRB::Db::File.new(empty)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "2309e1153e6794f26f9a6504f4a2d1f9"

      term = file.header.body.term.elements
      term.size.should == 11

      nil_atom  = CouchRB::Db::Term::Atom.new('nil')
      db_header = CouchRB::Db::Term::Atom.new('db_header')

      term.should == [ db_header, 1, 0, nil_atom, nil_atom, nil_atom, nil_atom,
        0, nil_atom, nil_atom, -402456576 ]
    end

    it 'parses headers of a tiny db' do
      file = CouchRB::Db::File.new(tiny)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "8d7e70dd9959f3fe8fbbc33ddb021f40"

      term = file.header.body.term.elements
      term.size.should == 11

      nil_atom  = CouchRB::Db::Term::Atom.new('nil')
      db_header = CouchRB::Db::Term::Atom.new('db_header')

      term[0..2].should == [db_header, 1, 4]
      term[3].elements.should == [ 1913651200, -1641676800 ]
      term[4].elements[0].should == 1228537856
      term[4].elements[1].elements.should == [ 4, 0 ]
      term[5].elements.should == [ -1371996160, 4 ]
      term[6..10].should == [ nil_atom, 0, nil_atom, nil_atom, -402456576 ]
    end

    it 'parses headers of a medium db' do
      file = CouchRB::Db::File.new(medium)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "c8fd19c870a8dc24ce5ca1d26342dbd0"

      term = file.header.body.term.elements
      term.size.should == 11

      nil_atom  = CouchRB::Db::Term::Atom.new('nil')
      db_header = CouchRB::Db::Term::Atom.new('db_header')

      term[0..2].should == [db_header, 1, 6]
      term[3].elements.should == [1159331840, -887357440]
      term[4].elements[0].should == 4128768
      term[4].elements[1].elements.should == [ 6, 0 ]
      term[5].elements.should == [-214106112, 6]
      term[6..10].should == [ nil_atom, 0, nil_atom, nil_atom, -402456576 ]
    end
  end
end

require 'spec/helper'

describe CouchRB::Db::Header do
  fixture = File.expand_path('../../../fixture', __FILE__)

  empty   = File.join(fixture, 'empty.couch')
  tiny    = File.join(fixture, 'tiny.couch')
  medium  = File.join(fixture, 'medium.couch')

  nil_atom  = CouchRB::Db::Term::Atom.new('nil')
  db_header = CouchRB::Db::Term::Atom.new('db_header')

  describe 'parsing headers' do
    it 'parses headers of an empty db' do
      file = CouchRB::Db::File.new(empty)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "2309e1153e6794f26f9a6504f4a2d1f9"

      body = file.header.body
      body.term.elements.size.should == 11

      body.disc_version.should == 1
      body.update_seq.should == 0
      body.summary_stream_state.should == nil_atom
      body.fulldocinfo_by_id_btree_state.should == nil_atom
      body.docinfo_by_seq_btree_state.should == nil_atom
      body.local_docs_btree_state.should == nil_atom
      body.purge_seq.should == 0
    end

    it 'parses headers of a tiny db' do
      file = CouchRB::Db::File.new(tiny)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "8d7e70dd9959f3fe8fbbc33ddb021f40"

      body = file.header.body
      body.term.elements.size.should == 11

      body.disc_version.should == 1
      body.update_seq.should == 4
      body.summary_stream_state.should == [ 1913651200, -1641676800 ]
      body.fulldocinfo_by_id_btree_state.should == [1228537856, [4, 0]]
      body.docinfo_by_seq_btree_state.should == [ -1371996160, 4 ]
      body.local_docs_btree_state.should == nil_atom
      body.purge_seq.should == 0
    end

    it 'parses headers of a medium db' do
      file = CouchRB::Db::File.new(medium)
      file.parse
      file.header.should.be.ok
      file.header.md5.should == "c8fd19c870a8dc24ce5ca1d26342dbd0"

      body = file.header.body
      body.term.elements.size.should == 11

      body.disc_version.should == 1
      body.update_seq.should == 6
      body.summary_stream_state.should == [1159331840, -887357440]
      body.fulldocinfo_by_id_btree_state.should == [4128768, [6, 0]]
      body.docinfo_by_seq_btree_state.should == [-214106112, 6]
      body.local_docs_btree_state.should == nil_atom
      body.purge_seq.should == 0
    end
  end
end

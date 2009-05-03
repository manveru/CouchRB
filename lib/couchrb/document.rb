module CouchRB
  class Document < Struct.new(:id, :rev, :doc, :seq)
    include Comparable

    # - id
    #   * must be a String
    #   * cannot start with '_'
    #
    # - rev
    #   * must be a String
    #   * cannot start with '_'
    #
    # The problem with revisions being a String is that the string comparision
    # will consider '99' as being greater than '100', but '99'.next will return
    # '100'. This wonderful idiocy can only be countered with even greater
    # stupidity, internally we keep the revision as an integer, and convert
    # in/out via base36, which is the most compact fast lossless conversion we
    # have available, this way we can compare internally correct and fast using
    # integers and present everybody outside with a String.
    def initialize(id, rev, doc = {})
      self.id, self.rev, self.doc =
        id.to_str, rev.to_i(36), doc.dup

      self.doc.delete '_rev'
      self.doc.delete '_id'
    end

    def rev!(sequence)
      self.seq = sequence

      if rev
        self.rev += 1
      else
        self.rev = 0
      end
    end

    def [](key)
      doc[key.to_s]
    end

    # nasty business goes here
    def <=>(other)
      case other
      when Document::Max # ignore revision
        comp = id <=> other.id
        comp == 0 ? -1 : comp
      when Document::Min # ignore revision
        comp = id <=> other.id
        comp == 0 ? 1 : comp
      when Document
        if rev = self.rev
          [id, rev] <=> [other.id, other.rev]
        else
          id <=> other.id
        end
      else
        raise(TypeError, "Not a Document")
      end
    end

    def to_json
      doc.merge(version_hash).to_json
    end

    def version_hash
      {'id' => id, 'rev' => rev.to_s(36)}
    end

    def _version_hash
      {'_id' => id, '_rev' => rev.to_s(36)}
    end

    def to_hash
      doc.dup
    end

    def self.refs_info
      { "_id" => "0",
        "_rev" => "2142263084",
        "_revs_info" => [
          { "rev" => "2142263084", "status" => "available" },
        ]
      }
    end

    def ==(other)
      self.id == other.id
    end

    # This document is smaller than any other of the same revision
    class Min < self
      def <=>(other)
        comp = id <=> other.id
        comp == 0 ? -1 : comp
      end
    end

    # This document is larger than any other of the same revision
    class Max < self
      def <=>(other)
        comp = id <=> other.id
        comp == 0 ? 1 : comp
      end
    end
  end
end

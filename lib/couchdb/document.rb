module CouchDB
  class Document
    include Comparable

    attr_accessor :rev
    attr_reader :doc, :id

    # The revision is handled special, CouchDB specifies that a revision cannot
    # start with '_', so that's going to be our root Document that can never be
    # queried out.
    # If the rev is a symbol, it will have special meaning in comparision, more
    # specifically, a :max rev for a Document with the same id will always be
    # higher than any other Document.
    # This enables us to find the latest revision of Documents in the
    # RedBlackTree by simple comparision.
    # A Document must have a String or nil rev to be inserted in the
    # RedBlackTree, if the rev is nil we generate a random value for it.
    def initialize(id, rev = '_', doc = {})
      @id, @rev, @doc = id, rev, doc
    end

    def rev!
      if @rev
        @rev.next!
      else
        @rev = SecureRandom.hex
      end
    end

    def [](key)
      doc[key.to_s]
    end

    def <=>(other)
      raise(TypeError, "Not a Document") unless other.is_a?(self.class)

      if rev
        other_rev, other_id = other.rev, other.id

        if id == other_id
          if other_rev == :max
            -1
          elsif rev == :max
            1
          else
            rev <=> other_rev # will raise if both are symbols (on 1.8)
          end
        else
          [id, rev] <=> [other_id, other_rev]
        end
      else
        id <=> other.id
      end
    end

    def to_json
      @doc.merge('_id' => id, '_rev' => rev).to_json
    end

    def to_hash
      @doc.dup
    end

    def self.refs_info
      { "_id" => "0",
        "_rev" => "2142263084",
        "_revs_info" => [
          { "rev" => "2142263084", "status" => "available" },
        ]
      }
    end
  end
end

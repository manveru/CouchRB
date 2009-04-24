module CouchDB
  class Document
    include Comparable

    attr_reader :doc, :id, :rev

    def initialize(id, rev = nil, doc = {})
      @id, @rev, @doc = id, rev, doc
    end

    def [](key)
      doc[key.to_s]
    end

    def []=(key, value)
      doc[key.to_s] = value
    end

    def <=>(other)
      raise(TypeError, "Not a Document") unless other.is_a?(self.class)

      if rev
        [id, rev] <=> [other.id, other.rev]
      else
        id <=> other.id
      end
    end

    def to_json
      {'_id' => id}.merge(@doc).to_json
    end

    def update(hash)
      @doc = hash
      self
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

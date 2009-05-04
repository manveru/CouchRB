module CouchRB
  class ViewContext
    attr_reader :results

    def initialize(db, functions = {})
      @db = db
      @map = functions[:map]
      @reduce = functions[:reduce]
      @results = []
    end

    def run_map
      @db.docs.each{|doc| yield(doc.value.dup) }
    end

    def run_reduce
      keys, values = [], []

      @results.each do |result|
        keys << result['key']
        values << result['value']
      end

      @results = []
      results << {'value' => yield(keys, values)}
    end

    def run
      if @map
        run_map(&instance_eval(@map))
        if @reduce
          run_reduce(&instance_eval(@reduce))
        end
      end

      self
    end

    def emit(key, value)
      results << {'key' => key, 'value' => value}
    end

    def sum(*args)
      args.flatten.inject{|s,v| s + v }
    end

    def rows
      results
    end

    def size
      results.size
    end
  end
end

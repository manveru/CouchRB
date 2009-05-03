module CouchRB
  class Database
    include Innate::Trinity
    extend Innate::Trinity

    ALL = {}

    def self.[](name)
      ALL[name]
    end

    def self.[]=(name, db)
      ALL[name] = db
    end

    def self.delete(name)
      ALL.delete(name)
    end

    def self.create(name)
      self[name] = new(name)
      {"ok" => true}
    end

    attr_reader :name, :docs

    def initialize(name)
      @name = name
      @docs = RedBlackTree.new(Document.new('_', '0'))
      @doc_count = 0
      @update_sequence = 0
      Innate.map(url, self)
    end

    def call(env)
      path = (env['PATH_INFO'] || '').sub(/^\//, '')
      method = env['REQUEST_METHOD']
      hash = {}

      if req_body = request.body.read
        begin
          hash = JSON.parse(req_body)
        rescue JSON::ParserError
        end
      end

      if path.empty?
        body = process_empty(method, hash)
      else
        body = process_path(method, path, hash)
      end

      response.body = body.to_json if body
      response.finish
    end

    def process_empty(method, hash = {})
      case method
      when 'GET'
        info
      when 'DELETE'
        delete
      when 'PUT'
        if hash.empty?
          response.status = 412
          { 'error'  => 'file_exists',
            'reason' => 'The database could not be created, the file already exists.'
          }
        else
          process_path(method, CouchRB.uuid)
        end
      else
        response.status = 405
        nil
      end
    end

    def process_path(method, path, hash = {})
      case method
      when 'GET'
        case path
        when '_all_docs'
          if request[:descending]
            rows = @docs.all_descending
          else
            rows = @docs.all_ascending
          end

          {:total_rows => rows.size, :rows => rows, :offset => 2}
        when '_all_docs_by_seq'
          rows = @docs.all_by_seq
          {:total_rows => rows.size, :rows => rows, :offset => 0}
        else
          if request['revs_info']
            Document.refs_info
          elsif rev = request[:rev]
            find_doc(path, rev)
          else
            latest_doc(path)
          end
        end
      when 'POST'
        case path
        when '_temp_view'
          temp_view(hash)
        else
          response.status = 405
          nil
        end
      when 'PUT'
        if rev = hash['_rev']
          update_doc(path, rev, hash)
        else
          put_doc(path, hash)
        end
      when 'DELETE'
        if rev = request['rev']
          delete_doc(path, rev)
        else
          delete_latest_doc(path)
        end
      else
        response.status = 405
      end
    end

    def temp_view(query)
      case query['language']
      when 'ruby'
        context = ViewContext.new(
          self, :map => query['map'], :reduce => query['reduce'])
        result = context.run

        { 'total_rows' => result.size,
          'rows'       => result.rows, }
      else
        raise "Not an option yet"
      end
    end

    def find_doc(id, rev)
      query = Document.new(id, rev)

      if found = @docs.find_value(query)
        found
      else
        response.status = 404
        {"error" => "not_found", "reason" => "missing"}
      end
    end

    def latest_doc(id)
      query = Document::Max.new(id, '0')

      if found = @docs.find_latest(query)
        doc = found.value
        doc.to_hash.merge(doc._version_hash)
      else
        response.status = 404
        {"error" => "not_found", "reason" => "missing"}
      end
    end

    def put_doc(id, hash)
      needle = Document::Max.new(id, '0')

      if found = @docs.find_latest(needle)
        response.status = 409
        {"error" => "conflict", "reason" => "Document update conflict."}
      else
        doc = insert(Document.new(id, '0', hash))
        response['Location'] = "/#{name}/#{doc.id}"
        doc.version_hash.merge('ok' => true)
      end
    end

    def update_doc(id, rev, hash)
      doc = Document.new(id, rev, hash)

      if found = @docs.find_value(doc)
        doc.rev!(updated!)
        @docs.insert(doc) # avoid increasing the count

        doc.version_hash.merge('ok' => true)
      else
        response.status = 404
        {"error" => "not_found", "reason" => "missing"}
      end
    end

    def delete_doc(id, rev)
      needle = Document.new(id, rev)

      if found = @docs.find_value(needle)
        @docs.delete(found)
        @doc_count -= 1
        found.version_hash.merge("ok" => true)
      else
        response.status = 404
        {"error" => "not_found", "reason" => "missing"}
      end
    end

    def delete_latest_doc(id)
      needle = Document::Max.new(id, '0')

      if found = @docs.find_latest(needle)
        @docs.delete(found)
        @doc_count -= 1
        found.version_hash.merge('ok' => true)
      else
        response.status = 404
        {"error" => "not_found", "reason" => "missing"}
      end
    end

    def info
      {'db_name' => name, 'doc_count' => @doc_count}
    end

    def delete
      Innate::DynaMap.delete(url)
      Database.delete(name)

      {'ok' => true}
    end

    def url
      "/#{name}"
    end

    def insert(doc)
      @doc_count += 1
      doc.rev!(updated!)
      @docs.insert(doc)
      return doc
    end

    def [](id)
      @docs.find_value(Document.new(id))
    end

    private

    def updated!
      @update_sequence += 1
    end
  end
end

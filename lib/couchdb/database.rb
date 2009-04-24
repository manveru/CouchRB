module CouchDB
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
      @docs = RedBlackTree.new(Document::Min.new('_', '0'))
      @doc_count = 0
      Innate.map(url, self)
    end

    def call(env)
      path = (env['PATH_INFO'] || '').sub(/^\//, '')
      method = env['REQUEST_METHOD']
      body = nil

      if path.empty?
        case method
        when 'GET'
          body = info
        when 'DELETE'
          body = delete
        when 'PUT'
          response.status = 412
          body = {
            'error'  => 'file_exists',
            'reason' => 'The database could not be created, the file already exists.'
          }
        else
          response.status = 405
        end
      else
#         pp :path => path
#         pp :method => method

        case method
        when 'GET'
          if request['revs_info']
            body = Document.refs_info
          else
            body = find_doc(path)
          end
        when 'POST'
          case path
          when '_temp_view'
            body = temp_view(JSON.parse(request.body.read))
          else
            response.status = 405
          end
        when 'PUT'
          json = JSON.parse(request.body.read)

          if rev = json['_rev']
            body = update_doc(path, rev, json)
          else
            body = put_doc(path, json)
          end
        when 'DELETE'
          if rev = request['rev']
            body = delete_doc(path, rev)
          else
            response.status = 409
            body = {"error" => "conflict", "reason" => "Document update conflict."}
          end
        else
          response.status = 405
        end
      end

      response.body = body.to_json if body
      response.finish
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

    def find_doc(id)
      query = Document::Max.new(id, '0')

      if found = @docs.find_latest(query)
        found.value
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
        doc.version_hash.merge('ok' => true)
      end
    end

    def update_doc(id, rev, hash)
      doc = Document.new(id, rev, hash)

      if found = @docs.find_value(doc)
        doc.rev!
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
      doc.rev!
      @docs.insert(doc)
      return doc
    end

    def [](id)
      @docs.find_value(Document.new(id))
    end
  end
end

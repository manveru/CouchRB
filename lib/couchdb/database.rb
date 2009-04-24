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
      instance = self[name] = new(name)
      response.body = {"ok" => true}.to_json
      instance
    end

    attr_reader :name

    def initialize(name)
      @name = name
      @docs = RedBlackTree.new(Document.new('0'))
      @doc_count = 1
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
            body = self[path]
          end
        when 'POST'
          if path == '_temp_view'
            body = temp_view(JSON.parse(request.body.read))
          else
            pp request
            raise
          end
        when 'PUT'
          if rev = request[:rev]
            body = update_doc(path, rev, JSON.parse(request.body.read))
          else
            body = put_doc(path, JSON.parse(request.body.read))
          end
        when 'DELETE'
          delete_doc(path)
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

    def put_doc(id, hash)
      query = Document.new(id)

      if doc = @docs.find(query)
        {'ok' => false, 'id' => doc.id, 'rev' => doc.value.rev}
      else
        insert(Document.new(id, '1', hash))
        {'ok' => true, 'id' => doc.id, 'rev' => doc.rev}
      end
    end

    def update_doc(id, rev, hash)
      query = Document.new(id, rev)
      doc = @docs.find(query)
      doc.update_from(hash)

      {'ok' => true, 'id' => doc.id, 'rev' => doc.rev}
    end

    def delete_doc(id)
      if found = self[id]
        @docs.delete(id)
      else
      end

      {'ok' => true}
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
      @docs.insert(doc)
    end

    def [](id)
      if found = @docs.find(Document.new(id))
        found.value
      end
    end
  end
end

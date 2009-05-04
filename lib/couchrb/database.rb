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
      @docs = RedBlackTree.new(Document::Root.new('_', '0'))
      @doc_count = 0
      @update_sequence = 0
      Innate.map(url, self)
    end

    def call(env)
      path = (env['PATH_INFO'] || '').sub(/\/$/, '')
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
        body = send("#{method.to_s.downcase}_path", path, hash)
      end

      response.body = [body.to_json] if body
      response.finish
    end

    def process_empty(method, hash = {})
      case method
      when 'GET';   db_info
      when 'DELETE'; db_delete
      when 'PUT'
        if hash.empty?
          response.status = 412
          { 'error'  => 'file_exists',
            'reason' => 'The database could not be created, the file already exists.' }
        else
          put_path('/', hash)
        end
      else
        response.status = 405
        nil
      end
    end

    def get_path(path, hash = {})
      case path
      when '/_all_docs'
        direction = request[:descending] ? :descending : :ascending

        if s_key = request[:startkey]
          s_doc = Document.new(s_key, '1')

          if e_key = request[:endkey]
            e_doc = Document.new(e_key, '1')
            @docs.all_within(s_doc, e_doc, direction)
          else
            @docs.all_from(s_doc, direction)
          end
        else
          @docs.all(direction)
        end
      when '/_all_docs_by_seq'
        rows = @docs.all_by_seq
        {:total_rows => rows.size, :rows => rows, :offset => 0}
      else
        id = path.sub(/^\//, '')

        if request['revs_info']
          Document.refs_info
        elsif rev = request[:rev]
          find_doc(id, rev)
        else
          latest_doc(id)
        end
      end
    end

    def post_path(path, hash = {})
      case path
      when '/_temp_view'
        temp_view(hash)
      else
        response.status = 405
        nil
      end
    end

    def put_path(path, hash = {})
      case path
      when '/'
        id = hash['_id'] || CouchRB.uuid

        if rev = hash['_rev']
          update_doc(id, rev, hash)
        else
          put_doc(id, hash)
        end
      when /^\/(.+)/
        id = hash['_id'] || $1
        put_doc(id, hash)
      else
        response.status = 405
        nil
      end
    end

    def delete_path(path, hash = {})
      case path
      when '/'
        id = hash['_id']

        if rev = request['rev']
          delete_doc(id, rev)
        else
          delete_latest_doc(id)
        end
      when /^\/(.+)/
        id = hash['_id'] || $1

        if rev = request['rev']
          delete_doc(id, rev)
        else
          delete_latest_doc(id)
        end
      else
        response.status = 405
        nil
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
    rescue CouchRB::Exception => ex
      response.status = 400
      {'error' => 'bad_request', 'reason' => ex.message}
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

    def db_info
      {'db_name' => name, 'doc_count' => @doc_count}
    end

    def db_delete
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

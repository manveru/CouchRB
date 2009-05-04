require File.expand_path('../../lib/couchrb', __FILE__)
require 'ramaze/spec'

shared :couch_client do
  Innate.middleware!(:couchrb) do |m|
    m.innate
  end

  Innate.options.mode = :couchrb
end

class DB
  include Rack::Test::Methods

  attr_reader :name, :app

  def initialize(name)
    @name = name
    @app = Innate.middleware(:couchrb)
  end

  def delete_db
    delete(url)
  end

  def create_db
    put(url)
  end

  def info
    get(url)
    json_body
  end

  def query(doc = {})
    doc['language'] ||= 'ruby'
    json = doc.to_json

    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    post(url('_temp_view'))

    json_body
  end

  def save(doc)
    id = (doc[:_id] || (doc['_id'] ||= CouchRB.uuid))
    doc['_id'] = id
    json = doc.to_json

    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    put(url)

    jbody = json_body
    doc['_rev'] = jbody['rev']
    jbody
  end

  def delete_doc(doc)
    delete(url(doc['_id']), :rev => doc['_rev'])

    json_body
  end

  def open(id, options = {})
    got = get(url(id.to_s), options)

    json_body unless last_response.status == 404
  end

  def all_docs(options = {})
    if options.empty?
      get(url('_all_docs'))
    elsif keys = options[:keys]
      request_input(:post, url('_all_docs'), :keys => keys)
    else
      get(url('_all_docs'), options)
    end

    json_body
  end

  def all_docs_by_seq(options = {})
    if options.empty?
      get(url('_all_docs_by_seq'))
    elsif keys = options[:keys]
      request_input(:post, url('_all_docs_by_seq'), :keys => keys)
    else
      get(url('_all_docs_by_seq'), options)
    end

    json_body
  end

  def check_ok
    last_response.status.should == 200
  end

  def json_body
    JSON.parse(last_response.body)
  end

  private

  def url(*suffix)
    ::File.join('/', name, *suffix)
  end

  def request_input(method, url, input, options = {})
    json = input.to_json
    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    __send__(method, url, options)
  end
end

__END__

  Innate.options.mode = :couchrb

  behaves_like :mock

  def json_body
    JSON.parse(last_response.body)
  end

  def put_doc(url, doc)
    json = doc.to_json

    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    put(url)
  end

  def post_doc(url, doc)
    json = doc.to_json

    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    post(url)
  end

  def query(url, hash = {})
    doc = {'language' => 'ruby'}
    doc['map'] = hash[:map] if hash[:map]
    doc['reduce'] = hash[:reduce] if hash[:reduce]
    post_doc(url, doc)
  end
end

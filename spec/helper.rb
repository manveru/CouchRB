require File.expand_path('../../lib/couchrb', __FILE__)
require 'ramaze/spec'

shared :couch_client do
  Innate.middleware!(:couchrb) do |m|
    m.innate
  end

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

  def save(doc)
    id = doc['_id'] ||= CouchRB.uuid
    json = doc.to_json

    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    put(url(id))
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

  private

  def url(*suffix)
    ::File.join('/', name, *suffix)
  end

  def json_body
    JSON.parse(last_response.body)
  end

  def request_input(method, url, input, options = {})
    json = input.to_json
    header 'CONTENT_LENGTH', json.bytesize
    header 'CONTENT_TYPE', 'application/json'
    header :input, json
    __send__(method, url, options)
  end
end

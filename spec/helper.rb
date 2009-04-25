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

  attr_reader :name

  def initialize(name)
    @name = name
  end

  def delete
    super("/#{name}")
  end

  def create
    put("/#{name}")
  end
end


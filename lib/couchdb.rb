require 'json'
require 'innate'

require 'couchdb/rbtree'
require 'couchdb/document'
require 'couchdb/view_context'
require 'couchdb/database'

module CouchDB
  extend Innate::Trinity

  def self.call(env)
    # p 'CouchDB::call(env)' => env
    if request.get?
      response.body = {'couchdb' => 'Welcome', 'version' => '0.9.0'}.to_json
    elsif request.put?
      name = request.env['PATH_INFO'].sub(/^\//, '')
      Database.create(name)
    elsif request.delete?
      response.status = 404
      response.body = {"error" => "not_found", "reason" => "Missing"}.to_json
    end

    response.finish
  end

  Innate.map('/', self)
end

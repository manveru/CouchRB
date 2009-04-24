require 'securerandom'
require 'json'

require 'innate'

require 'couchdb/rbtree'
require 'couchdb/document'
require 'couchdb/view_context'
require 'couchdb/database'

module CouchDB
  extend Innate::Trinity

  def self.call(env)
    path = (env['PATH_INFO'] || '').sub(/^\//, '')

    case env['REQUEST_METHOD']
    when 'GET'
      case path
      when '_uuids'
        count = (request[:count] || 1).to_i
        body = uuids(count)
      when '_all_dbs'
        response.status = 501
      when '_stats'
        response.status = 501
      else
        body = {'couchdb' => 'Welcome', 'version' => '0.9.0'}
      end
    when 'POST'
      case path
      when '_replicate'
        response.status = 501
      else
        response.status = 501
      end
    when 'PUT'
      body = Database.create(path)
    when 'DELETE'
      response.status = 404
      body = {"error" => "not_found", "reason" => "Missing"}
    else
      response.status = 405
    end

    response.body = body.to_json
    response.finish
  end

  def self.uuids(count)
    response['Cache-Control'] = 'no-cache'
    response['Pragma'] = 'no-cache'
    response['Expires'] = Time.now.httpdate
    response['Date'] = Time.now.httpdate
    {'uuids' => Array.new(count){ SecureRandom.hex }}
  end

  Innate.map('/', self)
end

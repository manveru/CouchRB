require 'securerandom'
require 'json'

require 'innate'

$LOAD_PATH.unshift(File.expand_path('../', __FILE__))

require 'couchrb/red_black_tree'
require 'couchrb/binary_tree'
require 'couchrb/document'
require 'couchrb/view_context'
require 'couchrb/database'

module CouchRB
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
      when '_uuids'
        response.status = 405
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




require 'thread'
require 'open3'
require 'pp'

Thread.abort_on_exception = true

log = Hash.new{|h,k| h[k] = Queue.new }

out = $stdout.dup

Thread.new do
  loop do
    log.each do |key, queue|
      if line = queue.deq
        out.puts({key => line}.inspect)
      end
    end

    sleep 0.1
  end
end

input = ['this']

Open3.popen3('js -i'){|stdin, stdout, stderr|
  ios = [stdin, stdout, stderr]

  begin
    read, write, error = all = select(ios, ios, ios)
    log[:all] << all
    running = all

    if r = [read, error].flatten.first
      log[:read] << r.read(1)
    end

    if w = write.first
      if line = input.shift
        w.puts(line)
      end
    end
  end while running
}

__END__

require 'lib/couchdb/seamonkey'

sm = CouchDB::SeaMonkey.new
sm.spawn
sm.puts 'print("Hello, World!");'
# sm.kill

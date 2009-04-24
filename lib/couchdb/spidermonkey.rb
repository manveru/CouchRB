require 'open3'
require 'thread'

module CouchDB
  # Abstract communication with the SpiderMonkey javascript interpreter
  #
  # This started out as a grand plan, but after 3 hours of messing around I
  # still haven't found a way to communicate interactively with the darn thing.
  # Everybody uses the C binding, but that's not an option for this project,
  # enlightenment welcome.
  # For now I'll concentrate on a ruby implementation, blocks FTW!
  class SpiderMonkey
    def initialize
      @in = Queue.new
      @out = Queue.new
    end

    def spawn
      Thread.new do
        Open3.popen3('echo "print(this);" | js'){|stdin, stdout, stderr|
          while input = @in.deq
            method, *args = input
            p [method, args]
            stdin.send(method, *args)

            while line = stdout.gets
              p line
              @out << line
            end
          end
        }
      end
    end

    def kill
      @in << :close
    end

    def puts(*strings)
      @in << [:puts, strings.join("\n")]
      @out.deq
    end
  end
end

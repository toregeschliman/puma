# frozen_string_literal: true

require_relative 'pipe_protocols'

module Puma
  class Cluster < Puma::Runner
    #—————————————————————— DO NOT USE — this class is for internal use only ———


    # This class is instantiated by the `Puma::Cluster` and represents a single
    # worker process.
    #
    # At the core of this class is running an instance of `Puma::Server` which
    # gets created via the `start_server` method from the `Puma::Runner` class
    # that this inherits from.
    class Worker < Puma::Runner # :nodoc:
      attr_reader :index, :master

      def initialize(index:, master:, launcher:, pipes:, server: nil)
        super(launcher)

        @index = index
        @master = master
        @check_pipe = pipes[:check_pipe]
        @worker_write = pipes[:worker_write]
        @fork_pipe = pipes[:fork_pipe]
        @wakeup = pipes[:wakeup]
        @server = server
        @hook_data = {}
        @mold = false
      end

      def run
        set_proc_title

        Signal.trap "SIGINT", "IGNORE"
        Signal.trap "SIGCHLD", "DEFAULT"

        Thread.new do
          Puma.set_thread_name "wrkr check"
          @check_pipe.wait_readable
          log "! Detected parent died, dying"
          exit! 1
        end

        # If we're not running under a Bundler context, then
        # report the info about the context we will be using
        if !ENV['BUNDLE_GEMFILE']
          if File.exist?("Gemfile")
            log "+ Gemfile in context: #{File.expand_path("Gemfile")}"
          elsif File.exist?("gems.rb")
            log "+ Gemfile in context: #{File.expand_path("gems.rb")}"
          end
        end

        # Invoke any worker boot hooks so they can get
        # things in shape before booting the app.
        @config.run_hooks(:before_worker_boot, index, @log_writer, @hook_data)

        begin
        server = @server ||= start_server
        rescue Exception => e
          log "! Unable to start worker"
          log e
          log e.backtrace.join("\n    ")
          exit 1
        end

        restart_server = Queue.new << true << false

        fork_worker = @options[:fork_worker] && index == 0

        if fork_worker
          restart_server.clear
          worker_pids = []
          Signal.trap "SIGCHLD" do
            wakeup! if worker_pids.reject! do |p|
              Process.wait(p, Process::WNOHANG) rescue true
            end
          end

          Thread.new do
            Puma.set_thread_name "wrkr fork"
            while (idx = @fork_pipe.gets)
              idx = idx.to_i
              if idx == -1 # stop server
                if restart_server.length > 0
                  restart_server.clear
                  server.begin_restart(true)
                  @config.run_hooks(:before_refork, nil, @log_writer, @hook_data)
                end
              elsif idx == -2 # refork cycle is done
                @config.run_hooks(:after_refork, nil, @log_writer, @hook_data)
              elsif idx == 0 # restart server
                restart_server << true << false
              else # fork worker
                worker_pids << pid = spawn_worker(idx)
                @worker_write << "#{PIPE_FORK}#{pid}:#{idx}\n" rescue nil
              end
            end
          end
        end

        if @options[:mold_worker]
          Signal.trap("SIGURG") do
            @mold = true
            restart_server.clear
            restart_server << false
            server.begin_restart(true)
          end
        end

        Signal.trap "SIGTERM" do
          @worker_write << "#{PIPE_EXTERNAL_TERM}#{Process.pid}\n" rescue nil
          restart_server.clear
          server.stop
          restart_server << false
        end

        begin
          @worker_write << "#{PIPE_BOOT}#{Process.pid}:#{index}\n"
        rescue SystemCallError, IOError
          Puma::Util.purge_interrupt_queue
          STDERR.puts "Master seems to have exited, exiting."
          return
        end

        while restart_server.pop
          server_thread = server.run

          if @log_writer.debug? && index == 0
            debug_loaded_extensions "Loaded Extensions - worker 0:"
          end

          make_sure_pinging(server)

          server_thread.join
        end

        # Invoke any worker shutdown hooks so they can prevent the worker
        # exiting until any background operations are completed
        @config.run_hooks(:before_worker_shutdown, index, @log_writer, @hook_data)

        if @mold
          set_proc_title(role: "mold")

          Signal.trap("SIGTERM") do
            @worker_write << "#{PIPE_EXTERNAL_TERM}#{Process.pid}\n" rescue nil
            @fork_pipe.close
          end

          worker_pids = []
          Signal.trap "SIGCHLD" do
            wakeup! if worker_pids.reject! do |p|
              Process.wait(p, Process::WNOHANG) rescue true
            end
          end

          @config.run_hooks(:on_mold_promotion, index, @log_writer, @hook_data)

          make_sure_pinging(server)
          wakeup!

          begin
            while (idx = PipeProtocols::Fork.read_from(@fork_pipe))
              worker_pids << pid = spawn_worker(idx)
              @worker_write << "#{PIPE_FORK}#{pid}:#{idx}\n"
              log "Forked worker #{idx} with pid #{pid}"
            end
          rescue StandardError => e
            log "Fork pipe terminated with exception: #{e.inspect}"
          end

          @config.run_hooks(:on_mold_shutdown, index, @log_writer, @hook_data)
        end
      ensure
        @worker_write << "#{PIPE_TERM}#{Process.pid}\n"
      end

      private

      def make_sure_pinging(server)
        # if the stat thread died, join and replace it
        if @thread && !@thread.alive?
          begin
            @thread.join rescue nil # just ignore exceptions here
            @thread = nil
          rescue => e
            log "While joining stat thread rescued #{e.class}: #{e.message}"
          end
        end

        @thread ||= Thread.new(@worker_write) do |io|
          Puma.set_thread_name "stat pld"
          base_payload = "#{PIPE_PING}#{Process.pid}"

          while true
            begin
              b = server.backlog || 0
              r = server.running || 0
              t = server.pool_capacity || 0
              m = server.max_threads || 0
              rc = server.requests_count || 0
              bt = server.busy_threads || 0
              payload = %Q!#{base_payload}{ "backlog":#{b}, "running":#{r}, "pool_capacity":#{t}, "max_threads":#{m}, "requests_count":#{rc}, "busy_threads":#{bt} }\n!
              io << payload
            rescue IOError
              Puma::Util.purge_interrupt_queue
              break
            end
            sleep @options[:worker_check_interval]
          end
        end

      end

      def set_proc_title(role: "worker")
        title  = "puma: #{role} #{index}: #{master}"
        title += " [#{@options[:tag]}]" if @options[:tag] && !@options[:tag].empty?
        $0 = title
      end

      def spawn_worker(idx)
        @config.run_hooks(:before_worker_fork, idx, @log_writer, @hook_data)

        pipes = {
          check_pipe: @check_pipe,
          worker_write: @worker_write,
        }

        if @options[:mold_worker]
          pipes[:fork_pipe] = @fork_pipe
          pipes[:wakeup] = @wakeup
        end

        pid = fork do
          new_worker = Worker.new index: idx,
                                  master: master,
                                  launcher: @launcher,
                                  pipes: pipes,
                                  server: @server
          new_worker.run
        end

        if !pid
          log "! Complete inability to spawn new workers detected"
          log "! Seppuku is the only choice."
          exit! 1
        end

        @config.run_hooks(:after_worker_fork, idx, @log_writer, @hook_data)
        pid
      end
    end
  end
end

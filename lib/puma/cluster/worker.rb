# frozen_string_literal: true

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
        Signal.trap "SIGURG", "DEFAULT"

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

        restart_server = @restart_server ||= Queue.new << true << false

        fork_worker = @options[:fork_worker]

        if fork_worker
          worker_pids = @worker_pids ||= []
          Signal.trap "SIGCHLD" do
            wakeup! if worker_pids.reject! do |p|
              Process.wait(p, Process::WNOHANG) rescue true
            end
          end
          Signal.trap "SIGURG" do
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

          stat_thread ||= Thread.new(@worker_write) do |io|
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
          server_thread.join
        end

        if fork_worker && @mold
          set_proc_title "mold"
          @config.run_hooks(:before_molding, nil, @log_writer, @hook_data)
          fork_worker_loop
        end

        # Invoke any worker shutdown hooks so they can prevent the worker
        # exiting until any background operations are completed
        @config.run_hooks(:before_worker_shutdown, index, @log_writer, @hook_data)
      ensure
        @worker_write << "#{PIPE_TERM}#{Process.pid}\n" rescue nil
        @worker_write.close
      end

      private

      def set_proc_title(role = "worker")
        title  = "puma: cluster #{role} #{index}: #{master}"
        title += " [#{@options[:tag]}]" if @options[:tag] && !@options[:tag].empty?
        $0 = title
      end

      def fork_worker_loop
        forking = true

        Signal.trap("SIGTERM") do
          forking = false
          @fork_pipe.close rescue nil
          @worker_write << "#{PIPE_EXTERNAL_TERM}#{Process.pid}\n" rescue nil
        end

        @config.run_hooks(:before_refork, nil, @log_writer, @hook_data)

        worker_pids = @worker_pids
        while forking && (idx = @fork_pipe.gets)
          idx = idx.to_i
          if idx == -1 # run before_refork hooks
            # these do nothing anymore
          elsif idx == -2 # run after_refork hooks
            # this is meaningless with molds
          elsif idx == 0
            # do nothing, we don't restart the server anymore
          else # fork worker
            worker_pids << pid = spawn_worker(idx)
            @worker_write << "#{PIPE_FORK}#{pid}:#{idx}\n" rescue nil
          end
        end
      rescue IOError
        # fork_pipe was closed, either parent died or a SIGTERM killed it
        # parent died is already handled, so only need to handle SIGTERM
        # but nothing to do in that case either.
      end

      def spawn_worker(idx)
        @config.run_hooks(:before_worker_fork, idx, @log_writer, @hook_data)

        pid = fork do
          new_worker = Worker.new index: idx,
                                  master: master,
                                  launcher: @launcher,
                                  pipes: { check_pipe: @check_pipe,
                                           worker_write: @worker_write },
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

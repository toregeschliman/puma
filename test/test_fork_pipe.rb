# frozen_string_literal: true

require_relative "helper"
require "puma/cluster/fork_pipe"

# These are tests to ensure that the protocol for fork_pipe works as expected
class TestForkPipe < PumaTest
  def test_fork_pipe_restart_server
    r, w = IO.pipe

    reader = Puma::Cluster::ForkPipeReader.new(r)
    writer = Puma::Cluster::ForkPipeWriter.new(w)
    writer.restart_server
    assert_equal Puma::Cluster::ForkPipeReader::RESTART_SERVER, reader.read

    r.close
    w.close
  end
end

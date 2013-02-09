require 'spec_helper'

describe SuperQueue do

  before :each do
    Fog.mock!
    @default_required_options = {
      :aws_access_key_id => "abc123",
      :aws_secret_access_key => "123abc",
      :name => "rspec-test",
      :buffer_size => 5
    }
  end

  describe "#new" do
    queue = SuperQueue.new(@default_required_options)
    queue.should be_an_instance_of SuperQueue
  end
end

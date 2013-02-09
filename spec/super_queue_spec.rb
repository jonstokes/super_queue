require 'spec_helper'

describe SuperQueue do
  before :each do
    Fog.mock!
    let(:aws_options) { {:aws_access_key_id => "abc123", :aws_secret_access_key => "123abc"} }
  end

  describe "#new" do
    queue = SuperQueue.new(aws_options)
    queue.should be_an_instance_of SuperQueue
  end
end

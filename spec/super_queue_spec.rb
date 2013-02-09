require 'spec_helper'

describe SuperQueue do

  before :each do
    Fog.mock!
    @defaults = {
      :aws_access_key_id => "abc123",
      :aws_secret_access_key => "123abc",
      :name => "rspec-test",
      :buffer_size => 5
    }
  end

  describe "#new" do
    it "should create a new SuperQueue" do
      queue = SuperQueue.new(@defaults)
      queue.should be_an_instance_of SuperQueue
    end
  end

  it "should require non-nil options" do
    expect {
      SuperQueue.new(nil)
    }.to raise_error(RuntimeError, "Options can't be nil!")
  end

  it "should require an AWS access key ID" do
    @defaults.delete(:aws_access_key_id)
    expect {
      SuperQueue.new(@defaults)
    }.to raise_error("AWS credentials :aws_access_key_id and :aws_secret_access_key required!")
  end

  it "should require an AWS secret" do
    @defaults.delete(:aws_secret_access_key)
    expect {
      SuperQueue.new(@defaults)
    }.to raise_error("AWS credentials :aws_access_key_id and :aws_secret_access_key required!")
  end

end

require 'spec_helper'

describe SuperQueue do

  before :each do
    SuperQueue.mock!
    @defaults = {
      :aws_access_key_id => "abc123",
      :aws_secret_access_key => "123abc",
    }
  end

  describe "#new" do
    it "should require non-nil options" do
      expect {
        SuperQueue.new(nil)
      }.to raise_error(RuntimeError, "Options can't be nil!")
    end

    describe "with standard defaults" do
      before :each do
        @queue = SuperQueue.new(@defaults)
      end

      it "should create a new SuperQueue" do
        @queue.should be_an_instance_of SuperQueue
      end

      it "should create a new SuperQueue with a url" do
        @queue.url.should include("http")
      end

      it "should create a new localized SuperQueue by default" do
        @queue.should be_localized
        @queue.name.should include("f528764d624db129b32c21fbca0cb8d6")
      end
    end

    describe "with missing or incorrect defaults" do
      it "should require an AWS access key ID" do
        @defaults.delete(:aws_access_key_id)
        expect {
          SuperQueue.new(@defaults)
        }.to raise_error(RuntimeError, "AWS credentials :aws_access_key_id and :aws_secret_access_key required!")
      end

      it "should require an AWS secret" do
        @defaults.delete(:aws_secret_access_key)
        expect {
          SuperQueue.new(@defaults)
        }.to raise_error(RuntimeError, "AWS credentials :aws_access_key_id and :aws_secret_access_key required!")
      end

      it "should require a minimum buffer size" do
        @defaults.merge!(:buffer_size => 0)
        expect {
          SuperQueue.new(@defaults)
        }.to raise_error(RuntimeError, "Minimun :buffer_size is 5.")
      end

      it "should require that visibility timeout be an integer" do
        @defaults.merge!(:visibility_timeout => "a")
        expect {
          SuperQueue.new(@defaults)
        }.to raise_error(RuntimeError, "Visbility timeout must be an integer (in seconds)!")
      end
    end

    describe "with optional options" do
      it "should use a supplied name" do
        @defaults.merge!(:name => "rspec-test")
        @queue = SuperQueue.new(@defaults)
        @queue.url.should include(@defaults[:name])
        @queue.name.should include(@defaults[:name])
      end
    end
  end

  describe "#push" do
    it "should add an element" do
      queue = SuperQueue.new(@defaults)
      queue.push "bar"
      sleep 0.5 # to let item propagate from in_buffer to SQS queue
      queue.length.should == 1
      queue.clear
    end
  end

  describe "#pop" do
    it "should remove an element" do
      queue = SuperQueue.new(@defaults)
      queue.push "foo"
      sleep 0.5 # to let item propagate from in_buffer to SQS queue
      queue.pop(true).should == "foo"
    end
  end

  describe "#clear" do
    it "should clear the queue" do
      queue = SuperQueue.new(@defaults)
      queue.push :item1
      queue.push :item2
      sleep 0.5 # to let items propagate from in_buffer to SQS queue
      queue.clear
      queue.length.should == 0
    end
  end

  describe "#length" do
    it "should give the length of the queue" do
      queue = SuperQueue.new(@defaults)
      queue.push :item1
      queue.push :item2
      sleep 0.5 # to let items propagate from in_buffer to SQS queue
      queue.length.should == 2
    end
  end

  describe "#empty?" do
    it "should should return true if the queue is empty" do
      queue = SuperQueue.new(@defaults)
      queue.should be_empty
    end

    it "should should return false if the queue is not empty" do
      queue = SuperQueue.new(@defaults)
      queue.push :item1
      sleep 0.5 # to let items propagate from in_buffer to SQS queue
      queue.should_not be_empty
    end
  end

  describe "#shutdown" do
    it "should not blow up" do
      queue = SuperQueue.new(@defaults)
      expect { queue.shutdown }.not_to raise_error
    end
  end

  describe "#destroy" do
    it "should not blow up" do
      queue = SuperQueue.new(@defaults)
      expect { queue.destroy }.not_to raise_error
    end
  end
end

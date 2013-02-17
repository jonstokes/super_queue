require 'aws-sdk'
require 'base64'
require 'digest/md5'

class SuperQueue

  class S3Pointer < Hash
    def initialize(key)
      super
      self.merge!(:s3_key => key)
    end

    def s3_key
      self[:s3_key]
    end

    def s3_key=(value)
      self[:s3_key] = value
    end
  end

  def initialize(opts)
    AWS.eager_autoload! # for thread safety
    check_opts(opts)
    @buffer_size = opts[:buffer_size] || 100
    @use_s3 = opts[:use_s3]
    @queue_name = generate_queue_name(opts)
    @request_count = 0
    initialize_aws(opts)

    @waiting = []
    @waiting.taint
    self.taint
    @mutex = Mutex.new
    @in_buffer = []
    @out_buffer = []
    @deletion_buffer = []
    @deletion_queue = []

    @gc = Thread.new do 
      begin
        collect_garbage
      rescue Exception => @gc_error
        raise @gc_error
      end
    end
  end

  def push(p)
    check_for_errors
    @mutex.synchronize {
      @in_buffer.push p
      clear_in_buffer if @in_buffer.size >= @buffer_size
      begin
        t = @waiting.shift
        t.wakeup if t
      rescue ThreadError
        retry
      end
    }
  end

  def pop(non_block=false)
    check_for_errors
    @mutex.synchronize {
      loop do
        if @out_buffer.empty?
          if fill_out_buffer_from_sqs_queue || fill_out_buffer_from_in_buffer
            return pop_out_buffer
          else
            raise ThreadError, "queue empty" if non_block
            @waiting.push Thread.current
            @mutex.sleep
          end
        else
          return pop_out_buffer
        end
      end
    }
  end

  def length
    check_for_errors
    @mutex.synchronize {
      sqsl = sqs_length
      return sqsl + @in_buffer.size + @out_buffer.size
    }
  end

  def num_waiting
    check_for_errors
    @waiting.size
  end

  def empty?
    self.length == 0
  end

  def clear
    begin
      self.pop(true)
    rescue ThreadError
      retry unless self.empty?
    end until self.empty?
  end

  def shutdown
    @mutex.synchronize { clear_in_buffer }
    @gc.terminate
    @mutex.synchronize { fill_deletion_queue_from_buffer } if @deletion_buffer.any?
    @mutex.synchronize { clear_deletion_queue } if @deletion_queue.any?
    @done = true
  end

  def destroy
    @gc.terminate
    delete_aws_resources
    @done = true
  end

  def sqs_requests
    check_for_errors
    @request_count
  end

  alias enq push
  alias << push

  alias deq pop
  alias shift pop

  alias size length

  def url
    q_url
  end

  def name
    queue_name
  end

  private

  #
  # Amazon AWS methods
  #
  def initialize_aws(opts)
    aws_options = {
      :access_key_id => opts[:aws_access_key_id], 
      :secret_access_key => opts[:aws_secret_access_key]
    }
    @sqs = AWS::SQS.new(aws_options)
    @s3 = AWS::S3.new(aws_options)
    create_sqs_queue(opts)
    if opts[:replace_existing_queue] && (sqs_length > 0)
      delete_queue
      puts "Waiting 60s to create new queue..."
      sleep 62 # You must wait 60s after deleting a q to create one with the same name
      create_sqs_queue(opts)
    end
    @bucket = open_s3_bucket
  end

  def create_sqs_queue(opts)
    @sqs_queue = find_queue_by_name || new_sqs_queue(opts)
    check_for_queue_creation_success
  end

  def open_s3_bucket
    @s3.buckets[queue_name].exists? ? @s3.buckets[queue_name] : @s3.buckets.create(queue_name)
  end

  def find_queue_by_name
    begin
      @sqs.queues.named(queue_name)
    rescue AWS::SQS::Errors::NonExistentQueue
      return nil
    end
  end

  def new_sqs_queue(opts)
    @request_count += 1
    if opts[:visibility_timeout]
      @sqs.queues.create(queue_name, { :visibility_timeout => opts[:visibility_timeout] })
    else
      @sqs.queues.create(queue_name)
    end
  end

  def check_for_queue_creation_success
    retries = 0
    while q_url.nil? && (retries < 5)
      retries += 1
      sleep 1
    end
    raise "Couldn't create queue #{queue_name}, or delete existing queue by this name." if q_url.nil?
  end

  def send_messages_to_queue
    number_of_batches = @in_buffer.size / 10
    number_of_batches += 1 if @in_buffer.size % 10
    batches = []
    number_of_batches.times do
      batch = []
      10.times do
        next if @in_buffer.empty?
        p = @in_buffer.shift
        unless should_send_to_s3?(p)
          batch << encode(p)
        else
          batch << encode(send_payload_to_s3(p))
        end
      end
      batches << batch
    end

    batches.each do |b|
      @request_count += 1
      @sqs_queue.batch_send(b) if b.any?
    end
  end

  def get_messages_from_queue(number_of_messages_to_receive)
    messages = []
    number_of_batches = number_of_messages_to_receive / 10
    number_of_batches += 1 if number_of_messages_to_receive % 10
    number_of_batches.times do
      batch = @sqs_queue.receive_messages(:limit => 10).compact
      batch.each do |message|
        obj = decode(message.body)
        unless obj.is_a?(SuperQueue::S3Pointer)
          messages << {
            :payload    => obj,
            :sqs_handle => message
          }
        else
          p = fetch_payload_from_s3(obj)
          messages << {
            :payload    => p,
            :sqs_handle => message,
            :s3_key     => obj.s3_key } if p
        end
      end
      @request_count += 1
    end
    messages
  end

  def send_payload_to_s3(p)
    dump = Marshal.dump(p)
    digest = Digest::MD5.hexdigest(dump)
    return digest if @bucket.objects[digest].exists?
    retryable(:tries => 5) { @bucket.objects[digest].write(dump) }
    S3Pointer.new(digest)
  end

  def fetch_payload_from_s3(pointer)
    payload = nil
    retries = 0
    begin
      payload = Marshal.load(@bucket.objects[pointer.s3_key].read)
    rescue AWS::S3::Errors::NoSuchKey
      return nil
    rescue
      retries +=1
      retry if retries < 5
    end
    payload
  end

  def should_send_to_s3?(p)
    @use_s3
  end

  def sqs_length
    n = @sqs_queue.approximate_number_of_messages
    return n.is_a?(Integer) ? n : 0
  end

  def delete_aws_resources
    @request_count += 1
    @sqs_queue.delete
    @bucket.delete!
  end

  def clear_deletion_queue
    while !@deletion_queue.empty?
      sqs_handles = @deletion_queue[0..9].map { |m| m[:sqs_handle] }.compact
      s3_keys = @deletion_queue[0..9].map { |m| m[:s3_key] }.compact
      10.times { @deletion_queue.shift }
      @sqs_queue.batch_delete(sqs_handles) if sqs_handles.any?
      s3_keys.each { |key| @bucket.objects[key].delete }
      @request_count += 1
    end
  end

  #
  # Buffer-related methods
  #
  def fill_out_buffer_from_sqs_queue
    return false if sqs_length == 0
    nil_count = 0
    while (@out_buffer.size < @buffer_size) && (nil_count < 2)
      messages = get_messages_from_queue(@buffer_size - @out_buffer.size)
      if messages.empty?
        nil_count += 1
      else
        messages.each { |m| @out_buffer.push(m) }
        nil_count = 0
      end
    end
    !@out_buffer.empty?
  end

  def fill_out_buffer_from_in_buffer
    return false if @in_buffer.empty?
    while (@out_buffer.size <= @buffer_size) && !@in_buffer.empty?
      @out_buffer.push(:payload => @in_buffer.shift)
    end
    !@out_buffer.empty?
  end

  def fill_deletion_queue_from_buffer
    @deletion_queue += @deletion_buffer
    @deletion_buffer = []
  end

  def pop_out_buffer
    m = @out_buffer.shift
    @deletion_buffer << { :sqs_handle => m[:sqs_handle], :s3_key => m[:s3_key] }
    m[:payload]
  end

  def clear_in_buffer
    while !@in_buffer.empty? do
      send_messages_to_queue
    end
  end

  #
  # Misc helper methods
  #
  def check_opts(opts)
    raise "Options can't be nil!" if opts.nil?
    raise "Minimun :buffer_size is 5." if opts[:buffer_size] && (opts[:buffer_size] < 5)
    raise "AWS credentials :aws_access_key_id and :aws_secret_access_key required!" unless opts[:aws_access_key_id] && opts[:aws_secret_access_key]
    raise "Visbility timeout must be an integer (in seconds)!" if opts[:visibility_timeout] && !opts[:visibility_timeout].is_a?(Integer)
  end

  def check_for_errors
    raise @gc_error if @gc_error
    raise @sqs_error if @sqs_error
    raise "Queue is no longer available!" if @done == true
  end

  def encode(p)
    Base64.urlsafe_encode64(Marshal.dump(p))
  end

  def decode(ser_obj)
    Marshal.load(Base64.urlsafe_decode64(ser_obj))
  end

  def generate_queue_name(opts)
    q_name = opts[:name] || random_name
    return opts[:namespace] ? "queue-#{opts[:namespace]}-#{q_name}" : "queue-#{q_name}"
  end

  def retryable(options = {}, &block)
    opts = { :tries => 1, :on => Exception }.merge(options)

    retry_exception, retries = opts[:on], opts[:tries]

    begin
      return yield
    rescue retry_exception
      retry if (retries -= 1) > 0
    end

    yield
  end

  #
  # Virtul attributes and convenience methods
  #
  def q_url
    return @q_url if @q_url
    @q_url = @sqs_queue.url
    @q_url
  end

  def random_name
    o =  [('a'..'z'),('1'..'9')].map{|i| i.to_a}.flatten
    random_element = (0...25).map{ o[rand(o.length)] }.join
    "temp-name-#{random_element}"
  end

  def queue_name
    @queue_name
  end

  #
  # Maintence thread-related methods
  #

  def collect_garbage
    loop do
      #This also needs a condition to clear the del queue if there are any handles where the invisibility is about to expire
      @mutex.synchronize { fill_deletion_queue_from_buffer } if @deletion_buffer.any?
      Thread.pass
      @mutex.synchronize { clear_deletion_queue } if @deletion_queue.any?
    end
  end
end

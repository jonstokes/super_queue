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
    check_opts(opts)
    @buffer_size = opts[:buffer_size] || 100
    @queue_name = generate_queue_name(opts)
    @bucket_name = opts[:bucket_name] || queue_name
    @write_count = 0
    @read_count = 0
    @delete_count = 0
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

    fill_out_buffer_from_sqs_queue
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
        if @out_buffer.empty? && !(fill_out_buffer_from_sqs_queue || fill_out_buffer_from_in_buffer)
          raise ThreadError, "queue empty" if non_block
          @waiting.push Thread.current
          @mutex.sleep
        else
          return pop_out_buffer
        end
      end
    }
  end

  def length
    check_for_errors
    @mutex.synchronize {
      return sqs_length + @in_buffer.size + @out_buffer.size
    }
  end

  def num_waiting
    check_for_errors
    @waiting.size
  end

  def empty?
    len = 0
    2.times { len += self.length; sleep(0.01) }
    len == 0
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
    @write_count + @read_count + @delete_count
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
    retryable(:tries => 3) do
      @s3.buckets[@bucket_name].exists? ? @s3.buckets[@bucket_name] : @s3.buckets.create(@bucket_name)
    end
  end

  def find_queue_by_name
    retries = 0
    begin
      @sqs.queues.named(queue_name)
    rescue AWS::SQS::Errors::NonExistentQueue
      return nil
    rescue NoMethodError => e
      sleep 1
      retries += 1
      retry if retries < 5
      raise e
    end
  end

  def new_sqs_queue(opts)
    @read_count += 1
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

  def send_messages_to_queue(batches)
    batches.each do |b|
      @write_count += 1
      retryable(:tries => 5) do
        @sqs_queue.batch_send(b)
      end if b.any?
    end
  end

  def get_messages_from_queue(number_of_messages_to_receive)
    messages = []
    number_of_batches = number_of_messages_to_receive / 10
    number_of_batches += 1 if number_of_messages_to_receive % 10
    number_of_batches.times do
      retryable(:retries => 5) { messages += @sqs_queue.receive_messages(:limit => 10).compact }
      @read_count += 1
    end
    messages.compact
  end

  def send_payload_to_s3(encoded_message)
    key = "#{queue_name}/#{Digest::MD5.hexdigest(encoded_message)}"
    key_exists = false
    retryable(:tries => 5) { key_exists = @bucket.objects[key].exists? }
    return S3Pointer.new(key) if key_exists
    retryable(:tries => 5) { @bucket.objects[key].write(encoded_message, :reduced_redundancy => true) }
    S3Pointer.new(key)
  end

  def fetch_payload_from_s3(pointer)
    payload = nil
    retries = 0
    begin
      payload = decode(@bucket.objects[pointer.s3_key].read)
    rescue AWS::S3::Errors::NoSuchKey
      return nil
    rescue
      retries +=1
      retry if retries < 5
    end
    payload
  end

  def should_send_to_s3?(encoded_message)
    encoded_message.bytesize > 64000
  end

  def sqs_length
    n = 0
    retryable(:retries => 5) { n = @sqs_queue.approximate_number_of_messages }
    @read_count += 1
    return n.is_a?(Integer) ? n : 0
  end

  def delete_aws_resources
    @delete_count += 1
    @sqs_queue.delete
    begin
      @bucket.clear!
      sleep 1
    end until @bucket.empty?
    @bucket.delete
  end

  def clear_deletion_queue
    retryable(:tries => 4) do
      while !@deletion_queue.empty?
        sqs_handles = @deletion_queue[0..9].map { |m| m[:sqs_handle].is_a?(AWS::SQS::ReceivedMessage) ? m[:sqs_handle] : nil }.compact
        s3_keys = @deletion_queue[0..9].map { |m| m[:s3_key] }.compact
        10.times { @deletion_queue.shift }
        @sqs_queue.batch_delete(sqs_handles) if sqs_handles.any?
        s3_keys.each { |key| @bucket.objects[key].delete }
        @delete_count += 1
      end
    end
  end

  #
  # Buffer-related methods
  #
  def fill_out_buffer_from_sqs_queue
    return false if sqs_length == 0
    nil_count = 0
    while (@out_buffer.size < @buffer_size) && (nil_count < 5)
      messages = get_messages_from_queue(@buffer_size - @out_buffer.size)
      if messages.any?
        messages.each do |message|
          obj = decode(message.body)
          unless obj.is_a?(SuperQueue::S3Pointer)
            @out_buffer.push(
              :payload    => obj,
              :sqs_handle => message)
          else
            if p = fetch_payload_from_s3(obj)
              @out_buffer.push(
                :payload    => p,
                :sqs_handle => message,
                :s3_key     => obj.s3_key)
            else
              @deletion_buffer.push(:sqs_handle => message, :s3_key => obj.s3_key)
            end
          end
        end
        nil_count = 0
        sleep 0.01
      else
        nil_count += 1
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
    batches = []
    message_stash = []
    while !@in_buffer.empty? do
      batch = message_stash
      message_stash = []
      message_count = batch.size
      batch_too_big = false
      while !@in_buffer.empty? && !batch_too_big && (message_count < 10) do
        encoded_message = encode(@in_buffer.shift)
        message = should_send_to_s3?(encoded_message) ? encode(send_payload_to_s3(encoded_message)) : encoded_message
        if (batch_bytesize(batch) + message.bytesize) < 64000
          batch << message
          batch_too_big == false
          message_count += 1
        else
          message_stash << message
          batch_too_big = true
        end
      end
      batches << batch
    end
    send_messages_to_queue(batches)
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

  def batch_bytesize(batch)
    sum = 0
    batch.each do |string|
      sum += string.bytesize
    end
    sum
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
      sleep 1
    end
  end
end

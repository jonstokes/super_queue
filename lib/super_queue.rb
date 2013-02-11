require 'fog'
require 'base64'
require 'socket'
require 'digest/md5'
require 'zlib'

class SuperQueue

  def self.mock!
    @@mock = true
    Fog.mock!
  end

  def self.mocking?
    defined?(@@mock) && @@mock
  end

  def initialize(opts)
    check_opts(opts)
    opts[:localize_queue] = true unless opts.has_key? :localized_queue
    @should_poll_sqs = opts[:should_poll_sqs]
    @buffer_size = opts[:buffer_size] || 100
    @localize_queue = opts[:localize_queue]
    @queue_name = generate_queue_name(opts)
    @request_count = 0
    initialize_sqs(opts)

    @waiting = []
    @waiting.taint
    self.taint
    @mutex = Mutex.new
    @in_buffer = []
    @out_buffer = []
    @deletion_queue = []
    @mock_length = 0 if SuperQueue.mocking?

    @compressor = Zlib::Deflate.new
    @decompressor = Zlib::Inflate.new

    @sqs_tracker = Thread.new { poll_sqs } if @should_poll_sqs
    @gc = Thread.new { collect_garbage }
  end

  def push(p)
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
    @mutex.synchronize {
      while true
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
    @mutex.synchronize {
      return sqs_length + @in_buffer.size + @out_buffer.size
    }
  end

  def empty?
    self.length == 0
  end

  def num_waiting
    @waiting.size
  end

  def clear
    begin
      self.pop(true)
    rescue ThreadError
      retry unless self.empty?
    end until self.empty?
  end

  def shutdown
    @sqs_tracker.terminate if @should_poll_sqs
    @mutex.synchronize { clear_in_buffer }
    @gc.terminate
    @mutex.synchronize { clear_deletion_queue }
  end

  def destroy
    @sqs_tracker.terminate if @should_poll_sqs
    @gc.terminate
    delete_queue
  end

  def sqs_requests
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

  def localized?
    !!@localize_queue
  end

  private

  #
  # Amazon SQS methods
  #
  def initialize_sqs(opts)
    create_sqs_connection(opts)
    create_sqs_queue(opts)
    if opts[:replace_existing_queue] && (sqs_length > 0)
      delete_queue
      puts "Waiting 60s to create new queue..."
      sleep 62 # You must wait 60s after deleting a q to create one with the same name
      create_sqs_queue(opts)
    end
  end

  def create_sqs_connection(opts)
    aws_options = {
      :aws_access_key_id => opts[:aws_access_key_id], 
      :aws_secret_access_key => opts[:aws_secret_access_key]
    }
    begin
      @sqs = Fog::AWS::SQS.new(aws_options)
    rescue Exception => e
      raise e
    end
  end

  def create_sqs_queue(opts)
    retries = 0
    begin
      @sqs_queue = new_sqs_queue(opts)
      check_for_queue_creation_success
    rescue RuntimeError => e
      retries += 1
      (retries >= 10) ? retry : raise(e)
    end
  end

  def new_sqs_queue(opts)
    @request_count += 1
    if opts[:visibility_timeout]
      @sqs.create_queue(queue_name, {"DefaultVisibilityTimeout" => opts[:visibility_timeout]})
    else
      @sqs.create_queue(queue_name)
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

  def send_message_to_queue
    p = @in_buffer.shift
    payload = is_a_link?(p) ? p : encode(p)
    retries = 0
    begin
      @request_count += 1
      @sqs.send_message(q_url, payload)
    rescue Excon::Errors::BadRequest => e
      retries += 1
      (retries >= 10) ? retry : raise(e)
    end
    @mock_length += 1 if SuperQueue.mocking?
  end

  def get_message_from_queue
    message = @sqs.receive_message(q_url)
    return nil if  message.body.nil? || message.body['Message'].first.nil?
    handle = message.body['Message'].first['ReceiptHandle']
    ser_obj = message.body['Message'].first['Body']
    return nil if ser_obj.nil? || ser_obj.empty?
    @mock_length -= 1 if SuperQueue.mocking?
    @request_count += 1
    return {:handle => handle, :payload => ser_obj} if is_a_link?(ser_obj)
    { :handle => handle, :payload => decode(ser_obj) }
  end

  def sqs_length
    return @mock_length if SuperQueue.mocking?
    body = @sqs.get_queue_attributes(q_url, "ApproximateNumberOfMessages").body
    @request_count += 1
    begin
      retval = 0
      if body
        attrs = body["Attributes"]
        if attrs
          retval = attrs["ApproximateNumberOfMessages"]
        end
      end
    end until !retval.nil?
    retval
  end

  def delete_queue
    @request_count += 1
    @sqs.delete_queue(q_url)
  end

  def clear_deletion_queue
    while !@deletion_queue.empty?
      @sqs.delete_message(q_url, @deletion_queue.shift)
      @request_count += 1
    end
  end

  #
  # Buffer-related methods
  #
  def fill_out_buffer_from_sqs_queue
    return false if sqs_length == 0
    @gc.wakeup if @gc.stop? # This is the best time to do GC, because there are no pops happening.
    nil_count = 0
    while (@out_buffer.size < @buffer_size) && (nil_count < 5) # If you get nil 5 times in a row, SQS is probably empty
      m = get_message_from_queue
      if m.nil?
        nil_count += 1
      else
        @out_buffer.push m
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

  def pop_out_buffer
    m = @out_buffer.shift
    @deletion_queue << m[:handle] if m[:handle]
    m[:payload]
  end

  def clear_in_buffer
    while !@in_buffer.empty? do
      send_message_to_queue
    end
  end

  def check_opts(opts)
    raise "Options can't be nil!" if opts.nil?
    raise "Minimun :buffer_size is 5." if opts[:buffer_size] && (opts[:buffer_size] < 5)
    raise "AWS credentials :aws_access_key_id and :aws_secret_access_key required!" unless opts[:aws_access_key_id] && opts[:aws_secret_access_key]
    raise "Visbility timeout must be an integer (in seconds)!" if opts[:visibility_timeout] && !opts[:visibility_timeout].is_a?(Integer)
  end

  #
  # Misc helper methods
  #
  def encode(p)
    text = Base64.encode64(Marshal.dump(p))
    retval = nil
    retries = 0
    begin
      retval = @compressor.deflate(text)
      retries += 1
    end until !(retval.nil? || retval.empty?) || (retries > 5)
    retval
  end

  def decode(ser_obj)
    text = nil
    retries = 0
    begin
      text = @decompressor.inflate(ser_obj)
      retries += 1
    end until !(text.nil? || text.empty?) || (retries > 5)
    Marshal.load(Base64.decode64(text))
  end

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
  end

  def generate_queue_name(opts)
    q_name = opts[:name] || random_name
    if opts[:namespace] && opts[:localize_queue]
      "#{@namespace}-#{Digest::MD5.hexdigest(local_ip)}-#{q_name}"
    elsif opts[:namespace]
      "#{@namespace}-#{q_name}"
    elsif opts[:localize_queue]
      "#{Digest::MD5.hexdigest(local_ip)}-#{q_name}"
    else
      q_name
    end
  end

  #
  # Virtul attributes and convenience methods
  #
  def q_url
    return @q_url if @q_url
    @q_url = @sqs_queue.body['QueueUrl']
    @q_url
  end

  def random_name
    o =  [('a'..'z'),('A'..'Z')].map{|i| i.to_a}.flatten
    (0...15).map{ o[rand(o.length)] }.join
  end

  def queue_name
    @queue_name
  end

  def local_ip
    orig, Socket.do_not_reverse_lookup = Socket.do_not_reverse_lookup, true  # turn off reverse DNS resolution temporarily
    return "127.0.0.1" if SuperQueue.mocking?
    UDPSocket.open do |s|
      s.connect '64.233.187 .99', 1
      s.addr.last
    end
  ensure
    Socket.do_not_reverse_lookup = orig
  end

  #
  # Maintence thread-related methods
  #
  def poll_sqs
    loop do
      @mutex.synchronize { fill_out_buffer_from_sqs_queue || fill_out_buffer_from_in_buffer } if @out_buffer.empty?
      @mutex.synchronize { clear_in_buffer } if !@in_buffer.empty? && (@in_buffer.size > @buffer_size)
      Thread.pass
    end
  end

  def collect_garbage
    loop do
      #This also needs a condition to clear the del queue if there are any handles where the invisibility is about to expire
      @mutex.synchronize { clear_deletion_queue } if !@deletion_queue.empty? && (@deletion_queue.size >= (@buffer_size / 2))
      sleep
    end
  end
end

require 'aws-sdk'
require 'base64'
require 'socket'
require 'digest/md5'
require 'zlib'

class SuperQueue

  def initialize(opts)
    AWS.eager_autoload! # for thread safety
    check_opts(opts)
    @should_poll_sqs = opts[:should_poll_sqs]
    @buffer_size = opts[:buffer_size] || 100
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
      :access_key_id => opts[:aws_access_key_id], 
      :secret_access_key => opts[:aws_secret_access_key]
    }
    begin
      @sqs = AWS::SQS.new(aws_options)
    rescue Exception => e
      raise e
    end
  end

  def create_sqs_queue(opts)
    retries = 0
    begin
      @sqs_queue = find_queue_by_name || new_sqs_queue(opts)
      check_for_queue_creation_success
    rescue RuntimeError => e
      retries += 1
      sleep 1
      (retries >= 20) ? retry : raise(e)
    end
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
        p = @in_buffer.shift
        batch << is_a_link?(p) ? p : encode(p)
      end
      batches << batch
    end
    batches.each do |b|
      @request_count += 1
      @sqs_queue.batch_send(b)
    end
  end

  def get_messages_from_queue(number_of_messages_to_receive)
    messages = []
    number_of_batches = number_of_messages_to_receive / 10
    number_of_batches += 1 if number_of_messages_to_receive % 10
    number_of_batches.times do
      m = @sqs_queue.receive_messages(:limit => 10)
      m.each do
        messages << is_a_link?(m.body) ? {:handle => message, :payload => m.body} : { :message => message, :payload => decode(m.body) }
      end
      @request_count += 1
    end
    messages
  end

  def sqs_length
    n = @sqs_queue.approximate_number_of_messages
    return n.is_a?(Integer) ? n : 0
  end

  def delete_queue
    @request_count += 1
    @sqs_queue.delete
  end

  def clear_deletion_queue
    while !@deletion_queue.empty?
      @sqs_queue.batch_delete(@deletion_queue[0..9])
      @request_count += 1
    end
  end

  #
  # Buffer-related methods
  #
  def fill_out_buffer_from_sqs_queue
    return false if sqs_length == 0
    @gc.wakeup if @gc.stop? # This is the best time to do GC, because there are no pops happening.
    while (@out_buffer.size < @buffer_size)
      messages = get_messages_from_queue(@buffer_size - @out_buffer.size)
      messages.each { |m| @out_buffer.push m }
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
    @deletion_queue << m[:message] if m[:message]
    m[:payload]
  end

  def clear_in_buffer
    while !@in_buffer.empty? do
      send_messages_to_queue
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
    text = Base64.urlsafe_encode64(Marshal.dump(p))
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
    Marshal.load(Base64.urlsafe_decode64(text))
  end

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
  end

  def generate_queue_name(opts)
    q_name = opts[:name] || random_name
    return opts[:namespace] ? "#{@namespace}-#{q_name}" : q_name
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
    o =  [('a'..'z'),('A'..'Z')].map{|i| i.to_a}.flatten
    (0...15).map{ o[rand(o.length)] }.join
  end

  def queue_name
    @queue_name
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

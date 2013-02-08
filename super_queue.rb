require 'fog'
require 'base64'
require 'socket'
require 'digest/md5'

class SuperQueue

  def initialize(opts)
    check_opts(opts)
    opts[:localize_queue] = true unless opts.has_key? :localized_queue
    @localize_queue = opts[:localize_queue]
    @queue_name = generate_queue_name(opts)
    initialize_sqs(opts)

    @waiting = []
    @waiting.taint
    self.taint
    @mutex = Mutex.new
    @in_buffer = SizedQueue.new(opts[:buffer_size])
    @out_buffer = SizedQueue.new(opts[:buffer_size])
    @deletion_queue = Queue.new

    @sqs_head_tracker = Thread.new { poll_sqs_head }
    @sqs_tail_tracker = Thread.new { poll_sqs_tail }
    @garbage_collector = Thread.new { collect_garbage }
  end

  def push(p)
    @mutex.synchronize {
      @in_buffer.push p
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
            return pop_out_buffer(non_block)
          else
            raise ThreadError, "queue empty" if non_block
            @waiting.push Thread.current
            @mutex.sleep
          end
        else
          return pop_out_buffer(non_block)
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

  def destroy
    @sqs_head_tracker.terminate
    while !@in_buffer.empty? do
      @sqs.delete_message(q_url, @in_buffer.pop)
    end  
    @sqs_tail_tracker.terminate
    @garbage_collector.terminate
    while !@deletion_queue.empty?
      @sqs.delete_message(q_url, @deletion_queue.pop)
    end
    delete_queue
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

  def check_opts(opts)
    raise "Parameter :buffer_size required!" unless opts[:buffer_size]
    raise "Minimun :buffer_size is 5." unless opts[:buffer_size] >= 5
    raise "AWS credentials :aws_access_key_id and :aws_secret_access_key required!" unless opts[:aws_access_key_id] && opts[:aws_secret_access_key]
    raise "Parameter :name required!" unless opts[:name]
    raise "Visbility timeout must be an integer (in seconds)!" if opts[:visibility_timeout] && !opts[:visibility_timeout].is_a?(Integer)
  end

  def initialize_sqs(opts)
    create_sqs_connection(opts)
    create_sqs_queue(opts)
    check_for_queue_creation_success
    @sqs.set_queue_attributes(q_url, "VisibilityTimeout", opts[:visibility_timeout]) if opts[:visibility_timeout]
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
    @sqs_queue = @sqs.create_queue(queue_name)
    if opts[:replace_existing_queue] && (sqs_length > 0)
      delete_queue
      puts "Waiting 60s to create new queue..."
      sleep 62 # You must wait 60s after deleting a q to create one with the same name
      if opts[:visibility_timeout]
        @sqs_queue = @sqs.create_queue(queue_name, {"DefaultVisibilityTimeout" => opts[:visibility_timeout]})
      else
        @sqs_queue = @sqs.create_queue(queue_name)
      end
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

  def send_message_to_queue(p)
    payload = is_a_link?(p) ? p : Base64.encode64(Marshal.dump(p))
    @sqs.send_message(q_url, payload)
  end

  def get_message_from_queue
    message = @sqs.receive_message(q_url)
    return nil if  message.body.nil? || message.body['Message'].first.nil?
    handle = message.body['Message'].first['ReceiptHandle']
    ser_obj = message.body['Message'].first['Body']
    return nil if ser_obj.nil? || ser_obj.empty?
    return {:handle => handle, :payload => ser_obj} if is_a_link?(ser_obj)
    { :handle => handle, :payload => Marshal.load(Base64.decode64(ser_obj)) }
  end

  def q_url
    return @q_url if @q_url
    @q_url = @sqs_queue.body['QueueUrl']
    @q_url
  end

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
  end

  def delete_queue
    @sqs.delete_queue(q_url)
  end

  def generate_queue_name(opts)
    if opts[:namespace] && opts[:localize_queue]
      "#{@namespace}-#{Digest::MD5.hexdigest(local_ip)}-#{opts[:name]}"
    elsif opts[:namespace]
      "#{@namespace}-#{opts[:name]}"
    elsif opts[:localize_queue]
      "#{Digest::MD5.hexdigest(local_ip)}-#{opts[:name]}"
    else
      opts[:name]
    end
  end

  def sqs_length
    body = @sqs.get_queue_attributes(q_url, "ApproximateNumberOfMessages").body
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

  def local_ip
    orig, Socket.do_not_reverse_lookup = Socket.do_not_reverse_lookup, true  # turn off reverse DNS resolution temporarily

    UDPSocket.open do |s|
      s.connect '64.233.187.99', 1
      s.addr.last
    end
  ensure
    Socket.do_not_reverse_lookup = orig
  end

  def poll_sqs_head
    loop { send_message_to_queue(@in_buffer.pop) }
  end

  def poll_sqs_tail
    loop do
      nil_count = 0
      while (@out_buffer.size < @out_buffer.max) && (nil_count < 5) # If you get nil 5 times in a row, SQS is probably empty
        m = get_message_from_queue
        if m.nil?
          nil_count += 1
        else
          @out_buffer.push m
          nil_count = 0
        end
      end unless sqs_length == 0
      sleep
    end
  end

  def collect_garbage
    loop { @sqs.delete_message(q_url, @deletion_queue.pop) }
  end

  def fill_out_buffer_from_sqs_queue
    @sqs_tail_tracker.wakeup if @sqs_tail_tracker.stop?
    count = 0
    while @out_buffer.empty? && count != 5
      sleep 1
      count += 1
    end
  end

  def fill_out_buffer_from_in_buffer
    while (@out_buffer.size < @out_buffer.max) && !@in_buffer.empty?
      @out_buffer.push @in_buffer.pop
    end
    !@out_buffer.empty?
  end

  def pop_out_buffer(non_block)
    m = @out_buffer.pop(non_block)
    @deletion_queue << m[:handle]
    m[:payload]
  end

  def queue_name
    @queue_name
  end
end

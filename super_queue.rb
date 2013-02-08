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

    @sqs_head_tracker = Thread.new { poll_sqs_head }
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
            return @out_buffer.pop(non_block)
          else
            raise ThreadError, "queue empty" if non_block
            @waiting.push Thread.current
            @mutex.sleep
          end
        else
          return @out_buffer.pop(non_block)
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
  end

  def initialize_sqs(opts)
    create_sqs_connection(opts)
    create_sqs_queue(opts)
    check_for_queue_creation_success
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
      @sqs_queue = @sqs.create_queue(queue_name)
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
    sqs.send_message(q_url, payload)
  end

  def get_message_from_queue
    message = sqs.receive_message(q_url)
    return nil if  message.body.nil? || message.body['Message'].first.nil?
    handle = message.body['Message'].first['ReceiptHandle']
    ser_obj = message.body['Message'].first['Body']
    return nil if ser_obj.nil? || ser_obj.empty?
    sqs.delete_message(q_url, handle)
    return ser_obj if is_a_link?(ser_obj)
    Marshal.load(Base64.decode64(ser_obj))
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

  def buffers_empty?
    @out_buffer.empty? && @in_buffer.empty?
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
    body = sqs.get_queue_attributes(q_url, "ApproximateNumberOfMessages").body
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

  def fill_out_buffer_from_sqs_queue
    count = 0
    while (@out_buffer.size < @out_buffer.max) && (count < sqs_length)
      m = get_message_from_queue
      @out_buffer.push m unless m.nil?
      count += 1
    end
    !@out_buffer.empty?
  end

  def fill_out_buffer_from_in_buffer
    while (@out_buffer.size < @out_buffer.max) && !@in_buffer.empty?
      @out_buffer.push @in_buffer.pop
    end
    !@out_buffer.empty?
  end
end

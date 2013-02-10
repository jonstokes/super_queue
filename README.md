SuperQueue
==========

SuperQueue is a thread-safe, SQS-backed queue structure for ruby that works just like a normal queue, except it's essentially infinite because it uses SQS on the back end.

To install, just "gem install SuperQueue".

To create a new SuperQueue, pass it an options hash. Some options are required,
and some are optional. When you're done with it, you should ideally call
"shutdown" on it to shut it down gracefully and preserve any data. Or,
if you want to delete the SQS queue and any lingering data, call
"destroy." 

Here's an short code example:

```ruby
opts = {
  :aws_access_key_id     => "12234abc",
  :aws_secret_access_key => "sdafsdl123212",
}
queue = SuperQueue.new(opts)
#=> SQS queue XML stuff...
queue << "foo"
#=> nil
queue.push "bar"
#=> nil
queue.enq "baz"
#=> nil
queue.length
#=> 3
queue.empty?
#=> false
queue.pop
#=> "foo"
queue.deq
#=> "bar"
queue.url
#=> "http://amazon-url/for-my-queue/alDkdFGjfglYUj"
queue.destroy
```

## Required options
* :aws_access_key_id
* :aws_secret_access_key

## Optional options (=> default)
* :name                   =>       #randomly generated name
* :buffer_size            => 100   #5 is the minimum
* :replace_existing_queue => false
* :namespace              => ""
* :localize_queue         => true
* :visibility_timeout     => 30    #in seconds. Max is 12 hours.

Let's go through these options one at a time.

### AWS credentials for fog
This should be obvious.

### Name
This is the name on AWS that you want to give the queue. It's
recommended to use this if you don't plan to destroy the queue via the
destroy method. Otherwise, SuperQueue generates a random name for it,
and you'll end up with these randomly named SQS queues on your AWS
account.

### Buffer size
For responsiveness and other reasons, SuperQueue uses two normal queues
as buffers at each end of the SQS queue. When you push to a SuperQueue,
your object goes into @in_buffer, where a polling thread that's blocking
on @in_buffer.pop will pop it and push it to SQS.

When you pop from a SuperQueue, it pops from @out_buffer. If @out_buffer
is empty, it wakes a thread that tries to fill the @out_buffer from either SQS or
@in_buffer. Note that there's no constantly-running polling thread that's trying
to fill @out_buffer from SQS, because that would run up the number of SQS
requests and hence the cost. As a general rule, SuperQueue tries to generate only one SQS
request per action (i.e. push, pop, size, etc.).

At any rate, you can tune the buffer size to trade off between memory
usage and performance (i.e larger buffer == more memory usage and more
performance).

### Replace existing queue
If there's already an SQS queue by this name, delete it, then re_create
this. Note that a delete_then_recreate on SQS takes a minimum of 60s.

### Namespace
If you want to namespace the queue on SQS, you can do that here.

### Localized_queues
In the application that I developed SuperQueue for (i.e. using with
Sidekiq and Anemone), I need the queues to act like local memory (but
with infinite size). So I don't want
the same code trying to generate the same queue names on different
machines. If you choose to localize the queues, then, it grabs your
local IP, creates an md5 hash of it, and uses that hash to namespace the
queue (in addition to any other namespacing you've done).

Localized queues are the default. Just set this to "false" if you want
to turn it off and have queues that are easily visible by simple
namespace + name combos from other machines.

### Visibility timeout 
Whenever a pop is executed against an empty out_buffer, SuperQueue wakes
a thread that tries to fill that out_buffer from SQS. Depending on what
you set the buffer_size attribute at, you could end up with quite a few
objects in the local out_buffer. If those objects aren't popped from
@out_buffer within the time window specified by visibility time_out
(maybe the system crashed and the object was destroyed, maybe the job
failed, and so on) then they'll become available again in the SQS queue.

The upside of this arrangement is that if the SuperQueue is somehow destroyed
with objects still in the out_buffer, those objects are not
lost and will become available again in SQS to be popped. The downside is that you must select both the visibility_timeout
and buffer_size attributes in tandem with each other.

If the out_buffer
is too large and the visibility_timeout is too small, objects in
out_buffer may timeout and you could lose them if the
SuperQueue dies. Or, an even bigger danger in this scenario is that
objects languishing in out_buffer will become
visible again in SQS and could be popped again from SQS, so you'd get dupes.

When in doubt, set the visibility_timeout for longer than you think
you'll need, because whenever an object is actually popped from @out_buffer it gets
deleted permanently from the SQS queue.

In a future version, I'll have the queue dynamically extend the
visibility_timeout of objects that are languishing in out_buffer, so
that this isn't so much of a worry.

For more on this attribute, see [this page on
Amazon](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html).

## Additional (non-Queue) methods
In addition to support for all the normal Queue methods, SuperQueue has
the following additional methods that reflect its SQS back end:

### #url
Returns the URL to the actual SQS queue.

### #localized?
Returns whether it's localized or not.

### #name
Returns the full name of the queue on SQS, with any namespace and
localization.

### #shutdown
Gracefully shuts down the queue by making sure all local buffers are
emptied and any garbage is collected. Call this if you plan to re-use
the queue and don't want to lose any data.

### #destroy
Terminates all the queue-related threads immediately and deletes the
queue from SQS. If you call this, then you'll need to wait 60 seconds
before re-creating a queue with the exact same name, namespace, and
localization.

## Caveats
For whatever reason, probably related to SQS and my buffering code, you
can't rely on SuperQueue to be strictly ordered. I've seen objects get
popped out of order a few times. This doesn't matter so much for my
applications, so I may not get around to troubleshooting this any time
soon. But if absolutely strict ordering matters for you, then SuperQueue isn't a
good choice.

## Mocking
If you're running tests with SuperQueue, you probably don't want to work
with a live SQS queue for time and cost reasons. So just call
SuperQueue.mock!, and SuperQueue will use fog's mock library to simulate
SQS calls. All functions should still work in mocking mode.

## Misc Notes
I created this as a drop-in solution for the anemone gem. The idea is to
swap out anemone's link and page queues with SuperQueues, and solve the
infinite memory problem that plagues the gem. I've yet to test this with
a live crawl, but that'll happen in the next week or so.

I bring this up, because you may notice some peculiarities in the code
that arise from its specific intended use. For instance, I check to see
if an object is a URL string before pushing it, and if it is, I push it
as a string; all other objects get marshalled and then base64-encoded
before being pushed.

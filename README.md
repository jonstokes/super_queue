SuperQueue
==========

SuperQueue is a thread-safe, SQS-backed queue structure for ruby that works just like a normal queue, except it's essentially infinite because it uses SQS on the back end.

Note: I haven't done any multithreaded testing on this, so in saying
it's "thread safe", what I mean is that I hope it is. I'll be testing it
over the next few days. If anyone else wants to try it out and tell me
their results, I'd love to hear from you. Also, I haven't put this into
production yet... as I said, I just wrote it and am starting to test it.

To create a new one, pass it an options hash. Some options are required,
and some are optional.

# Required options
* :name
* :buffer_size (minimum of 5)
* :aws_access_key_id
* :aws_secret_access_key

# Optional options (=> default)
* :replace_existing_queue => false
* :namespace => ""
* :localize_queue => true

Let's go through these options one at a time.

## Name
This is the name on AWS that you want to give the queue. 

## Buffer size
For responsiveness and other reasons, SuperQueue uses two normal queues
as buffers at each end of the SQS queue. When you push to a SuperQueue,
your object goes into @in_buffer, where a polling thread that's blocking
on @in_buffer(pop) will pop it and push it to SQS.

When you pop from a SuperQueue, it pops from @out_buffer. If @out_buffer
is empty, it tries to fill the @out_buffer from either SQS or
@in_buffer. Note that there's no polling thread that's constantly trying
to fill @out_buffer, because that would run up the number of SQS
requests and hence the cost. SuperQueue tries to generate only one SQS
request per action (i.e. push, pop, size).

At any rate, you can tune the buffer size to trade off between memory
usage and performance (i.e larger buffer == more memory usage and more
performance).

Note that with this design, pops from an empty @out_buffer can take a
long time, depending on the buffer size. Eventually I'll try to optimize
this a bit more.

## AWS credentials for fog
This should be obvious.

## Replace existing queue
If there's already an SQS queue by this name, delete it, then re_create
this. Note that a delete_then_recreate on SQS takes a minimum of 60s.

## Namespace
If you want to namespace the queue on SQS, you can do that here.

## Localized_queues
For the application that I developed SuperQueue for (i.e. using with
Sidekiq), I need the queues to act like local memory. So I don't want
the same code trying to generate the same queue names on different
machines. If you choose to localize the queues, then, it grabs your
local IP, creates an md5 hash of it, and uses that hash to namespace the
queue (in addition to any other namespacing you've done).

Localized queues are the default. Just set this to "false" if you want
to turn it off.

# Misc Notes
I created this as a drop-in solution for the anemone gem. The idea is to
swap out anemone's link and page queues with SuperQueues, and solve the
infinite memory problem that plagues the gem. I've yet to test this with
a live crawl, but that'll happen in the next week or so.

I bring this up, because you may notice some peculiarities in the code
that arise from its specific intended use. For instance, I check to see
if an object is a URL string before pushing it, and if it is, I push it
as a string; all other objects get marshalled and then base64-encoded
before being pushed.

# NATS.jl

Unofficial client to NATS in Julia

## Supports

  - publish-subscribe
  - request-reply
  - connect to multiple nats servers in a cluster
  - channel-based non-blocking interaction

## Usage

### Iterate subscription

```julia
using NATS

nc = NATS.connect()

subject = "subject"

sub = subscribe(nc, subject)
sub_task = @async for msg in sub
    println(msg)
end

publish(nc, subject, "hi!")

drain(nc)
```

### Subscription callback

```julia
using NATS

nc = NATS.connect()

subject = "subject"

sub = subscribe(nc, subject, println)
publish(nc, subject, "hi!")
publish(nc, subject, "hello!")
unsubscribe(nc, sub)

publish(nc, subject, "wow!") # This will not arrive

drain(nc)
```

### Request Reply

```julia
using NATS

nc = NATS.connect()
subject = "hello"

echo_sub = subscribe(nc, subject, msg -> publish(nc, msg.reply_to, msg.payload))

msg = request(nc, subject, "hey")

drain(nc)
```

### Consume subscription in multiple threads

```julia
using NATS

nc = NATS.connect()
subject = "hello"
sub = subscribe(nc, subject)
t = @async Threads.foreach(x -> println("ID: $(Threads.threadid()) - $x"), channel(sub))

for i = 1:100
    publish(nc, subject, "m: $i")
end

drain(nc)
```

### Handle requests in multiple threads

```julia
using NATS

nc = NATS.connect()
subject = "hello"
sub = subscribe(nc, subject)

function handle_request(msg)
    println("Thread: $(Threads.threadid()): arrived: $msg")
    return publish(nc, msg.reply_to, msg.payload)
end

t = @async Threads.foreach(handle_request, channel(sub))

# Launch requests in parallel to see that it uses multiple threads in the consumer
results = asyncmap(x -> request(nc, subject, "hey"), 1:100)

drain(nc)
```

## Missing features

  - support for messages with headers
  - handle reconnections
  - handle info message from servers for cluster reconfiguration
  - JetStream

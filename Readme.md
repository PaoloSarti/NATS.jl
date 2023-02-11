# NATS.jl

Unofficial client to NATS.

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

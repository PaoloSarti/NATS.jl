module NATS

export publish,
    subscribe, unsubscribe, request, drain, string_payload, channel, ConnectOptions

import Base
import Random

include("./messages.jl")
include("./connection.jl")

"""
struct that represents a subscription to a subject, it will keep the Channel of relevant messages.
Get this struct by using the subscribe function.
"""
mutable struct Subscription
    sid::String
    subject::String
    channel::Channel{MSG}
    task::Union{Task, Nothing}
    max_msgs::Float64

    Subscription(sid::String, subject::String) =
        new(sid, subject, Channel{MSG}(Inf), nothing, Inf)
    Subscription(sid::Int64, subject::String) = Subscription("$(sid)", subject)
end

Base.iterate(s::Subscription, state = nothing) = iterate(s.channel, state)

Base.IteratorSize(::Type{Subscription}) = Base.SizeUnknown()

"""
Return the Channel of Messages from the subscription
"""
channel(sub::Subscription) = sub.channel

"""
This is the main object of the module, that represents a set of connections to a NATS cluster.
It keeps the commands and messages in Channels and uses tasks to dispatch the messages from the single NATSConnections to the subscribers.
"""
mutable struct NATSClient
    commands::Channel{Union{PUB, SUB, UNSUB}}
    messages::Channel{MSG}
    connections::Vector{NATSConnection}
    subscriptions::Dict{String, Subscription}
    messages_to_subscribers_task::Union{Task, Nothing}
    sid::Int64
    subscriptions_lock::ReentrantLock

    NATSClient(
        commands::Channel{Union{PUB, SUB, UNSUB}},
        messages::Channel{MSG},
        connections::Vector{NATSConnection},
    ) = new(
        commands,
        messages,
        connections,
        Dict{String, Subscription}(),
        nothing,
        0,
        ReentrantLock(),
    )
end

function Base.show(io::IO, nc::NATSClient)
    uris = [c.uri for c in nc.connections]
    return write(io, "NATSClient($(uris))")
end

"""
Connect to a NATS cluster, accepts a vector of uris, and optionally a ConnectOptions
struct, returns a NATSClient
"""
function connect(
    uris::Vector{String} = ["nats://localhost:4222"];
    options::ConnectOptions = ConnectOptions(),
)
    commands = Channel{Union{PUB, SUB, UNSUB}}(Inf)
    messages = Channel{MSG}(Inf)
    connections =
        asyncmap(uri -> connect_to_server(messages, commands, uri, options = options), uris)

    nc = NATSClient(commands, messages, connections)

    nc.messages_to_subscribers_task = errormonitor(@async messages_to_subscribers(nc))

    return nc
end

function messages_to_subscribers(nc::NATSClient)
    for msg in nc.messages
        try
            lock(nc.subscriptions_lock)
            if msg.sid in keys(nc.subscriptions)
                subscription = nc.subscriptions[msg.sid]
                if subscription.max_msgs > 0
                    @debug "Push to subscriber channel"
                    push!(subscription.channel, msg)
                    subscription.max_msgs -= 1
                    if subscription.max_msgs == 0
                        @debug "Subscription drained! Closing it..."
                        close(subscription.channel)
                        delete!(nc.subscriptions, msg.sid)
                    end
                else
                    @info "Arrived message on drained subscription, ignoring it!"
                end
            else
                @info "Received message for not found subscription id $(msg.sid), ignoring it"
            end
        finally
            unlock(nc.subscriptions_lock)
        end
    end
end

"""
Publish a message to a subject, the payload is raw bytes. Optionally specify a 
reply_to subject to implement request/response
"""
function publish(
    nc::NATSClient,
    subject::String,
    payload::Vector{UInt8},
    reply_to::Union{String, Nothing} = nothing,
)
    push!(nc.commands, PUB(subject, reply_to, payload))
    return
end

"""
Publish a string message to a subject
"""
publish(nc::NATSClient, subject::String, payload::String) =
    publish(nc, subject, Vector{UInt8}(payload))

"""
Subscribe to a subject, optionally specify a queue_group. Returns a Subscription
"""
function subscribe(
    nc::NATSClient,
    subject::String;
    queue_group::Union{String, Nothing} = nothing,
)
    nc.sid += 1
    sid = "$(nc.sid)"
    subscription = Subscription(sid, subject)
    try
        lock(nc.subscriptions_lock)
        nc.subscriptions[sid] = subscription
    finally
        unlock(nc.subscriptions_lock)
    end
    push!(nc.commands, SUB(subject, queue_group, sid))
    return subscription
end

"""
Subscribe to a subject, and directly register a callback on the subscription 
that will consume the subscription message in a dedicated task.

For more control on the execution, don't pass the callback and explicitely consume the messages.

Returns a Subscription
"""
function subscribe(
    nc::NATSClient,
    subject::String,
    cb::Function;
    queue_group::Union{String, Nothing} = nothing,
)
    subscription = subscribe(nc, subject, queue_group = queue_group)
    task = @async for message in subscription.channel
        cb(message)
    end
    subscription.task = errormonitor(task)
    return subscription
end

"""
Request a Response by publishing a message to a subject and waiting until the timeout for the response to arrive.


This method will also unsubscribe to the reply_to subscription created internally.


Returns the Response or throws an error in case of timeout.
"""
function request(
    nc::NATSClient,
    subject::String,
    payload::Vector{UInt8};
    timeout = 10,
    pollint = 0.1,
)
    reply_to = Random.randstring(22)
    sub = subscribe(nc, reply_to)
    publish(nc, subject, payload, reply_to)
    wait_result = timedwait(() -> isready(sub.channel), timeout; pollint = pollint)
    if wait_result == :timed_out
        unsubscribe(nc, sub)
        error("Timed out!")
    end
    response = take!(sub.channel)
    unsubscribe(nc, sub)
    return response
end

"""
Request a Response by publishing a string message to a subject and waiting until the timeout for the response to arrive.

Returns the response Message or throws in case of timeout
"""
request(nc::NATSClient, subject::String, payload::String; timeout = 10, pollint = 0.1) =
    request(nc, subject, Vector{UInt8}(payload); timeout = timeout, pollint = pollint)

"""
Unsubscribe from a subscription. If max_msgs is specified, the subscription won't be closed immediately but only after that number of messages.
"""
function unsubscribe(nc::NATSClient, subscription::Subscription; max_msgs::Int64 = 0)
    push!(nc.commands, UNSUB(subscription.sid, max_msgs))
    if max_msgs == 0
        try
            lock(nc.subscriptions_lock)
            delete!(nc.subscriptions, subscription.sid)
        finally
            unlock(nc.subscriptions_lock)
        end
        close(subscription.channel)
    else
        subscription.max_msgs = max_msgs
    end
    return nothing
end

"""
Drain the connection by unsubscribing to all the subscriptions, and closing the connections.
"""
function drain(nc::NATSClient; timeout = 10)
    subs = collect(values(nc.subscriptions))
    for sub in subs
        unsubscribe(nc, sub)
    end
    timedwait(() -> !isready(nc.commands), timeout)
    asyncmap(conn -> drain(conn, timeout = timeout), nc.connections)
    close(nc.commands)
    return nothing
end

end # module NATS

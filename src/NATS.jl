module NATS

export publish,
    subscribe, unsubscribe, request, drain, string_payload, channel, ConnectOptions

import Base
import Random

include("./messages.jl")
include("./connection.jl")

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

channel(sub::Subscription) = sub.channel

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

function publish(
    nc::NATSClient,
    subject::String,
    payload::Vector{UInt8},
    reply_to::Union{String, Nothing} = nothing,
)
    push!(nc.commands, PUB(subject, reply_to, payload))
    return
end

publish(nc::NATSClient, subject::String, payload::String) =
    publish(nc, subject, Vector{UInt8}(payload))

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

function request(nc::NATSClient, subject::String, payload::Vector{UInt8})
    reply_to = Random.randstring(22)
    sub = subscribe(nc, reply_to)
    publish(nc, subject, payload, reply_to)
    response = take!(sub.channel)
    unsubscribe(nc, sub)
    return response
end

request(nc::NATSClient, subject::String, payload::String) =
    request(nc, subject, Vector{UInt8}(payload))

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

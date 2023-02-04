module NATS

export publish, subscribe, unsubscribe, request, drain, CONNECT

import Base
import Random
import Sockets
using URIs: URI
import JSON3 as JSON
import StructTypes

include("./messages.jl")

const CR_LF = "\r\n"
const MSG_RE = r"MSG\s+(?<subject>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)\s+)?(?<bytes>\d+)"
const ERR_RE = r"-ERR\s+(?<error>.*)"
const INFO_RE = r"INFO\s+(?<error>.*)"

struct NATSInfo
    server_id::String
    server_name::String
    version::String
    proto::Int64
    git_commit::String
    go::String
    host::String
    port::Int64
    headers::Bool
    max_payload::Int64
    client_id::Int64
    client_ip::String
    cluster::String
end

StructTypes.StructType(::Type{NATSInfo}) = StructTypes.Struct()

mutable struct Subscription
    sid::String
    subject::String
    channel::Channel{MSG}
    task::Union{Task, Nothing}
    max_msgs::Float64

    Subscription(sid::String, subject::String) = new(sid, subject, Channel{MSG}(Inf), nothing, Inf)
    Subscription(sid::Int64, subject::String) = Subscription("$(sid)", subject)
end

function Base.iterate(s::Subscription, state=nothing)
    if s.max_msgs > 0
        s.max_msgs -= 1
        return iterate(s.channel, state)
    else
        close(s.channel)
        return nothing
    end
end

mutable struct NATSClient
    socket::Sockets.TCPSocket
    info::NATSInfo
    messages::Channel{MSG}
    commands::Channel{Union{CONNECT, PONG, PUB, SUB, UNSUB}}
    subscriptions::Dict{String, Subscription}
    read_loop_task::Union{Task, Nothing}
    write_loop_task::Union{Task, Nothing}
    sid::Int64
    subscriptions_lock::ReentrantLock

    NATSClient(socket::Sockets.TCPSocket, info::NATSInfo) = new(socket, info, Channel{MSG}(Inf), Channel{Union{CONNECT, PONG, PUB, SUB, UNSUB}}(Inf), Dict{String, Vector{Channel{MSG}}}(), nothing, nothing, 0, ReentrantLock())
end

function connect(uri::String = "nats://localhost:4222", connectOptions::ConnectOptions = ConnectOptions())
    u = URI(uri)
    socket = Sockets.connect(u.host, parse(Int64, u.port))
    data = readuntil(socket, CR_LF)
    cmd, payload = split(data, " ", limit=2)
    
    if cmd != "INFO"
        error("Received wrong command from NATS!")
    end

    info = JSON.read(payload, NATSInfo)

    nc = NATSClient(socket, info)

    push!(nc.commands, CONNECT(connectOptions))

    nc.read_loop_task = @async read_loop(nc)
    nc.write_loop_task = @async write_loop(nc)

    return nc
end

function read_loop(nc::NATSClient)
    read_payload = false
    payload_bytes = 0
    msg_subject = ""
    msg_sid = ""
    msg_reply_to = ""

    while true
        if !isopen(nc.socket)
            @info "Socket closed"
            break
        end
        if !read_payload
            data = readuntil(nc.socket, b"\r\n")

            if data == b"PING"
                @debug "Pinged"
                push!(nc.commands, pong)
                continue
            elseif data == b"+OK"
                @debug "OK"
                continue
            end

            s = String(data)
            @debug s
            m = match(MSG_RE, s)
            if m !== nothing
                msg_subject = m[:subject]
                msg_sid = m[:sid]
                payload_bytes = parse(Int64, m[:bytes])
                msg_reply_to = m[:reply_to]
                read_payload = true
                @debug "Message subject: $msg_subject"
                @debug "MSG sid: $msg_sid"
                @debug "Message payload bytes: $payload_bytes"
                @debug "Message reply_to: $msg_reply_to"
                continue
            end
            
            m_err = match(ERR_RE, s)
            if m_err !== nothing
                error = m[:error]
                @error error
                continue
            end
        else
            @debug "Read payload"
            data = read(nc.socket, payload_bytes)
            @debug "Read $payload_bytes"
            readuntil(nc.socket, b"\r\n")
            @debug "Payload read"
            read_payload = false
            try
                lock(nc.subscriptions_lock)
                if msg_sid in keys(nc.subscriptions)
                    subscription = nc.subscriptions[msg_sid]
                    if subscription.max_msgs > 0
                        @debug "Push to subscriber channel"
                        push!(subscription.channel, MSG(msg_subject, msg_sid, msg_reply_to, data))
                        subscription.max_msgs -= 1
                    else
                        @debug "Subscription drained! Closing it..."
                        close(subscription.channel)
                        delete!(nc.subscriptions, msg_sid)
                    end
                end
            finally
                unlock(nc.subscriptions_lock)
            end
        end
    end
end

function send(nc::NATSClient, conn::CONNECT)
    @debug "Writing CONNECT"
    write(nc.socket, b"CONNECT ")
    write(nc.socket, JSON.write(conn.options))
    write(nc.socket, CR_LF)
    @debug "Sent CONNECT"
end

function send(nc::NATSClient, ::PONG)
    @debug "Writing PONG"
    write(nc.socket, "PONG\r\n")
    @debug "Sent PONG"
end

function send(nc::NATSClient, cmd::SUB)
    @debug "Writing SUB"
    write(nc.socket, "SUB $(cmd.subject)")
    if cmd.queue_group !== nothing
        write(nc.socket, " $(cmd.queue_group)")
    end
    write(nc.socket, " $(cmd.sid)\r\n")
    @debug "Sent SUB"
end

function send(nc::NATSClient, cmd::PUB)
    @debug "Writing PUB"
    write(nc.socket, "PUB $(cmd.subject)")
    if cmd.reply_to !== nothing
        write(nc.socket, " $(cmd.reply_to)")
    end
    write(nc.socket, " $(length(cmd.payload))\r\n")
    write(nc.socket, cmd.payload)
    write(nc.socket, b"\r\n")
    @debug "Sent PUB"
end

function send(nc::NATSClient, cmd::UNSUB)
    @debug "Writing UNSUB"
    write(nc.socket, "UNSUB $(cmd.sid)")
    if cmd.max_msgs > 0
        write(nc.socket, " $(cmd.max_msgs)")
    end
    write(nc.socket, "\r\n")
    @debug "Sent UNSUB"
end

function write_loop(nc::NATSClient)
    for command in nc.commands
        send(nc, command)
    end
end

function publish(nc::NATSClient, subject::String, payload::Vector{UInt8}, reply_to::Union{String, Nothing} = nothing)
    push!(nc.commands, PUB(subject, reply_to, payload))
    return
end

publish(nc::NATSClient, subject::String, payload::String) = publish(nc, subject, Vector{UInt8}(payload))

function subscribe(nc::NATSClient, subject::String; queue_group::Union{String, Nothing} = nothing)
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

function subscribe(nc::NATSClient, subject::String, cb::Function; queue_group::Union{String, Nothing} = nothing)
    subscription = subscribe(nc, subject, queue_group=queue_group)
    task = @async for message in subscription.channel
        cb(message)
    end
    subscription.task = task
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

request(nc::NATSClient, subject::String, payload::String) = request(nc, subject, Vector{UInt8}(payload))

function unsubscribe(nc::NATSClient, subscription::Subscription, max_msgs::Int64 = 0)
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

function drain(nc::NATSClient)
    close(nc.commands)
    close(nc.socket)
end

end # module NATS

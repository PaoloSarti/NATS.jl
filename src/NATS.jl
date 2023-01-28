module NATS

export publish, subscribe, drain

import Sockets
using URIs: URI
import JSON3 as JSON
import StructTypes

include("./messages.jl")

const CR_LF = "\r\n"
const MSG_RE = r"MSG\s+(?<subject>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)\s+)?(?<bytes>\d+)"
const ERR_RE = r"-ERR\s+(?<error>.*)"

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

    Subscription(sid::String, subject::String) = new(sid, subject, Channel{MSG}(Inf), nothing)
    Subscription(sid::Int64, subject::String) = Subscription("$(sid)", subject)
end

mutable struct NATSClient
    socket::Sockets.TCPSocket
    info::NATSInfo
    messages::Channel{MSG}
    commands::Channel{Union{PONG, PUB, SUB, UNSUB}}
    subscriptions::Dict{String, Vector{Subscription}}
    read_loop_task::Union{Task, Nothing}
    write_loop_task::Union{Task, Nothing}
    sid::Int64

    NATSClient(socket::Sockets.TCPSocket, info::NATSInfo) = new(socket, info, Channel{MSG}(Inf), Channel{Union{PONG, PUB, SUB, UNSUB}}(Inf), Dict{String, Vector{Channel{MSG}}}(), nothing, nothing, 0)
end

function connect(uri::String = "nats://localhost:4222")
    u = URI(uri)
    socket = Sockets.connect(u.host, parse(Int64, u.port))
    data = readuntil(socket, CR_LF)
    cmd, payload = split(data, " ", limit=2)
    
    if cmd != "INFO"
        error("Received wrong command from NATS!")
    end

    info = JSON.read(payload, NATSInfo)

    nc = NATSClient(socket, info)

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
                read_payload = true
                @debug "Message subject: $msg_subject"
                @debug "MSG sid: $msg_sid"
                @debug "Message payload bytes: $payload_bytes"
                continue
            end
            
            m_err = match(ERR_RE, s)
            if m_err !== nothing
                error = m[:error]
                @error error
            end
        else
            @debug "Read payload"
            data = read(nc.socket, payload_bytes)
            @debug "Read $payload_bytes"
            readuntil(nc.socket, b"\r\n")
            @debug "Payload read"
            read_payload = false
            if msg_subject in keys(nc.subscriptions)
                for subscription in nc.subscriptions[msg_subject]
                    @debug "Push to subscriber channel"
                    push!(subscription.channel, MSG(msg_subject, msg_sid, msg_reply_to, data))
                end
            end
        end
    end
end

function send(nc::NATSClient, ::PONG)
    @debug "Writing PONG"
    write(nc.socket, "PONG\r\n")
    @debug "Sent Pong"
end

function send(nc::NATSClient, cmd::SUB)
    @debug "Writing SUB"
    write(nc.socket, "SUB $(cmd.subject) $(cmd.queue_group)")
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

function publish(nc::NATSClient, subject::String, payload::Vector{UInt8})
    push!(nc.commands, PUB(subject, "", payload))
    return
end

publish(nc::NATSClient, subject::String, payload::String) = publish(nc, subject, Vector{UInt8}(payload))

function subscribe(nc::NATSClient, subject::String, queue_group::Union{String, Nothing} = nothing)
    nc.sid += 1
    subscription = Subscription(nc.sid, subject)
    if !(subject in keys(nc.subscriptions))
        nc.subscriptions[subject] = [subscription]
    else
        push!(nc.subscriptions[subject], subscription)
    end
    push!(nc.commands, SUB(subject, queue_group, "$(nc.sid)"))
    return subscription
end

function subscribe(nc::NATSClient, subject::String, cb::Function)
    subscription = subscribe(nc, subject)
    task = @async for message in subscription.channel
        cb(message)
    end
    subscription.task = task
    return subscription
end

function unsubscribe(nc::NATSClient, subscription::Subscription, max_msgs::Int64 = 0)
    subs = nc.subscriptions[subscription.subject]
    
    found_index = 0
    for (i, sub) in enumerate(subs)
        if sub.sid == subscription.sid
            found_index = i 
            break
        end
    end
    if found_index > 0
        push!(nc.commands, UNSUB(subscription.sid, max_msgs))
        #=
        deleteat!(subs, found_index)
        if length(subs) == 0
            delete!(nc.subscriptions, subscription.subject)
        end
        =#
    end
    return nothing
end

function drain(nc::NATSClient)
    close(nc.commands)
    close(nc.socket)
end

end # module NATS

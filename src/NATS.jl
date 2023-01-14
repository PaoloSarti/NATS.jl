module NATS

using Sockets: connect, TCPSocket
using URIs: URI
import JSON3 as JSON
import StructTypes

const CR_LF = "\r\n"
const MSG_RE = r"MSG\s+(?<subject>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)\s+)?(?<bytes>\d+)"

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

struct SUB
    subject::String
    queue_group::Union{String, Nothing}
    sid::String
end

struct MSG
    subject::String
    sid::String
    reply_to::Union{String, Nothing}
    payload::Vector{UInt8}
end

struct PUB
    subject::String
    reply_to::Union{String, Nothing}
    payload::Vector{UInt8}
end

struct PONG end

const pong = PONG()

mutable struct NATSClient
    socket::TCPSocket
    info::NATSInfo
    messages::Channel{MSG}
    commands::Channel{Union{PONG, PUB, SUB}}
    subscriptions::Dict{String, Vector{Channel{MSG}}}
    read_loop_task::Union{Task, Nothing}
    write_loop_task::Union{Task, Nothing}
end

function connect_to_nats(uri::String)
    u = URI(uri)
    socket = connect(u.host, parse(Int64, u.port))
    data = readuntil(socket, CR_LF)
    cmd, payload = split(data, " ", limit=2)
    
    if cmd != "INFO"
        error("Received wrong command from NATS!")
    end

    info = JSON.read(payload, NATSInfo)

    nc = NATSClient(socket, info, Channel{MSG}(Inf), Channel{Union{PONG, PUB, SUB}}(Inf), Dict{String, Vector{Channel{MSG}}}(), nothing, nothing)

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
            else
                @warn "Message not yet supported: $s"
            end
        else
            @debug "Read payload"
            data = read(nc.socket, payload_bytes)
            @debug "Read $payload_bytes"
            readuntil(nc.socket, b"\r\n")
            @debug "Payload read"
            read_payload = false
            if msg_subject in keys(nc.subscriptions)
                for channel in nc.subscriptions[msg_subject]
                    @debug "Push to subscriber channel"
                    push!(channel, MSG(msg_subject, msg_sid, msg_reply_to, data))
                end
            end
        end
    end
end

function send_to_nats(nc::NATSClient, pong::PONG)
    @debug "Writing PONG"
    write(nc.socket, "PONG\r\n")
    @debug "Sent Pong"
end

function send_to_nats(nc::NATSClient, cmd::SUB)
    @debug "Writing SUB"
    write(nc.socket, "SUB $(cmd.subject) $(cmd.queue_group) $(cmd.sid)\r\n")
    @debug "Sent SUB"
end

function send_to_nats(nc::NATSClient, cmd::PUB)
    @debug "Writing PUB"
    write(nc.socket, "PUB $(cmd.subject) $(cmd.reply_to) $(length(cmd.payload))\r\n")
    write(nc.socket, cmd.payload)
    write(nc.socket, b"\r\n")
    @debug "Sent PUB"
end

function write_loop(nc::NATSClient)
    for command in nc.commands
        send_to_nats(nc, command)
    end
end

function publish(nc::NATSClient, subject::String, payload::Vector{UInt8})
    push!(nc.commands, PUB(subject, "", payload))
end

function subscribe(nc::NATSClient, subject::String, sid::String)
    channel = Channel{MSG}(Inf)
    if !(subject in keys(nc.subscriptions))
        nc.subscriptions[subject] = [channel]
    else
        push!(nc.subscriptions[subject], channel)
    end
    push!(nc.commands, SUB(subject, "", sid))
    return channel
end

function subscribe(nc::NATSClient, subject::String, sid::String, cb::Function)
    channel = subscribe(nc, subject, sid)
    return @async for message in channel
        cb(message)
    end
end

function drain(nc::NATSClient)
    close(nc.commands)
    close(nc.socket)
end

end # module NATS

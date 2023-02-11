
import Sockets
using URIs: URI
import JSON3 as JSON
import StructTypes

const CR_LF = "\r\n"
const MSG_RE =
    r"MSG\s+(?<subject>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)\s+)?(?<bytes>\d+)"
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
    auth_required::Union{Bool, Nothing}
    max_payload::Int64
    client_id::Int64
    client_ip::String
    cluster::Union{String, Nothing}
end

StructTypes.StructType(::Type{NATSInfo}) = StructTypes.Struct()

mutable struct NATSConnection
    uri::String
    options::ConnectOptions
    socket::Sockets.TCPSocket
    info::NATSInfo
    commands::Channel{Union{CONNECT, PONG, PUB, SUB, UNSUB}}
    client_commands::Channel{Union{PUB, SUB, UNSUB}}
    messages::Channel{MSG}
    read_loop_task::Union{Task, Nothing}
    write_loop_task::Union{Task, Nothing}
    dispatch_commands_task::Union{Task, Nothing}
    reconnections::Int64
    status::Symbol

    NATSConnection(
        uri::String,
        options::ConnectOptions,
        socket::Sockets.TCPSocket,
        info::NATSInfo,
        messages::Channel{MSG},
        client_commands::Channel{Union{PUB, SUB, UNSUB}},
    ) = new(
        uri,
        options,
        socket,
        info,
        Channel{Union{CONNECT, PONG, PUB, SUB, UNSUB}}(Inf),
        client_commands,
        messages,
        nothing,
        nothing,
        nothing,
        0,
        :connected,
    )
end

function Base.show(io::IO, nc::NATSConnection)
    return write(io, "NATSConnection(\"$(nc.uri)\", $(nc.status)\")")
end

function connect_to_server(
    messages::Channel{MSG},
    client_commands::Channel{Union{PUB, SUB, UNSUB}},
    uri::String = "nats://localhost:4222";
    options::ConnectOptions = ConnectOptions(),
)
    u = URI(uri)
    socket = Sockets.connect(u.host, parse(Int64, u.port))
    data = readuntil(socket, CR_LF)
    cmd, payload = split(data, " ", limit = 2)

    if cmd != "INFO"
        error("Received wrong command from NATS!")
    end

    info = JSON.read(payload, NATSInfo)

    nc = NATSConnection(uri, options, socket, info, messages, client_commands)

    push!(nc.commands, CONNECT(options))

    nc.read_loop_task = errormonitor(@async read_loop(nc))
    nc.write_loop_task = errormonitor(@async write_loop(nc))
    nc.dispatch_commands_task = errormonitor(@async dispatch_commands(nc))

    return nc
end

function read_loop(nc::NATSConnection)
    read_payload = false
    payload_bytes = 0
    msg_subject = ""
    msg_sid = ""
    msg_reply_to = ""

    while true
        if !isopen(nc.socket)
            nc.status = :disconnected
            @debug "Socket closed"
            break
        end
        if !read_payload
            data = readuntil(nc.socket, b"\r\n")

            if length(data) == 0
                @debug "Read empty data from socket, close it"
                close(nc.socket)
                nc.status = :disconnected
            end

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
                error = m_err[:error]
                @error error
                if error == "'Authorization Violation'"
                    break
                end
                continue
            end
        else
            @debug "Read payload"
            data = read(nc.socket, payload_bytes)
            @debug "Read $payload_bytes"
            readuntil(nc.socket, b"\r\n")
            @debug "Payload read"
            read_payload = false
            push!(nc.messages, MSG(msg_subject, msg_sid, msg_reply_to, data))
        end
        @debug "Read loop cycle done"
    end
end

function send(nc::NATSConnection, conn::CONNECT)
    @debug "Writing CONNECT"
    write(nc.socket, b"CONNECT ")
    write(nc.socket, JSON.write(conn.options))
    write(nc.socket, CR_LF)
    @debug "Sent CONNECT"
end

function send(nc::NATSConnection, ::PONG)
    @debug "Writing PONG"
    write(nc.socket, "PONG\r\n")
    @debug "Sent PONG"
end

function send(nc::NATSConnection, cmd::SUB)
    @debug "Writing SUB"
    write(nc.socket, "SUB $(cmd.subject)")
    if cmd.queue_group !== nothing
        write(nc.socket, " $(cmd.queue_group)")
    end
    write(nc.socket, " $(cmd.sid)\r\n")
    @debug "Sent SUB"
end

function send(nc::NATSConnection, cmd::PUB)
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

function send(nc::NATSConnection, cmd::UNSUB)
    @debug "Writing UNSUB"
    write(nc.socket, "UNSUB $(cmd.sid)")
    if cmd.max_msgs > 0
        write(nc.socket, " $(cmd.max_msgs)")
    end
    write(nc.socket, "\r\n")
    @debug "Sent UNSUB"
end

function dispatch_commands(nc::NATSConnection)
    for command in nc.client_commands
        if isopen(nc.socket)
            push!(nc.commands, command)
        else
            push!(nc.client_commands, command)
            break
        end
    end
    close(nc.commands)
    @debug "Dispatch commands ended"
end

function write_loop(nc::NATSConnection)
    for command in nc.commands
        if isopen(nc.socket)
            send(nc, command)
        else
            push!(nc.client_commands, command)
            break
        end
    end
    @debug "Write loop ended"
end

function drain(nc::NATSConnection; timeout = 10)
    timedwait(() -> !isready(nc.commands), timeout)
    close(nc.socket)
    return nothing
end

function reconnect(nc::NATSConnection)
    u = URI(nc.uri)
    nc.socket = Sockets.connect(u.host, parse(Int64, u.port))
    data = readuntil(nc.socket, CR_LF)
    cmd, payload = split(data, " ", limit = 2)

    if cmd != "INFO"
        error("Received wrong command from NATS!")
    end

    nc.info = JSON.read(payload, NATSInfo)

    push!(nc.commands, CONNECT(nc.options))

    nc.read_loop_task = errormonitor(@async read_loop(nc))
    nc.write_loop_task = errormonitor(@async write_loop(nc))
    nc.dispatch_commands_task = errormonitor(@async dispatch_commands(nc))
    nc.reconnections += 1
    nc.status = :connected
    return nothing
end

using Sockets
import URIs: URI
import JSON3 as JSON

const CR_LF = "\r\n"

uri = URI("nats://localhost:4222")
socket = connect(String(uri.host), parse(Int64, uri.port))

data = readuntil(socket, CR_LF)

struct NATSServerInfo
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

StructTypes.StructType(::Type{NATSServerInfo}) = StructTypes.Struct()

struct NATS
    socket::TCPSocket
    info::NATSServerInfo
end

function connect_to_nats(uri::String)
    uri = URI(uri)
    socket = connect(String(uri.host), parse(Int64, uri.port))
    data = readuntil(socket, CR_LF)
    cmd, payload = split(data, limit = 2)
    if cmd == "INFO"
        return NATS(socket, JSON.read(payload, NATSServerInfo))
    else
        throw("Failed to connect to nats")
    end
end

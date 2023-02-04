import Base
import StructTypes

Base.@kwdef mutable struct ConnectOptions
    verbose::Bool = false
    pedantic::Bool = false
    tls_required::Bool = false
    auth_token::Union{String, Nothing} = nothing
    user::Union{String, Nothing} = nothing
    pass::Union{String, Nothing} = nothing
    name::Union{String, Nothing} = nothing
    lang::Union{String, Nothing} = "Julia"
    version::Union{String, Nothing} = "0.1.0"
    protocol::Int64 = 0
    echo::Bool = true
    sig::Union{String, Nothing} = nothing
    jwt::Union{String, Nothing} = nothing
    no_responders::Bool = false
end

StructTypes.StructType(::Type{ConnectOptions}) = StructTypes.Struct()


struct CONNECT
    options::ConnectOptions
end

struct SUB
    subject::String
    queue_group::Union{String, Nothing}
    sid::String
end

struct UNSUB
    sid::String
    max_msgs::Int64
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

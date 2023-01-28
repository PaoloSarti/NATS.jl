
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

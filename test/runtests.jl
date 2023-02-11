using Test
using NATS
import JuliaFormatter

@testset "NATS" begin
    @testset "Basic pub sub" begin
        nc = NATS.connect()
        subject = "hello"
        sub = subscribe(nc, subject)
        publish(nc, subject, "hi!")
        publish(nc, subject, "hola!")
        msg, _ = iterate(sub)
        @test string_payload(msg) == "hi!"
        msg, _ = iterate(sub)
        @test string_payload(msg) == "hola!"
        drain(nc)
        @test channel(sub).state == :closed
    end

    @testset "Unsubscribe with max msgs" begin
        nc = NATS.connect()
        subject = "hello"
        sub = subscribe(nc, subject)
        unsubscribe(nc, sub, max_msgs = 2)
        publish(nc, subject, "hi!")
        publish(nc, subject, "hola!")
        publish(nc, subject, "not arrived")
        msgs = collect(sub)
        @test length(msgs) == 2
        @test string_payload.(msgs) == ["hi!", "hola!"]
        @test channel(sub).state == :closed
        drain(nc)
    end

    @testset "Request - Response" begin
        nc = NATS.connect()
        subject = "hello"
        echo_sub = subscribe(nc, subject, msg -> publish(nc, msg.reply_to, msg.payload))
        msg = request(nc, subject, "hey")
        @test string_payload(msg) == "hey"
        drain(nc)
    end

    @test JuliaFormatter.format(
        "..",
        indent = 4,
        whitespace_typedefs = true,
        whitespace_ops_in_indices = true,
        remove_extra_newlines = true,
        always_use_return = true,
        whitespace_in_kwargs = true,
        trailing_comma = true,
        format_markdown = true,
    )
end

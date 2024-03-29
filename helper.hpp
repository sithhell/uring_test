//
// Created by th on 3/29/24.
// Copyright (c) 2024 Exasol AG. All rights reserved.
//

#pragma once

#include <liburing.h>

#include <source_location>
#include <system_error>

void throw_error_code_if(
    bool cond, int ec, std::source_location loc = std::source_location::current())
{
    if (cond)
    {
        throw std::system_error(ec, std::system_category());
    }
}

// Helper to retrieve a new submission queue entry
// If there are no entries available, we submit to inform the kernel
// of new submissions to be able to retrieve a new one.
::io_uring_sqe* get_sqe(::io_uring* ring)
{
    do
    {
        auto* sqe = ::io_uring_get_sqe(ring);
        if (sqe)
        {
            return sqe;
        }
        fprintf(stderr, "WOOOT?\n");
        ::io_uring_submit(ring);
    }
    while (true);
}

enum class operation_type
{
    accept = 1,
    connect = 2,
    recv = 3,
    send = 4,
    ring_msg = 5,
    stop = 6
};

std::string print_op(operation_type t)
{
    switch (t)
    {
    case operation_type::accept:
        return "accept";
    case operation_type::connect:
        return "connect";
    case operation_type::recv:
        return "recv";
    case operation_type::send:
        return "send";
    case operation_type::ring_msg:
        return "ring_msg";
    case operation_type::stop:
        return "ring_msg";
    default:
        return "<unknown>";
    }
}

struct base_operation
{
    virtual ~base_operation() = default;

    virtual void handle_completion(::io_uring* ring,
                                   operation_type type,
                                   ::io_uring_cqe* cqe) = 0;
};

static constexpr std::uint64_t operation_type_mask = 0xffff;
static constexpr std::uint64_t base_operation_shift = 16;

std::uint64_t encode_userdata(operation_type type, base_operation* op)
{
    return static_cast<std::uint64_t>(type) | std::bit_cast<std::uint64_t>(op)
        << base_operation_shift;
}

std::pair<operation_type, base_operation*> decode_userdata(std::uint64_t data)
{
    return {
        static_cast<operation_type>(data & operation_type_mask),
        std::bit_cast<base_operation*>(data >> base_operation_shift)
    };
}

void request_stop(io_uring* ring)
{
    ::io_uring dispatch_ring;

    int rc{0};
    int flags = IORING_SETUP_SINGLE_ISSUER;
    flags |= IORING_SETUP_DEFER_TASKRUN;
    rc = ::io_uring_queue_init(1024, &dispatch_ring, flags);
    throw_error_code_if(rc < 0, -rc);

    auto* sqe = get_sqe(&dispatch_ring);
    auto msg_data = encode_userdata(operation_type::stop, nullptr);
    ::io_uring_prep_msg_ring(sqe, ring->ring_fd, 0, msg_data, 0);
    ::io_uring_submit_and_wait(&dispatch_ring, 1);

    ::io_uring_queue_exit(&dispatch_ring);
}

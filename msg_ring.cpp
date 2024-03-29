//
// Created by th on 3/29/24.
// Copyright (c) 2024 Exasol AG. All rights reserved.
//

#include "helper.hpp"
#include <liburing.h>
#include <vector>
#include <thread>

std::size_t num_messages{1'000'000};
std::size_t num_dispatcher{1};

std::atomic<bool> event_loop_running{false};
std::atomic<bool> stop{false};
::io_uring event_ring;

void handle_events(::io_uring* ring)
{
    ::io_uring_cqe* cqe{nullptr};
    unsigned head;
    unsigned i = 0;
    io_uring_for_each_cqe(ring, head, cqe)
    {
        auto [type, op] = decode_userdata(cqe->user_data);
        if (op != nullptr)
        {
            op->handle_completion(ring, type, cqe);
        }
        i++;
    }
    if (i == 0)
    {
        return;
    }
    io_uring_cq_advance(ring, i);
}

void event_loop()
{
    int rc{0};
    int flags = IORING_SETUP_SINGLE_ISSUER;
    flags |= IORING_SETUP_DEFER_TASKRUN;
    rc = ::io_uring_queue_init(1024, &event_ring, flags);
    throw_error_code_if(rc < 0, -rc);

    event_loop_running = true;

    while (!stop)
    {
        rc = ::io_uring_submit_and_wait(&event_ring, 1);
        throw_error_code_if(rc < 0, -rc);
        handle_events(&event_ring);
    }

    ::io_uring_queue_exit(&event_ring);
}

// This is responsible for handling the completion of the msg we sent to the
// event ring. Once this message completed, we immediately respond with the reply
// to the source ring.
struct message_dispatch : base_operation
{
    void handle_completion(io_uring* ring, operation_type type,
        io_uring_cqe* cqe) final
    {
        throw_error_code_if(cqe->res < 0, -cqe->res);
        // cqe->res contains the file descriptor for the source ring
        int ring_fd = cqe->res;

        auto* sqe = get_sqe(ring);
        auto msg_data = encode_userdata(operation_type::ring_msg, nullptr);
        ::io_uring_prep_msg_ring(sqe, ring_fd, 0, msg_data, 0);
        sqe->flags = IOSQE_CQE_SKIP_SUCCESS;
    }
};


// The "main" loop of this example:
// We repeat the message "ping-pong" for a couple of times and then exit.
void dispatch()
{
    ::io_uring dispatch_ring;

    int rc{0};
    int flags = IORING_SETUP_SINGLE_ISSUER;
    flags |= IORING_SETUP_DEFER_TASKRUN;
    rc = ::io_uring_queue_init(1024, &dispatch_ring, flags);
    throw_error_code_if(rc < 0, -rc);

    message_dispatch msg;
    for (std::size_t i = 0; i != num_messages; ++i)
    {
        auto* sqe = get_sqe(&dispatch_ring);
        auto msg_data = encode_userdata(operation_type::ring_msg, &msg);
        ::io_uring_prep_msg_ring(sqe, event_ring.ring_fd, dispatch_ring.ring_fd, msg_data, 0);
        sqe->flags = IOSQE_CQE_SKIP_SUCCESS;
        ::io_uring_submit_and_wait(&dispatch_ring, 1);
        handle_events(&dispatch_ring);
    }

    ::io_uring_queue_exit(&dispatch_ring);
}

int main(int argc, char** argv)
{
    for (int i = 1; i != argc; ++i) {
        if (argv[i] == std::string_view{"num_requests"}) {
            if (i + 1 < argc) {
                num_messages = std::atoi(argv[i + 1]);
            }
        }

        if (argv[i] == std::string_view{"num_dispatcher"}) {
            if (i + 1 < argc) {
                num_dispatcher = std::atoi(argv[i + 1]);
            }
        }
    }

    std::jthread event_loop_thread{&event_loop};
    while (event_loop_running == false)
    {
        std::this_thread::yield();
    }

    std::vector<std::jthread> dispatcher;
    dispatcher.reserve(num_dispatcher-1);

    auto begin = std::chrono::steady_clock::now();
    for (std::size_t i = 1; i != num_dispatcher; ++i)
    {
        dispatcher.emplace_back(dispatch);
    }
    dispatch();
    // Reset the dispatcher...
    dispatcher.clear();
    auto end = std::chrono::steady_clock::now();
    // Convert the runtime into seconds, casting elapsed time to a floating
    // point number
    double duration = std::chrono::duration<double>(end - begin).count();
    printf("Requests/s: %f\n", num_messages / duration);

    // Send stop signal...
    stop = true;
    request_stop(&event_ring);
}
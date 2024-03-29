//
// Created by th on 3/29/24.
// Copyright (c) 2024 Exasol AG. All rights reserved.
//
#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <bit>
#include <chrono>
#include <cstring>
#include <list>
#include <mutex>
#include <optional>
#include <source_location>
#include <span>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "helper.hpp"

std::size_t num_requests = 1'000'000;
bool client_sync{false};
bool server_sync{false};
bool output{false};

struct basic_socket {
    void connect(sockaddr_un& addr) {
        native_handle = ::socket(AF_UNIX, SOCK_STREAM, 0);
        throw_error_code_if(native_handle == -1, errno);

        int res = 0;
        res = ::connect(native_handle, (const sockaddr*)&addr, sizeof(addr));
        throw_error_code_if(res == -1, errno);
    }

    int native_handle{-1};
};
struct basic_acceptor {
    explicit basic_acceptor(sockaddr_un& addr) {
        native_handle = ::socket(AF_UNIX, SOCK_STREAM, 0);
        throw_error_code_if(native_handle == -1, errno);

        int res = 0;
        res = bind(native_handle, (const sockaddr*)&addr, sizeof(addr));
        throw_error_code_if(res == -1, errno);

        res = listen(native_handle, 128);
        throw_error_code_if(res == -1, errno);
    }

    void local_endpoint(sockaddr_un& addr) {
        socklen_t addr_len = sizeof(addr);
        int res = ::getsockname(native_handle, (sockaddr*)&addr, &addr_len);
        throw_error_code_if(res == -1, errno);
    }
    int native_handle{-1};
};

struct buffer_ring {
    buffer_ring(::io_uring* ring,
                int buffer_id,
                std::uint32_t num_buffers,
                int buffer_size)
        : ring(ring),
          bgid(buffer_id),
          num_buffers(num_buffers),
          buffer_size(buffer_size) {
        static int page_size = sysconf(_SC_PAGESIZE);

        ::io_uring_buf_reg reg{};
        // allocate memory for sharing buffer ring
        auto res = ::posix_memalign(reinterpret_cast<void**>(&br),
                                    page_size,
                                    num_buffers * sizeof(::io_uring_buf_ring));
        throw_error_code_if(res != 0, res);

        res = ::posix_memalign(&buffer, page_size, num_buffers * buffer_size);
        throw_error_code_if(res != 0, res);

        // assign and register buffer ring
        reg.ring_addr = std::bit_cast<unsigned long>(br);
        reg.ring_entries = num_buffers;
        reg.bgid = buffer_id;
        res = ::io_uring_register_buf_ring(ring, &reg, 0);
        throw_error_code_if(res < 0, -res);

        // Add initial buffers to the ring
        ::io_uring_buf_ring_init(br);
        auto* ptr = reinterpret_cast<char*>(buffer);
        for (std::uint32_t i = 0; i < num_buffers; ++i) {
            ::io_uring_buf_ring_add(
                br, ptr, buffer_size, i, ::io_uring_buf_ring_mask(num_buffers), i);
            ptr += buffer_size;
        }
        ::io_uring_buf_ring_advance(br, num_buffers);
    }
    ~buffer_ring() noexcept {
        if (ring == nullptr) {
            return;
        }
        ::io_uring_unregister_buf_ring(ring, bgid);
        ::free(br);
        ::free(buffer);
    }

    buffer_ring(buffer_ring const&) = delete;
    buffer_ring& operator=(buffer_ring const&) = delete;

    buffer_ring(buffer_ring&& other) noexcept
        : ring(std::exchange(other.ring, nullptr)),
          bgid(other.bgid),
          num_buffers(other.num_buffers),
          buffer_size(other.buffer_size),
          br(other.br),
          buffer(other.buffer) {
    }
    buffer_ring& operator=(buffer_ring&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        ring = std::exchange(other.ring, nullptr);
        bgid = other.bgid;
        num_buffers = other.num_buffers;
        buffer_size = other.buffer_size;
        br = other.br;
        buffer = other.buffer;
        return *this;
    }

    int id() const noexcept {
        return bgid;
    }

    std::span<std::byte> get(int buffer_id) const noexcept {
        return {reinterpret_cast<std::byte*>(buffer) + (buffer_id * buffer_size),
                buffer_size};
    }

    void replenish(int buffer_id, void* buf) const noexcept {
        ::io_uring_buf_ring_add(
            br, buf, buffer_size, buffer_id, ::io_uring_buf_ring_mask(num_buffers), 0);
        ::io_uring_buf_ring_advance(br, 1);
    }

    ::io_uring* ring;
    int bgid;
    std::uint32_t num_buffers;
    std::size_t buffer_size;
    ::io_uring_buf_ring* br{nullptr};
    void* buffer{nullptr};
};

struct server_connection : base_operation {
    server_connection(server_connection const&) = delete;
    server_connection& operator=(server_connection const&) = delete;

    server_connection(int fd, ::io_uring* ring, buffer_ring recv_buffer)
        : socket_{fd}, recv_buffer_(std::move(recv_buffer)) {
        submit_recv(ring);
    }

    void submit_recv(::io_uring* ring) {
        auto* sqe = get_sqe(ring);
        ::io_uring_prep_recv_multishot(sqe, socket_, nullptr, 0, MSG_NOSIGNAL);
        sqe->flags |= IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        sqe->buf_group = recv_buffer_.id();
        sqe->user_data = encode_userdata(operation_type::recv, this);
    }

    void handle_completion(::io_uring* ring,
                           operation_type type,
                           ::io_uring_cqe* cqe) final {
        switch (type) {
            case operation_type::recv: {
                if (cqe->res == -ENOBUFS) {
                    submit_recv(ring);
                    return;
                }
                int size = cqe->res;
                if (size == -EPIPE) {
                    break;
                }
                throw_error_code_if(size < 0, -size);
                if (cqe->flags & IORING_CQE_F_SOCK_NONEMPTY) {
                    fprintf(stdout, "server would have more data on socket\n");
                }
                handle_recv(ring, size, cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                // handle case where we need to rearm the request...
                if ((cqe->flags & IORING_CQE_F_MORE) == 0) {
                    submit_recv(ring);
                }
                break;
            }
            case operation_type::send: {
                int size = cqe->res;
                if (size == -EPIPE) {
                    break;
                }
                throw_error_code_if(size < 0, -size);
                bytes_sent += size;
                if (bytes_sent != bytes_expected) {
                    submit_send(ring);
                } else {
                    bytes_sent = 0;
                }
                break;
            }
            default:
                std::abort();
        }
    }

    void handle_recv(::io_uring* ring, int size, int buffer_id) {
        auto buffer = recv_buffer_.get(buffer_id);
        if (response_.size() != bytes_expected) {
            response_.resize(bytes_expected);
        }
        std::memcpy(response_.data() + bytes_recvd, buffer.data(), size);
        recv_buffer_.replenish(buffer_id, buffer.data());
        bytes_recvd += size;

        if (bytes_recvd == bytes_expected) {
            submit_send(ring);
            bytes_recvd = 0;
        }
    }

    void submit_send(::io_uring* ring) {
        auto* sqe = get_sqe(ring);
        ::io_uring_prep_send(
            sqe, socket_, response_.data(), response_.size(), MSG_NOSIGNAL);
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        sqe->flags |= IOSQE_FIXED_FILE;
        sqe->user_data = encode_userdata(operation_type::send, this);
    }

    int socket_;
    buffer_ring recv_buffer_;
    std::vector<std::byte> response_;
    std::size_t bytes_expected{128};
    std::size_t bytes_recvd{0};
    std::size_t bytes_sent{0};
};

struct client_connection;

struct message_handler {
    explicit message_handler(sockaddr_un& endpoint) : acceptor_{endpoint} {
        eventfd_ = ::eventfd(0, 0);
    }

    void register_stop() {
        auto* sqe = get_sqe(&ring_);
        ::io_uring_prep_read(sqe, eventfd_, &eventfd_dummy_, sizeof(eventfd_dummy_), 0);
        sqe->user_data = encode_userdata(operation_type::stop, nullptr);
    }

    void run() {
        if (server_sync) {
            int fd = ::accept(acceptor_.native_handle, nullptr, nullptr);
            throw_error_code_if(fd < 0, fd);
            std::vector<std::byte> response_;
            std::size_t bytes_expected{128};
            std::size_t bytes_recvd{0};
            for (std::size_t i = 0; i != num_requests; ++i) {
                std::size_t bytes_recvd = 0;
                std::size_t bytes_sent = 0;
                if (response_.size() != bytes_expected) {
                    response_.resize(bytes_expected);
                }
                while (bytes_recvd != bytes_expected) {
                    int recvd = recv(fd,
                                     response_.data() + bytes_recvd,
                                     response_.size() - bytes_recvd,
                                     MSG_NOSIGNAL);
                    if (recvd == 0) {
                        break;
                    }
                    throw_error_code_if(recvd < 0, errno);
                    bytes_recvd += recvd;
                }
                while (bytes_sent != bytes_expected) {
                    int sent = send(fd,
                                    response_.data() + bytes_sent,
                                    response_.size() - bytes_sent,
                                    MSG_NOSIGNAL);
                    throw_error_code_if(sent < 0, errno);
                    bytes_sent += sent;
                }
            }

            return;
        }
        ::io_uring_queue_init(
            1024, &ring_, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
        ::io_uring_register_ring_fd(&ring_);
        static constexpr int max_files = 65536;
        ::io_uring_register_files_sparse(&ring_, max_files);
        ::io_uring_register_file_alloc_range(&ring_, 1, max_files / 2);
        fd_offset = max_files / 2 + 1;
        // Submit initial accept...
        submit_accept();
        running_.store(true, std::memory_order_release);

        // Submit eventfd handler...
        register_stop();

        // Start the event loop...
        static constexpr int min_complete = 1;
        __kernel_timespec ts{};
        ts.tv_sec = 0;
        ts.tv_nsec = 1000 * 1000;

        int num_calls = 0;
        while (!stop_.load(std::memory_order_acquire)) {
            ::io_uring_cqe *cqe;
            int num_completed = ::io_uring_submit_and_wait(&ring_, 1);
            //int num_completed = ::io_uring_submit_and_get_events(&ring_);
            // Ignore EINTR...
            if (num_completed == -EINTR) {
                num_completed = 0;
            }
            throw_error_code_if(num_completed < 0, -num_completed);
            handle_completions(&ring_, cqe);
        }


        ::io_uring_queue_exit(&ring_);
    }

    void local_endpoint(sockaddr_un& endpoint) {
        acceptor_.local_endpoint(endpoint);
    }

    client_connection& client(sockaddr_un& endpoint);

    void stop() {
        stop_.store(true, std::memory_order_release);
        eventfd_write(eventfd_, 1);
    }

    buffer_ring get_buffer_ring(std::size_t num_buffers, std::size_t buffer_size) {
        int bgid = buffer_id;
        ++buffer_id;
        return buffer_ring(&ring_, bgid, num_buffers, buffer_size);
    }

    int register_fd(int fd) {
        int off = fd_offset;
        ++fd_offset;
        ::io_uring_register_files_update(&ring_, off, &fd, 1);
        return off;
    }

    int ring_fd() const {
        return ring_.ring_fd;
    }

    void submit_accept() {
        int acceptor_fd = acceptor_.native_handle;
        ::io_uring_register_files_update(&ring_, 0, &acceptor_fd, 1);
        auto* sqe = get_sqe(&ring_);
        ::io_uring_prep_multishot_accept_direct(sqe, 0, nullptr, nullptr, 0);
        sqe->flags = IOSQE_FIXED_FILE;
        sqe->file_index = IORING_FILE_INDEX_ALLOC;
        sqe->user_data = encode_userdata(operation_type::accept, nullptr);
    }

    void handle_completions(::io_uring* ring, ::io_uring_cqe* cqe) {
        unsigned head;
        unsigned i = 0;
        io_uring_for_each_cqe(ring, head, cqe) {
            auto [type, connection] = decode_userdata(cqe->user_data);
            if (type == operation_type::accept) {
                new_connection(ring, cqe);
                if ((cqe->flags & IORING_CQE_F_MORE) == 0) {
                    submit_accept();
                }
            } else if (connection != nullptr) {
                connection->handle_completion(ring, type, cqe);
            }
            i++;
        }
        if (i == 0) {
            return;
        }
        io_uring_cq_advance(ring, i);
    }

    void new_connection(::io_uring* ring, ::io_uring_cqe* cqe) {
        int fd = cqe->res;
        throw_error_code_if(fd < 0, -fd);
        accepted_connections_.emplace_back(fd, ring, get_buffer_ring(16, 128));
        ++buffer_id;
    }

    basic_acceptor acceptor_;
    std::list<server_connection> accepted_connections_;
    int buffer_id{0};
    int fd_offset{0};

    std::atomic<bool> stop_{false};

    ::io_uring ring_;
    int eventfd_{-1};
    eventfd_t eventfd_dummy_{0};
    std::atomic<bool> running_{false};

    // WORKAROUND FOR REPRO, THIS IS REALLY A CONNECTION POOL...
    std::unique_ptr<client_connection> connection_;
};

struct dispatch_context {
    dispatch_context() {
        int rc = ::io_uring_queue_init(
            2, &ring_, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
        throw_error_code_if(rc < 0, -rc);
        ::io_uring_register_ring_fd(&ring_);
    }
    ~dispatch_context() noexcept {
        ::io_uring_queue_exit(&ring_);
    }
    ::io_uring_sqe* get_sqe() {
        return ::get_sqe(&ring_);
    }

    int ring_fd() const {
        return ring_.ring_fd;
    }

    void submit_and_wait(int n) {
        while (n > 0) {
            //int rc = ::io_uring_submit_and_get_events(&ring_);
            int rc = ::io_uring_submit_and_wait(&ring_, 1);
            throw_error_code_if(rc < 0, -rc);
            // Replenish CQEs
            ::io_uring_cqe* cqe;
            unsigned head;
            unsigned i = 0;
            io_uring_for_each_cqe(&ring_, head, cqe) {
                ++i;
            }
            if (i == 0) {
                return;
            }
            io_uring_cq_advance(&ring_, i);
            n -= i;
        }
    }

    ::io_uring ring_;
};

struct client_connection : base_operation {
    static constexpr std::size_t initial = 0x0;
    static constexpr std::size_t sending = 0x1;
    static constexpr std::size_t receiving = 0x2;
    static constexpr std::size_t waiting = 0x4;

    client_connection(client_connection const&) = delete;
    client_connection& operator=(client_connection const&) = delete;

    explicit client_connection(message_handler* handler, sockaddr_un& endpoint)
        : handler_{handler} {
        socket_.connect(endpoint);
    }

    ~client_connection() noexcept {
    }

    template <typename Buffer>
    void invoke(Buffer& buffer) {
        bytes_sent = 0;
        bytes_recvd = 0;

        if (client_sync) {
            while (bytes_sent != send_buffer_.size()) {
                int sent = send(socket_.native_handle,
                                send_buffer_.data() + bytes_sent,
                                send_buffer_.size() - bytes_sent,
                                MSG_NOSIGNAL);
                throw_error_code_if(sent < 0, errno);
                bytes_sent += sent;
            }
            while (bytes_recvd != send_buffer_.size()) {
                int recvd = recv(socket_.native_handle,
                                 send_buffer_.data() + bytes_recvd,
                                 send_buffer_.size() - bytes_recvd,
                                 MSG_NOSIGNAL);
                throw_error_code_if(recvd < 0, errno);
                bytes_recvd += recvd;
            }
            return;
        }
        static thread_local dispatch_context dispatcher;
        dispatch_ring_fd_ = dispatcher.ring_fd();
        auto* sqe = dispatcher.get_sqe();
        std::uint64_t msg_data = encode_userdata(operation_type::ring_msg, this);
        ::io_uring_prep_msg_ring(sqe, handler_->ring_fd(), 0, msg_data, 0);
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
        dispatcher.submit_and_wait(1);
        std::size_t expected = 0;
        while (!state_.compare_exchange_strong(
            expected, sending | receiving | waiting, std::memory_order_acq_rel)) {
            expected = 0;
            std::this_thread::yield();
        }
    }

    void handle_completion(::io_uring* ring,
                           operation_type type,
                           ::io_uring_cqe* cqe) final {
        switch (type) {
            case operation_type::recv: {
                // If we got a receive completion and no buffer space available, we need
                // to handle it early and re submit out receive operation
                if (cqe->res == -ENOBUFS) {
                    fprintf(stdout, "resubmit receive ... no buffer available\n");
                    submit_recv(ring);
                    return;
                }
                int size = cqe->res;
                throw_error_code_if(size < 0, -size);
                if (cqe->flags & IORING_CQE_F_SOCK_NONEMPTY) {
                    fprintf(stdout, "client would have more data on socket\n");
                }
                bool done =
                    handle_recv(ring, size, cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                // handle case where we need to rearm the request...
                if ((cqe->flags & IORING_CQE_F_MORE) == 0) {
                    submit_recv(ring);
                }
                if (done) {
                    auto prev = state_.fetch_xor(receiving, std::memory_order_release);
                    if ((prev ^ receiving) == 0) {
                        notify(ring);
                    }
                }
                return;
            }
            case operation_type::send: {
                int size = cqe->res;
                throw_error_code_if(size < 0, -size);
                bytes_sent += size;
                if (bytes_sent == send_buffer_.size()) {
                    auto prev = state_.fetch_xor(sending, std::memory_order_release);
                    if ((prev ^ sending) == 0) {
                        notify(ring);
                    }
                } else {
                    submit_send(ring);
                }
                return;
            }
            case operation_type::ring_msg: {
                int size = cqe->res;
                throw_error_code_if(size < 0, -size);
                if (registered_socket_fd_ == -1) {
                    registered_socket_fd_ = handler_->register_fd(socket_.native_handle);
                }
                // Before we initiate any send, we need to check if we need to rearm our
                // receive side.
                if (!recv_buffer_) {
                    recv_buffer_.emplace(handler_->get_buffer_ring(16, 256));
                    submit_recv(ring);
                }
                submit_send(ring);
                auto prev = state_.fetch_xor(waiting, std::memory_order_release);
                if ((prev ^ waiting) == 0) {
                    notify(ring);
                }
                return;
            }
            default: {
                fprintf(stdout, "TODO\n");
                std::abort();
            }
        }
    }

    void submit_send(::io_uring* ring) {
        // Now, submit the send operation!
        auto* sqe = get_sqe(ring);
        ::io_uring_prep_send(sqe,
                             registered_socket_fd_,
                             send_buffer_.data() + bytes_sent,
                             send_buffer_.size() - bytes_sent,
                             MSG_NOSIGNAL);
        sqe->flags |= IOSQE_FIXED_FILE;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        sqe->user_data = encode_userdata(operation_type::send, this);
    }

    bool handle_recv(::io_uring* ring, int size, int buffer_id) {
        auto buffer = recv_buffer_->get(buffer_id);
        if (response_.size() != send_buffer_.size()) {
            response_.resize(send_buffer_.size());
        }
        std::memcpy(response_.data() + bytes_recvd, buffer.data(), size);
        recv_buffer_->replenish(buffer_id, buffer.data());
        bytes_recvd += size;

        if (bytes_recvd == send_buffer_.size()) {
            // Once we got the receive completion, we need to inform the initial
            // dispatcher that everything is done.
            return true;
        }
        return false;
    }

    void notify(::io_uring* ring) {
        auto* sqe = get_sqe(ring);
        std::uint64_t msg_data = encode_userdata(operation_type::ring_msg, this);
        ::io_uring_prep_msg_ring(sqe, dispatch_ring_fd_, 0, msg_data, 0);
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
        sqe->user_data = encode_userdata(operation_type::ring_msg, nullptr);
    }

    void submit_recv(::io_uring* ring) {
        auto* sqe = get_sqe(ring);
        ::io_uring_prep_recv_multishot(
            sqe, registered_socket_fd_, nullptr, 0, MSG_NOSIGNAL);
        sqe->flags |= IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        sqe->buf_group = recv_buffer_->id();
        sqe->user_data = encode_userdata(operation_type::recv, this);
    }

    void wait_outstanding() {
        while (state_.load(std::memory_order_acquire) != 0) {
            std::this_thread::yield();
        }
    }

    ::basic_socket socket_;
    message_handler* handler_;
    int registered_socket_fd_{-1};

    std::array<std::byte, 128> send_buffer_;
    std::size_t bytes_sent{0};

    std::optional<buffer_ring> recv_buffer_;
    std::size_t bytes_recvd{0};

    std::vector<std::byte> response_;

    int dispatch_ring_fd_{-1};

    alignas(64) std::atomic<std::size_t> state_{sending | receiving | waiting};
};

client_connection& message_handler::client(sockaddr_un& endpoint) {
    if (!server_sync) {
        while (!running_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    if (!connection_) {
        connection_ = std::make_unique<client_connection>(this, endpoint);
    }
    return *connection_.get();
}

void client(sockaddr_un& endpoint, int clientid) {
    sockaddr_un tmp_endpoint{};
    tmp_endpoint.sun_family = AF_UNIX;
    memcpy(tmp_endpoint.sun_path, "\0sock", 6);
    memcpy(tmp_endpoint.sun_path + 7, &clientid, sizeof(clientid));

    message_handler handler{tmp_endpoint};

    std::jthread client_handler{[&] {
        if (server_sync)
            return;
        handler.run();
    }};

    auto& client = handler.client(endpoint);

    std::array<std::byte, 128> buffer;
    for (std::size_t i = 0; i != num_requests; ++i) {
        auto begin = std::chrono::steady_clock::now();
        client.invoke(buffer);
        auto end = std::chrono::steady_clock::now();
        if (output) {
            fprintf(stderr, "%lu\n", (end - begin).count());
        }
    }
    handler.stop();
}

void run(message_handler& handler, std::size_t num_clients) {
    std::jthread server{[&] { handler.run(); }};

    std::vector<std::jthread> clients;
    clients.reserve(num_clients);
    auto begin = std::chrono::steady_clock::now();
    sockaddr_un endpoint{};
    handler.local_endpoint(endpoint);
    for (std::size_t i = 0; i < num_clients; ++i) {
        clients.emplace_back([&, i] { client(endpoint, i + 1); });
    }

    for (std::jthread& t : clients) {
        t.join();
    }
    auto end = std::chrono::steady_clock::now();
    auto run_duration = std::chrono::duration<double>(end - begin);
    fprintf(stdout, "Clients took %f seconds\n", run_duration.count());
    fprintf(
        stdout, "Requests/s: %f\n", (num_requests * num_clients) / run_duration.count());
    handler.stop();
}

int main(int argc, char** argv) {
    std::size_t num_clients = 1;

    for (int i = 1; i != argc; ++i) {
        if (argv[i] == std::string_view{"output"}) {
            output = true;
        }
        if (argv[i] == std::string_view{"client_sync"}) {
            client_sync = true;
        }
        if (argv[i] == std::string_view{"server_sync"}) {
            server_sync = true;
        }
        if (argv[i] == std::string_view{"num_requests"}) {
            if (i + 1 < argc) {
                num_requests = std::atoi(argv[i + 1]);
            }
        }

        if (argv[i] == std::string_view{"num_clients"}) {
            if (i + 1 < argc) {
                num_clients = std::atoi(argv[i + 1]);
            }
        }
    }

    sockaddr_un endpoint{};
    endpoint.sun_family = AF_UNIX;
    memcpy(endpoint.sun_path, "\0sock", 6);
    message_handler handler{endpoint};
    run(handler, num_clients);
}

# Examples/benchmarks for using liburing

## Compilation

You need to have liburing installed first under `${PREFIX}` (which can be anything).
This project is using pkg-config to get information where to find your liburing installation.
This is done by prepending `${PREFIX}/lib/pkgconfig` to the `PKG_CONFIG_PATH` environment variable
(i.e. `export PKG_CONFIG="${PREFIX}/lib/pkgconfig":${PKG_CONFIG_PATH}`).

If you have cmake installed, you can use that to have the project setup. Otherwise, use the Makefile.

## msg_ring

Here we test the ability to communicate from one ring to another from multiple threads.
The setup is as follows:
 - One "event-loop" thread which is waiting in `io_uring_submit_and_wait`
 - Dispatcher threads which send a message to the event loop.
   Once that message is received by the "event-loop" it is being sent back to the dispatcher

This example is to test the scalability of the approach to have a single IO event loop which is being
fed with IO requests from different threads.

Additional optional arguments are:

- `num_dispatcher N` start N dispatcher. Default is 1
- `num_messages N` send N messages. Default is 1000000

## ping_pong

Building on the msg_ring application, this is supposed to demonstrate how to dispatch socket send/recv
from a particular thread to the event loop using liburing.
Please see https://github.com/axboe/liburing/discussions/1109 for additional information and the performance
considerations.

The general idea of this application can be summarized as follows:
 
 - One process is serving as both a client and a server.
 - The "client" and "server" connections can be considered persistent throughout the application run.
 - The "server" side is accepting connect requests (through io_uring_prep_multishot_accept_direct) and is then receiving (through io_uring_prep_recv_multishot). Once a single message is complete, a response is sent. This shoudl later be used to invoke a RPC.
 - Accept, receive and send does not require any interference from another thread is completely handled in the IO uring event handling loop.
 - When a client wants to invoke a request however, it is initiated from a different thread. The idea is to use io_uring_prep_ring_msg to notify the event handling loop, and then initiate the send and receive part. Since the "client" is persistent, the receive is again handled with a multishot request.
 - All requests are being issued with IOSQE_FIXED_FILE.

The application can be run in three different modes:

 - ./ping_pong # Additional optional arguments
   Runs the application in "full" io_uring mode
 - ./ping_pong client_sync # Additional optional arguments
   Only performs the "receiving" end in the io uring event loop. The "dispatcher" is doing regular send/recv
 - ./ping_pong client_sync server_sync # Additional optional arguments
   This does not use io_uring at all.

Additional optional arguments are:

 - `num_clients N` start N clients. Default is 1
 - `num_requests N` send N requests. Default is 1000000
 - `output` output duration for each request
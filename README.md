# rillnet

rillnet is a C++20 library for building asynchronous, typed request/response protocols on top of byte-stream transports. It currently provides the core protocol pieces needed to encode frames, register typed messages, move bytes over TCP, and drive one client-side or server-side connection with Boost.Asio coroutines.

The library is useful in its current state when you want a small protocol layer that separates application message types from transport details. It gives you a deterministic frame format, typed message dispatch, connection-local stream identifiers, a single ordered write path, and explicit status codes without requiring each application to rebuild those foundations around Boost.Asio sockets.

## Implemented Features

- C++20 library target `rillnet::rillnet` with public headers under `include/rillnet`.
- Strongly typed identifiers for streams and message types, including client/server stream-id partitioning.
- Protocol version, frame flags, frame validation, and status/error classification helpers.
- A fixed 18-byte big-endian frame header format plus full frame encoding.
- Incremental frame decoding from byte-stream input, including malformed-frame terminal error state.
- A transport abstraction with a Boost.Asio TCP implementation.
- Asynchronous TCP client connection establishment with timeout-aware `ConnectResult` failures.
- Asynchronous TCP server accept loop that hands each accepted socket to a connection handler as a `Transport`.
- A codec concept with `EncodeResult` and `DecodeResult<T>` result types.
- `PodCodec`, a minimal built-in codec for trivially copyable messages, intended for tests and simple examples rather than portable application wire formats.
- A typed message registry mapping C++ message types to non-zero wire `MessageType` identifiers per protocol version.
- Typed message payload and frame encoding/decoding, with the wire message type stored as a 4-byte big-endian payload prefix.
- A FIFO `WriteQueue` that serializes all writes for a connection so concurrent operations never overlap writes on the same transport.
- `ClientConnection`, which sends typed requests, waits for matching typed responses by stream id, supports relative timeouts and absolute deadlines, fails pending requests when the connection closes, and supports bounded graceful shutdown.
- `ServerConnection`, which dispatches request frames to registered typed asynchronous handlers, writes typed responses through the connection write queue, and supports bounded graceful shutdown.
- GoogleTest unit coverage for the implemented public headers and connection behavior.

## Build Requirements

- CMake 3.24 or newer.
- A C++20 compiler.
- Boost 1.83 or newer headers.
- GoogleTest when `RILLNET_BUILD_TESTS` and CTest testing are enabled.

## Building and Testing

Configure and build the default debug preset:

```sh
cmake --preset debug
cmake --build --preset debug
```

Run the unit tests:

```sh
ctest --preset debug
```

The repository also defines sanitizer presets:

```sh
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan
```

## Basic Shape

Applications define message types, register each type with a `MessageRegistry`, provide or select a codec, then run a connection object on a Boost.Asio executor. Client code uses `ClientConnection::request<Request, Response>()`; server code registers handlers with `ServerConnection::handle<Request>()`, where each handler returns an awaitable response type.

The transport-facing layer works in frames and bytes. The application-facing layer works in typed request and response values.

## Graceful Shutdown

Call `shutdown(duration)` on a client or server connection to enter the draining state. New client
requests are rejected, while existing client requests and server handlers are allowed to finish.
When the duration expires, unfinished work is completed with `StatusCode::connection_closed` and
the transport is closed. The operation is awaitable and returns after the connection reaches the
closed state.

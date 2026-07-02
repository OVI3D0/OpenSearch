/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightRuntimeException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtocolOutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.nativeprotocol.NativeOutboundMessage;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Outbound handler for Arrow Flight streaming responses.
 * It must invoke messageListener and relay any exception back to the caller and not supress them
 * @opensearch.internal
 */
class FlightOutboundHandler extends ProtocolOutboundHandler {
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;

    public FlightOutboundHandler(String nodeName, Version version, String[] features, StatsTracker statsTracker, ThreadPool threadPool) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
    }

    @Override
    public void sendRequest(
        DiscoveryNode node,
        TcpChannel channel,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        Version channelVersion,
        boolean compressRequest,
        boolean isHandshake
    ) throws IOException, TransportException {
        throw new UnsupportedOperationException("sendRequest not implemented for FlightOutboundHandler");
    }

    @Override
    public void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in FlightOutboundHandler; use sendResponseBatch()"
        );
    }

    @Override
    public void sendErrorResponse(
        Version nodeVersion,
        Set<String> features,
        TcpChannel channel,
        long requestId,
        String action,
        Exception error
    ) throws IOException {
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in FlightOutboundHandler; use sendResponseBatch()"
        );
    }

    public void sendResponseBatch(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake,
        final boolean sync
    ) throws IOException {
        ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext();
        BatchTask task = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            response,
            compress,
            isHandshake,
            false,
            false,
            null,
            storedContext
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        // Block the producer thread before queuing the batch so a slow consumer throttles
        // allocation rather than letting the eventloop's queue grow. Note: isReady()
        // reflects only gRPC's outbound buffer, not our own queue depth — see
        // docs/backpressure.md "Known limitation: unbounded eventloop queue".
        flightChannel.awaitReadyOrThrow();

        // NOTE: 3.5 lacks #22244's releaseUnsent/handedOff buffer-ownership model, so we keep #22359's
        // sync/async send path but omit the releaseUnsent cleanup wrapper (the BatchTask try-with-resources
        // frees under 3.5's older buffer lifecycle instead).
        final Runnable sendBatch = threadPool.getThreadContext().preserveContext(() -> {
            try (BatchTask ignored = task) {
                processBatchTask(task);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        });
        // sync blocks the caller until the batch has been pushed to gRPC's outbound buffer before
        // returning; async returns once it is queued. Both run on the channel's single-threaded executor.
        if (sync) {
            Future<?> future = flightChannel.getExecutor().submit(sendBatch);
            awaitSend(future);
        } else {
            flightChannel.getExecutor().execute(sendBatch);
        }
    }

    /**
     * Blocks the caller until the executor has run the submitted send, so the caller cannot submit the
     * next batch until this one has been pushed to gRPC's outbound buffer (a full buffer is throttled by
     * the readiness back-pressure gate). The send itself runs on the channel's flight executor (so it is
     * serialized with the stream-root free that {@code close()} posts to the same executor); this only
     * parks the caller for the result. The caller must be a producer thread, not the flight executor
     * thread itself (blocking on the result from that thread would deadlock the single-threaded executor).
     */
    private void awaitSend(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamException(StreamErrorCode.INTERNAL, "Interrupted while sending batch synchronously", e);
        } catch (ExecutionException e) {
            // The work body catches its own exceptions and routes them to the listener, so this is not
            // expected; surface it rather than swallow if it ever happens.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof StreamException se) {
                throw se;
            }
            throw new StreamException(StreamErrorCode.INTERNAL, "Error sending batch synchronously", cause);
        }
    }

    private void processBatchTask(BatchTask task) {
        task.storedContext().restore();
        if (!(task.channel() instanceof FlightServerChannel flightChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            try (VectorStreamOutput out = new VectorStreamOutput(flightChannel.getAllocator(), flightChannel.getRoot())) {
                task.response().writeTo(out);
                flightChannel.sendBatch(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()), out);
                messageListener.onResponseSent(task.requestId(), task.action(), task.response());
            }
        } catch (FlightRuntimeException e) {
            messageListener.onResponseSent(task.requestId(), task.action(), FlightErrorMapper.fromFlightException(e));
        } catch (Exception e) {
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    public void completeStream(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action
    ) {
        ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext();
        BatchTask completeTask = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            TransportResponse.Empty.INSTANCE,
            false,
            false,
            true,
            false,
            null,
            storedContext
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        flightChannel.getExecutor().execute(() -> {
            try (BatchTask ignored = completeTask) {
                processCompleteTask(completeTask);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        });
    }

    private void processCompleteTask(BatchTask task) {
        task.storedContext().restore();
        if (!(task.channel() instanceof FlightServerChannel flightChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            flightChannel.completeStream(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()));
            messageListener.onResponseSent(task.requestId(), task.action(), TransportResponse.Empty.INSTANCE);
        } catch (Exception e) {
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    public void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action,
        final Exception error
    ) {
        ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext();
        BatchTask errorTask = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            null,
            false,
            false,
            false,
            true,
            error,
            storedContext
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        flightChannel.getExecutor().execute(() -> {
            try (BatchTask ignored = errorTask) {
                processErrorTask(errorTask);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        });
    }

    private void processErrorTask(BatchTask task) {
        task.storedContext().restore();
        if (!(task.channel() instanceof FlightServerChannel flightServerChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            Exception flightError = task.error();
            if (task.error() instanceof StreamException se) {
                flightError = FlightErrorMapper.toFlightException(se);
            }
            flightServerChannel.sendError(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()), flightError);
            messageListener.onResponseSent(task.requestId(), task.action(), task.error());
        } catch (Exception e) {
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    private ByteBuffer getHeaderBuffer(long requestId, Version nodeVersion, Set<String> features) throws IOException {
        // Just a way( probably inefficient) to serialize header to reuse existing logic present in
        // NativeOutboundMessage.Response#writeVariableHeader()
        NativeOutboundMessage.Response headerMessage = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            out -> {},
            Version.min(version, nodeVersion),
            requestId,
            false,
            false
        );
        try (BytesStreamOutput bytesStream = new BytesStreamOutput()) {
            BytesReference headerBytes = headerMessage.serialize(bytesStream);
            return ByteBuffer.wrap(headerBytes.toBytesRef().bytes);
        }
    }

    record BatchTask(Version nodeVersion, Set<String> features, TcpChannel channel, FlightTransportChannel transportChannel, long requestId,
        String action, TransportResponse response, boolean compress, boolean isHandshake, boolean isComplete, boolean isError,
        Exception error, ThreadContext.StoredContext storedContext) implements AutoCloseable {

        @Override
        public void close() {
            if (storedContext != null) {
                storedContext.close();
            }
            if ((isComplete || isError) && transportChannel != null) {
                transportChannel.releaseChannel(isError);
            }
        }
    }
}

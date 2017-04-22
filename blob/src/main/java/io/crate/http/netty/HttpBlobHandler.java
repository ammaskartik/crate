/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.http.netty;

import io.crate.blob.BlobService;
import io.crate.blob.DigestBlob;
import io.crate.blob.RemoteDigestBlob;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.blob.exceptions.MissingHTTPEndpointException;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.blob.v2.BlobsDisabledException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedFile;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpResponseStatus.TEMPORARY_REDIRECT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


public class HttpBlobHandler extends SimpleChannelInboundHandler<Object> {

    private static final String CACHE_CONTROL_VALUE = "max-age=315360000";
    private static final String EXPIRES_VALUE = "Thu, 31 Dec 2037 23:59:59 GMT";
    private static final String BLOBS_ENDPOINT = "/_blobs";
    public static final Pattern BLOBS_PATTERN = Pattern.compile(String.format(Locale.ENGLISH, "^%s/([^_/][^/]*)/([0-9a-f]{40})$", BLOBS_ENDPOINT));
    private static final Logger LOGGER = Loggers.getLogger(HttpBlobHandler.class);

    private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("^bytes=(\\d+)-(\\d*)$");

    private final Matcher blobsMatcher = BLOBS_PATTERN.matcher("");
    private final BlobService blobService;
    private final BlobIndicesService blobIndicesService;
    private final boolean sslEnabled;
    private final String scheme;
    private HttpMessage currentMessage;

    private RemoteDigestBlob digestBlob;
    private ChannelHandlerContext ctx;

    public HttpBlobHandler(BlobService blobService, BlobIndicesService blobIndicesService, boolean sslEnabled) {
        this.blobService = blobService;
        this.blobIndicesService = blobIndicesService;
        this.sslEnabled = sslEnabled;
        this.scheme = sslEnabled ? "https://" : "http://";
    }


    private boolean possibleRedirect(HttpRequest request, String index, String digest) {
        HttpMethod method = request.method();
        if (method.equals(HttpMethod.GET) ||
            method.equals(HttpMethod.HEAD) ||
            (method.equals(HttpMethod.PUT) &&
             HttpUtil.is100ContinueExpected(request))) {
            String redirectAddress;
            try {
                redirectAddress = blobService.getRedirectAddress(index, digest);
            } catch (MissingHTTPEndpointException ex) {
                simpleResponse(HttpResponseStatus.BAD_GATEWAY);
                return true;
            }

            if (redirectAddress != null) {
                LOGGER.trace("redirectAddress: {}", redirectAddress);
                sendRedirect(scheme + redirectAddress);
                return true;
            }
        }
        return false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.ctx = ctx;
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            currentMessage = request;
            String uri = request.uri();

            if (!uri.startsWith(BLOBS_ENDPOINT)) {
                reset();
                ctx.fireChannelRead(msg);
                return;
            }

            Matcher matcher = blobsMatcher.reset(uri);
            if (!matcher.matches()) {
                simpleResponse(HttpResponseStatus.NOT_FOUND);
                reset();
                return;
            }

            handleBlobRequest(request, matcher);

        } /*else if (msg instanceof HttpChunk) {
            if (currentMessage == null) {
                // the chunk is probably from a regular non-blob request.
                ctx.fireChannelRead(msg);
                return;
            }
            HttpChunk chunk = (HttpChunk) msg;
            writeToFile(chunk.getContent(), chunk.isLast(), false);
            if (chunk.isLast()) {
                reset();
            }
        } */ else {
            // Neither HttpMessage or HttpChunk
            ctx.fireChannelRead(msg);
        }
    }

    private void handleBlobRequest(HttpRequest request, Matcher matcher) throws IOException {
        digestBlob = null;
        String index = matcher.group(1);
        String digest = matcher.group(2);

        LOGGER.trace("matches index:{} digest:{}", index, digest);
        LOGGER.trace("HTTPMessage:%n{}", request);

        index = BlobIndex.fullIndexName(index);

        if (possibleRedirect(request, index, digest)) {
            reset();
            return;
        }

        HttpMethod method = request.method();
        if (method.equals(HttpMethod.GET)) {
            get(request, index, digest);
            reset();
        } else if (method.equals(HttpMethod.HEAD)) {
            head(index, digest);
            reset();
        } else if (method.equals(HttpMethod.PUT)) {
            put(request, index, digest);
        } else if (method.equals(HttpMethod.DELETE)) {
            delete(index, digest);
            reset();
        } else {
            simpleResponse(HttpResponseStatus.METHOD_NOT_ALLOWED);
            reset();
        }
    }

    private void reset() {
        currentMessage = null;
    }

    private void sendRedirect(String newUri) {
        HttpResponse response = prepareResponse(TEMPORARY_REDIRECT);
        response.headers().add(HttpHeaderNames.LOCATION, newUri);
        sendResponse(response);
    }

    private HttpResponse prepareResponse(HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        HttpUtil.setContentLength(response, 0);

        if (currentMessage == null || !HttpUtil.isKeepAlive(currentMessage)) {
            response.headers().set(HttpHeaderNames.CONNECTION, "close");
        }
        return response;
    }

    private void simpleResponse(HttpResponseStatus status) {
        sendResponse(prepareResponse(status));
    }

    private void simpleResponse(HttpResponseStatus status, String body) {
        if (body == null) {
            simpleResponse(status);
            return;
        }
        HttpResponse response = prepareResponse(status);
        if (!body.endsWith("\n")) {
            body += "\n";
        }
        HttpUtil.setContentLength(response, body.length());
        ByteBufUtil.writeUtf8(ctx.alloc(), body);
        // FIXME: response.setContent(ChannelBuffers.copiedBuffer(body, CharsetUtil.UTF_8));
        sendResponse(response);
    }

    private void sendResponse(HttpResponse response) {
        ChannelFuture cf = ctx.channel().write(response);
        if (currentMessage != null && !HttpUtil.isKeepAlive(currentMessage)) {
            cf.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ClosedChannelException) {
            LOGGER.trace("channel closed: {}", cause.toString());
            return;
        } else if (cause instanceof IOException) {
            String message = cause.getMessage();
            if (message != null && message.contains("Connection reset by peer")) {
                LOGGER.debug(message);
            } else {
                LOGGER.warn(message, cause);
            }
            return;
        }

        HttpResponseStatus status;
        String body = null;
        if (cause instanceof DigestMismatchException || cause instanceof BlobsDisabledException
            || cause instanceof IllegalArgumentException) {
            status = HttpResponseStatus.BAD_REQUEST;
            body = String.format(Locale.ENGLISH, "Invalid request sent: %s", cause.getMessage());
        } else if (cause instanceof DigestNotFoundException || cause instanceof IndexNotFoundException) {
            status = HttpResponseStatus.NOT_FOUND;
        } else if (cause instanceof EsRejectedExecutionException) {
            status = HttpResponseStatus.TOO_MANY_REQUESTS;
            body = String.format(Locale.ENGLISH, "Rejected execution: %s", cause.getMessage());
        } else {
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            body = String.format(Locale.ENGLISH, "Unhandled exception: %s", cause);
        }
        if (body != null) {
            LOGGER.debug(body);
        }
        simpleResponse(status, body);
    }

    private void head(String index, String digest) throws IOException {

        // this method only supports local mode, which is ok, since there
        // should be a redirect upfront if data is not local

        BlobShard blobShard = localBlobShard(index, digest);
        long length = blobShard.blobContainer().getFile(digest).length();
        if (length < 1) {
            simpleResponse(HttpResponseStatus.NOT_FOUND);
            return;
        }
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        HttpUtil.setContentLength(response, length);
        setDefaultGetHeaders(response);
        sendResponse(response);
    }

    private void get(HttpRequest request, String index, final String digest) throws IOException {
        String range = request.headers().get(HttpHeaderNames.RANGE);
        if (range != null) {
            partialContentResponse(range, request, index, digest);
        } else {
            fullContentResponse(request, index, digest);
        }
    }

    private BlobShard localBlobShard(String index, String digest) {
        return blobIndicesService.localBlobShard(index, digest);
    }

    private void partialContentResponse(String range, HttpRequest request, String index, final String digest)
        throws IOException {
        assert range != null : "Getting partial response but no byte-range is not present.";
        Matcher matcher = CONTENT_RANGE_PATTERN.matcher(range);
        if (!matcher.matches()) {
            LOGGER.warn("Invalid byte-range: {}; returning full content", range);
            fullContentResponse(request, index, digest);
            return;
        }
        BlobShard blobShard = localBlobShard(index, digest);

        final RandomAccessFile raf = blobShard.blobContainer().getRandomAccessFile(digest);
        long start;
        long end;
        try {
            try {
                start = Long.parseLong(matcher.group(1));
                if (start > raf.length()) {
                    LOGGER.warn("416 Requested Range not satisfiable");
                    simpleResponse(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                    raf.close();
                    return;
                }
                end = raf.length() - 1;
                if (!matcher.group(2).equals("")) {
                    end = Long.parseLong(matcher.group(2));
                }
            } catch (NumberFormatException ex) {
                LOGGER.error("Couldn't parse Range Header", ex);
                start = 0;
                end = raf.length();
            }

            HttpResponse response = prepareResponse(HttpResponseStatus.PARTIAL_CONTENT);
            HttpUtil.setContentLength(response, end - start + 1);
            response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + raf.length());
            setDefaultGetHeaders(response);

            ctx.channel().write(response);
            ChannelFuture writeFuture = transferFile(digest, raf, start, end - start + 1);
            if (!HttpUtil.isKeepAlive(request)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /*
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private void fullContentResponse(HttpRequest request, String index, final String digest) throws IOException {
        BlobShard blobShard = localBlobShard(index, digest);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        final RandomAccessFile raf = blobShard.blobContainer().getRandomAccessFile(digest);
        try {
            HttpUtil.setContentLength(response, raf.length());
            setDefaultGetHeaders(response);
            LOGGER.trace("HttpResponse: {}", response);
            Channel channel = ctx.channel();
            channel.write(response);
            ChannelFuture writeFuture;
            if (sslEnabled) {
                // Cannot use zero-copy with HTTPS.
                writeFuture = channel.write(new ChunkedFile(raf, 0, raf.length(), 8192));
            } else {
                writeFuture = transferFile(digest, raf, 0, raf.length());
            }
            if (!HttpUtil.isKeepAlive(request)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /*
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private ChannelFuture transferFile(final String digest, RandomAccessFile raf, long position, long count)
        throws IOException {

        final FileRegion region = new DefaultFileRegion(raf.getChannel(), position, count);
        ChannelFuture writeFuture = ctx.channel().write(region);
        writeFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
                LOGGER.debug("transferFile digest={} progress={} total={}", digest, progress, total);
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) throws Exception {
                region.release();
                LOGGER.trace("transferFile operationComplete");
            }
        });
        return writeFuture;
    }

    private void setDefaultGetHeaders(HttpResponse response) {
        response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");
        response.headers().set(HttpHeaderNames.EXPIRES, EXPIRES_VALUE);
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, CACHE_CONTROL_VALUE);
    }

    private void put(HttpRequest request, String index, String digest) throws IOException {
        if (digestBlob != null) {
            throw new IllegalStateException(
                "received new PUT Request " + HttpRequest.class.getSimpleName() +
                "with existing " + DigestBlob.class.getSimpleName());
        }

        // shortcut check if the file existsLocally locally, so we can immediatly return
        // if (blobService.existsLocally(digest)) {
        //    simpleResponse(HttpResponseStatus.CONFLICT, null);
        //}

        // TODO: Respond with 413 Request Entity Too Large

        digestBlob = blobService.newBlob(index, digest);

        /* FIXME
        if (request.isChunked()) {
            writeToFile(request.getContent(), false, HttpUtil.is100ContinueExpected(request));
        } else {
        */
            writeToFile(((ByteBufHolder) request).content(), true, HttpUtil.is100ContinueExpected(request));
            reset();
        //}
    }

    private void delete(String index, String digest) throws IOException {
        digestBlob = blobService.newBlob(index, digest);
        if (digestBlob.delete()) {
            // 204 for success
            simpleResponse(HttpResponseStatus.NO_CONTENT);
        } else {
            simpleResponse(HttpResponseStatus.NOT_FOUND);
        }
    }

    private void writeToFile(ByteBuf input, boolean last, final boolean continueExpected) throws IOException {
        if (digestBlob == null) {
            throw new IllegalStateException("digestBlob is null in writeToFile");
        }

        RemoteDigestBlob.Status status = digestBlob.addContent(input, last);
        HttpResponseStatus exitStatus = null;
        switch (status) {
            case FULL:
                exitStatus = HttpResponseStatus.CREATED;
                break;
            case PARTIAL:
                // tell the client to continue
                if (continueExpected) {
                    ctx.write(new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.CONTINUE));
                }
                return;
            case MISMATCH:
                exitStatus = HttpResponseStatus.BAD_REQUEST;
                break;
            case EXISTS:
                exitStatus = HttpResponseStatus.CONFLICT;
                break;
            case FAILED:
                exitStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                break;
        }

        assert exitStatus != null : "exitStatus should not be null";
        LOGGER.trace("writeToFile exit status http:{} blob: {}", exitStatus, status);
        simpleResponse(exitStatus);
    }
}

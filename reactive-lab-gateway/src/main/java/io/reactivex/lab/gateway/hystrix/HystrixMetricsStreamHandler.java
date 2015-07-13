package io.reactivex.lab.gateway.hystrix;

import com.netflix.hystrix.HystrixCommandMetrics;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import rx.Observable;
import rx.exceptions.Exceptions;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.Charset.*;

/**
 * Streams Hystrix metrics in Server Sent Event (SSE) format. RxNetty application handlers shall
 * be wrapped by this handler. It transparently intercepts HTTP requests at a configurable path
 * (default "/hystrix.stream"), and sends unbounded SSE streams back to the client. All other requests
 * are transparently forwarded to the application handlers.
 * <p/>
 * For RxNetty client tapping into SSE stream: remember to use unpooled HTTP connections. If not, the pooled HTTP
 * connection will not be closed on unsubscribe event and the event stream will continue to flow towards the client
 * (unless the client is shutdown).
 */
public class HystrixMetricsStreamHandler implements RequestHandler<ByteBuf, ByteBuf> {

    public static final String DEFAULT_HYSTRIX_PREFIX = "/hystrix.stream";

    public static final int DEFAULT_INTERVAL = 2000;

    private static final byte[] HEADER = "data: ".getBytes(defaultCharset());
    private static final byte[] FOOTER = { 10, 10 };

    private final String hystrixPrefix;
    private final long interval;
    private final RequestHandler<ByteBuf, ByteBuf> appHandler;

    public HystrixMetricsStreamHandler(RequestHandler<ByteBuf, ByteBuf> appHandler) {
        this(DEFAULT_HYSTRIX_PREFIX, DEFAULT_INTERVAL, appHandler);
    }

    public HystrixMetricsStreamHandler(String hystrixPrefix, long intervalInMillis) {
        this.hystrixPrefix = hystrixPrefix;
        this.interval = intervalInMillis;
        this.appHandler = (HttpServerRequest<ByteBuf> request, HttpServerResponse<ByteBuf> response) ->
                response.writeString(Observable.just("Only supported path is /" + hystrixPrefix)
                );
    }

    public HystrixMetricsStreamHandler(String hystrixPrefix, long interval,
                                       RequestHandler<ByteBuf, ByteBuf> appHandler) {
        this.hystrixPrefix = hystrixPrefix;
        this.interval = interval;
        this.appHandler = appHandler;
    }


    @Override
    public Observable<Void> handle(HttpServerRequest<ByteBuf> request, HttpServerResponse<ByteBuf> response) {
        if (request.getUri().endsWith(hystrixPrefix)) {
            return handleHystrixRequest(response);
        }
        return appHandler.handle(request, response);
    }

    private Observable<Void> handleHystrixRequest(final HttpServerResponse<ByteBuf> response) {
        return response.addHeader("Content-Type", "text/event-stream;charset=UTF-8")
                       .addHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate")
                       .addHeader("Pragma", "no-cache")
                       .writeBytes(Observable.interval(0, interval, TimeUnit.MILLISECONDS)
                                             .flatMap(aTick -> {
                                                 Observable<byte[]> threadPoolMetrics =
                                                         Observable.from(HystrixThreadPoolMetrics.getInstances())
                                                                   .flatMap(threadPoolMetric ->
                                                                                    Observable.just(HEADER,
                                                                                                    toJson(threadPoolMetric),
                                                                                                    FOOTER)
                                                                   );

                                                 Observable<byte[]> cmdMetrics =
                                                         Observable.from(HystrixCommandMetrics.getInstances())
                                                                   .flatMap(cmdMetric ->
                                                                                    Observable.just(HEADER,
                                                                                                    toJson(cmdMetric),
                                                                                                    FOOTER));

                                                 return cmdMetrics.concatWith(threadPoolMetrics);

                                             }));
    }

    private static byte[] toJson(HystrixCommandMetrics commandMetrics) {
        try {
            return JsonMapper.toJson(commandMetrics).getBytes(Charset.defaultCharset());
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    private static byte[] toJson(HystrixThreadPoolMetrics threadPoolMetrics) {
        try {
            return JsonMapper.toJson(threadPoolMetrics).getBytes(Charset.defaultCharset());
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }
}
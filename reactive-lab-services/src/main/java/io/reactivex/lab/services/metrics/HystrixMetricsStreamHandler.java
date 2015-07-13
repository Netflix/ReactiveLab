package io.reactivex.lab.services.metrics;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import rx.Observable;

import java.io.IOException;
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

    private Metrics metrics;

    public HystrixMetricsStreamHandler(Metrics metrics, String hystrixPrefix, long interval,
                                       RequestHandler<ByteBuf, ByteBuf> appHandler) {
        this.metrics = metrics;
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
                                                 try {
                                                     return Observable.just(HEADER,
                                                                            JsonMapper.toJson(metrics)
                                                                                      .getBytes(defaultCharset()),
                                                                            FOOTER);
                                                 } catch (IOException e) {
                                                     return Observable.error(e);
                                                 }
                                             }));
    }
}
package io.reactivex.lab.edge.nf;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsPoller;

public class EdgeServer {

    public static void main(String... args) {
        // hystrix stream => http://localhost:9999
        startHystrixMetricsStream();

        // start web services => http://localhost:8080
        RxNetty.createHttpServer(8080, (request, response) -> {
            System.out.println("Server => Request: " + request.getPath());
            try {
                if (request.getPath().equals("/device/home")) {
                    return EndpointForDeviceHome.getInstance().handle(request, response).onErrorResumeNext(error -> {
                        error.printStackTrace();
                        return writeError(request, response, "Failed: " + error.getMessage());
                    });
                } else if (request.getPath().endsWith(".js")) {
                    System.out.println("Server => Javascript Request: " + request.getPath());
                    return JavascriptRuntime.getInstance().handleRequest(request, response);
                } else {
                    return writeError(request, response, "Unknown path: " + request.getPath());
                }
            } catch (Throwable e) {
                System.err.println("Server => Error [" + request.getPath() + "] => " + e);
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return response.writeStringAndFlush("Error 500: Bad Request\n" + e.getMessage() + "\n");
            }
        }, PipelineConfigurators.<ByteBuf> sseServerConfigurator()).startAndWait();
    }

    private static void startHystrixMetricsStream() {
        RxNetty.createHttpServer(9999, (request, response) -> {
            System.out.println("Start Hystrix Stream");
            response.getHeaders().add("content-type", "text/event-stream");
            return Observable.create(new Observable.OnSubscribe<Void>() {
                @Override
                public void call(Subscriber<? super Void> s) {
                    s.add(streamPoller.subscribe(json -> {
                        response.writeAndFlush(new ServerSentEvent("", "data", json));
                    }, new Action1<Throwable>() {
                        @Override
                        public void call(Throwable error) {
                            s.onError(error);
                        }
                    }));

                    s.add(Observable.interval(1000, TimeUnit.MILLISECONDS).flatMap(
                            new Func1<Long, Observable<? extends Void>>() {
                                @Override
                                public Observable<? extends Void> call(Long n) {
                                    return response.writeAndFlush(new ServerSentEvent("", "ping", ""))
                                                   .onErrorReturn(new Func1<Throwable, Void>() {
                                                       @Override
                                                       public Void call(Throwable e) {
                                                           System.out.println(
                                                                   "Connection closed, unsubscribing from Hystrix Stream");
                                                           s.unsubscribe();
                                                           return null;
                                                       }
                                                   });
                                }
                            }).subscribe());
                }
            });
        }, PipelineConfigurators.<ByteBuf> sseServerConfigurator()).start();
    }

    final static Observable<String> streamPoller = Observable.create(new Observable.OnSubscribe<String>() {
        @Override
        public void call(Subscriber<? super String> s) {
            try {
                System.out.println("Start Hystrix Metric Poller");
                HystrixMetricsPoller poller = new HystrixMetricsPoller(
                        new HystrixMetricsPoller.MetricsAsJsonPollerListener() {
                            @Override
                            public void handleJsonMetric(String json) {
                                s.onNext(json);
                            }
                        }, 1000);
                s.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("Shutdown Hystrix Stream");
                        poller.shutdown();
                    }
                }));
                poller.start();
            } catch (Exception e) {
                s.onError(e);
            }
        }
    }).publish().refCount();

    public static Observable<Void> writeError(HttpServerRequest<?> request, HttpServerResponse<?> response, String message) {
        System.err.println("Server => Error [" + request.getPath() + "] => " + message);
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
        return response.writeStringAndFlush("Error 500: " + message + "\n");
    }
}

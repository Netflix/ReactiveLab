package io.reactivex.lab.gateway;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.Eurekas;
import com.netflix.eureka2.client.resolver.ServerResolvers;
import com.netflix.hystrix.HystrixRequestLog;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.lab.gateway.clients.ClientRegistry;
import io.reactivex.lab.gateway.clients.ClientRegistryImpl;
import io.reactivex.lab.gateway.hystrix.HystrixMetricsStreamHandler;
import io.reactivex.lab.gateway.routes.RouteForDeviceHome;
import io.reactivex.lab.gateway.routes.mock.TestRouteBasic;
import io.reactivex.lab.gateway.routes.mock.TestRouteWithHystrix;
import io.reactivex.lab.gateway.routes.mock.TestRouteWithSimpleFaultTolerance;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.Observable;

public class StartGatewayServer {

    public static void main(String... args) {
        EurekaInterestClient eurekaInterestClient = Eurekas.newInterestClientBuilder()
                                                           .withServerResolver(ServerResolvers.fromHostname("127.0.0.1")
                                                                                              .withPort(7005))
                                                           .build();

        final ClientRegistry clientRegistry = new ClientRegistryImpl(eurekaInterestClient);

        // hystrix stream => http://localhost:9999
        startHystrixMetricsStream();

        System.out.println("Server => Starting at http://localhost:8080/");
        System.out.println("   Sample URLs: ");
        System.out.println("      - http://localhost:8080/device/home?userId=123");
        System.out.println("----------------------------------------------------------------");

        // start web services => http://localhost:8080
        HttpServer.newServer(8080)
                  .start((request, response) -> {
                      if (request.getUri().contains("favicon.ico")) {
                          return Observable.empty();
                      }

                      return Observable.defer(() -> {
                          HystrixRequestContext.initializeContext();
                          try {
                              return handleRoutes(clientRegistry, request, response);
                          } catch (Throwable e) {
                              System.err.println("Server => Error [" + request.getUri() + "] => " + e);
                              response.setStatus(HttpResponseStatus.BAD_REQUEST);
                              return response
                                      .writeString(Observable.just("Error 500: Bad Request\n" + e.getMessage() + "\n"));
                          }
                      }).onErrorResumeNext(error -> {
                          System.err.println("Server => Error: " + error.getMessage());
                          error.printStackTrace();
                          return writeError(request, response, "Failed: " + error.getMessage());
                      }).doOnTerminate(() -> {
                          if (HystrixRequestContext.isCurrentThreadInitialized()) {
                              System.out.println("Server => Request [" + request.getUri() + "] => " +
                                                 HystrixRequestLog.getCurrentRequest().getExecutedCommandsAsString());
                              HystrixRequestContext.getContextForCurrentThread().shutdown();
                          } else {
                              System.err.println(
                                      "HystrixRequestContext not initialized for thread: " + Thread.currentThread());
                          }
                      });
                  }).awaitShutdown();
    }

    /**
     * Hard-coded route handling.
     */
    private static Observable<Void> handleRoutes(ClientRegistry clientRegistry, HttpServerRequest<ByteBuf> request,
                                                 HttpServerResponse<ByteBuf> response) {
        if (request.getUri().startsWith("/device/home")) {
            return RouteForDeviceHome.handle(clientRegistry, request, response);
        } else if (request.getUri().startsWith("/testBasic")) {
            return TestRouteBasic.handle(request, response);
        } else if (request.getUri().startsWith("/testWithSimpleFaultTolerance")) {
            return TestRouteWithSimpleFaultTolerance.handle(request, response);
        } else if (request.getUri().startsWith("/testWithHystrix")) {
            return TestRouteWithHystrix.handle(clientRegistry, request, response);
        } else {
            return writeError(request, response, "Unknown path: " + request.getUri());
        }
    }

    private static void startHystrixMetricsStream() {
        HystrixMetricsStreamHandler hystrixMetricsStreamHandler = new HystrixMetricsStreamHandler("/", 1000);
        HttpServer.newServer(9999)
                  .start((request, response) -> {
                      System.out.println("Server => Start Hystrix Stream at http://localhost:9999");
                      return hystrixMetricsStreamHandler.handle(request, response);
                  });
    }

    public static Observable<Void> writeError(HttpServerRequest<?> request, HttpServerResponse<ByteBuf> response, String message) {
        System.err.println("Server => Error [" + request.getUri() + "] => " + message);
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
        return response.writeString(Observable.just("Error 500: " + message));
    }

}

package io.reactivex.lab.services.impls;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.EurekaRegistrationClient;
import com.netflix.eureka2.client.registration.RegistrationObservable;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.registry.instance.ServicePort;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.logging.LogLevel;
import io.reactivex.lab.services.metrics.HystrixMetricsStreamHandler;
import io.reactivex.lab.services.metrics.Metrics;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.Observable;
import rx.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/**
 * Common base for the service impls
 */
public abstract class AbstractMiddleTierService {

    private HttpServer<ByteBuf, ByteBuf> server;
    protected final String eurekaVipAddress;
    protected final EurekaInterestClient interestClient;
    protected final EurekaRegistrationClient registrationClient;
    private final Metrics metrics;
    private Subscription registrationSubscription;

    protected AbstractMiddleTierService(String eurekaVipAddress, EurekaRegistrationClient registrationClient) {
        this(eurekaVipAddress, null, registrationClient);
    }

    protected AbstractMiddleTierService(String eurekaVipAddress, EurekaInterestClient interestClient,
                                        EurekaRegistrationClient registrationClient) {
        this.eurekaVipAddress = eurekaVipAddress;
        this.interestClient = interestClient;
        this.registrationClient = registrationClient;
        this.metrics = new Metrics(eurekaVipAddress);
    }

    private HttpServer<ByteBuf, ByteBuf> startServer(int port) {
        System.out.println("Start " + getClass().getSimpleName() + " on port: " + port);

        // declare handler chain (wrapped in Hystrix)
        HystrixMetricsStreamHandler handlerChain
          = new HystrixMetricsStreamHandler(metrics, "/hystrix.stream", 1000, (request, response) -> {
            try {
                long startTime = System.currentTimeMillis();
                return handleRequest(request, response)
                        .doOnCompleted(() -> metrics.getRollingPercentile()
                                                    .addValue((int) (System.currentTimeMillis() - startTime)))
                        .doOnCompleted(() -> metrics.getRollingNumber().add(Metrics.EventType.SUCCESS, 1))
                        .doOnError(t -> metrics.getRollingNumber().add(Metrics.EventType.FAILURE, 1));
            } catch (Throwable e) {
                e.printStackTrace();
                System.err.println("Server => Error [" + request.getUri() + "] => " + e);
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return response.writeString(Observable.just("data: Error 500: Bad Request\n" + e.getMessage() + "\n"));
            }
        });

        return HttpServer.newServer(port).enableWireLogging(LogLevel.ERROR).start(handlerChain::handle);
    }

    public void start(int port) {
        server = startServer(port);
        InstanceInfo instanceInfo = createInstanceInfo(port);
        RegistrationObservable registrationObservable = registrationClient.register(Observable.just(instanceInfo));
        registrationSubscription = registrationObservable.subscribe();
        registrationObservable.initialRegistrationResult().toBlocking().firstOrDefault(null);
    }

    public void shutdown() {
        registrationSubscription.unsubscribe();
        server.shutdown();
    }

    protected abstract Observable<Void> handleRequest(HttpServerRequest<?> request, HttpServerResponse<ByteBuf> response);

    protected InstanceInfo createInstanceInfo(int port) {
        final HashSet<ServicePort> ports = new HashSet<>(Collections.singletonList(new ServicePort(port, false)));

        String hostAddress;
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            hostAddress = "unknown-" + UUID.randomUUID();
        }

        return new InstanceInfo.Builder()
                .withId(hostAddress + "-" + port)
                .withApp("reactive-lab")
                .withStatus(InstanceInfo.Status.UP)
                .withVipAddress(eurekaVipAddress)
                .withPorts(ports)
                .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
                .build();
    }

    protected static Observable<Void> writeError(HttpServerRequest<?> request, HttpServerResponse<?> response, String message) {
        System.err.println("Server => Error [" + request.getUri() + "] => " + message);
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
        return response.writeString(Observable.just("Error 500: " + message + "\n"));
    }

    protected static int getParameter(HttpServerRequest<?> request, String key, int defaultValue) {
        List<String> v = request.getQueryParameters().get(key);
        if (v == null || v.size() != 1) {
            return defaultValue;
        } else {
            return Integer.parseInt(String.valueOf(v.get(0)));
        }
    }

    public Metrics getMetrics() {
        return metrics;
    }
}

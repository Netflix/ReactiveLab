package io.reactivex.lab.tutorial;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.EurekaRegistrationClient;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.instance.NetworkAddress;
import com.netflix.eureka2.registry.instance.ServicePort;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import netflix.ocelli.Instance;
import netflix.ocelli.eureka2.Eureka2InterestManager;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.reactivex.lab.tutorial.ClientServerWithDiscovery.*;

/**
 * This example builds over the {@link ClientServerWithDiscovery} example by adding the load balancer Ocelli.
 * So, in comparison to {@link ClientServerWithDiscovery} which directly fetches the server instance information from
 * eureka, this example chooses an optimal host from the load balancer.
 *
 * In order to be a standalone example, this also starts an embedded eureka server.
 */
public class ClientServerWithLoadBalancer {

    public static void main(String[] args) throws Exception {

        final int eurekaReadServerPort = 7005;
        final int eurekaWriteServerPort = 7006;

        /**
         * Starts an embedded eureka server with the defined read and write ports.
         */
        // TODO: Till Eureka2 moves to RxNetty 0.5.X, we can embedd eureka write server
        //ClientServerWithDiscovery.startEurekaServer(eurekaReadServerPort, eurekaWriteServerPort);

        /**
         * Create eureka client with the same read and write ports for the embedded eureka server.
         */
        EurekaRegistrationClient eurekaRegistrationClient = createEurekaRegistrationClient(eurekaWriteServerPort);
        EurekaInterestClient eurekaInterestClient = createEurekaInterestClient(eurekaReadServerPort);

        /**
         * Reuse {@link ClientServer} example to start an RxNetty server on the passed port.
         */
        HttpServer<ByteBuf, ByteBuf> server = ClientServer.startServer(8089);

        /**
         * Register the server started above with eureka using a unique virtual IP address (VIP).
         * Eureka uses VIPs to group homogeneous instances of a service together, so that they can be used by clients,
         * interchangeably.
         */
        String vipAddress = "mock_server-" + server.getServerPort();
        ClientServerWithDiscovery.registerWithEureka(server.getServerPort(), eurekaRegistrationClient, vipAddress);

        /**
         * Using the eureka client, create an Ocelli Host event stream.
         * Ocelli, uses this host stream to know about the available hosts.
         */
        Observable<Instance<SocketAddress>> eurekaHostSource = createEurekaHostStream(eurekaInterestClient,
                                                                                      vipAddress);

        /**
         * Instead of directly using the host and port from eureka as in example {@link ClientServerWithDiscovery},
         * choose a host from the load balancer.
         */
        createRequestFromLB(eurekaHostSource)
                /* Block till you get the response. In a real world application, one should not be blocked but chained
                 * into a response to the caller. */
                .toBlocking()
                /**
                 * Print each content of the response.
                 */
                .forEach(System.out::println);
    }

    public static Observable<String> createRequestFromLB(Observable<Instance<SocketAddress>> eurekaHostSource) {

        return HttpClient.newClient(HttpLoadBalancer.<ByteBuf, ByteBuf>roundRobin(eurekaHostSource).toConnectionProvider())
                /* Submit an HTTP GET request with uri "/hello" */
                .createGet("/hello")
                .retryWhen(errStream -> errStream.flatMap(err -> {
                    if (err instanceof NoSuchElementException) {
                        System.out.println("No hosts available, retrying after 10 seconds.");
                        return Observable.timer(10, TimeUnit.SECONDS);
                    }
                    return Observable.error(err);
                }))
                /* Print the HTTP initial line and headers. Return the content.*/
                .flatMap(response -> {
                    /**
                     * Printing the HTTP headers.
                     */
                    System.out.println(response);
                    return response.getContent();
                })
                /* Convert the ByteBuf for each content chunk into a string. */
                .map(byteBuf -> byteBuf.toString(Charset.defaultCharset()));
   }

    public static Observable<Instance<SocketAddress>> createEurekaHostStream(EurekaInterestClient eurekaClient,
                                                                             String vipAddress) {

        Eureka2InterestManager eureka2InterestManager = new Eureka2InterestManager(eurekaClient);

        /**
         * Eureka client caches data streams from the server to decouple users from blips in connection between client
         * & server. The below call makes sure that the data is fetched by the client and hence it is guaranteed to be
         * available when the load balancer queries for the same.
         *
         * This is just to make sure that the demo runs without inducing artificial delays in the code.
         * A real application should not advertise itself healthy in eureka till it has all the information it requires
         * in order to run gracefully. In this case, the list of target servers from eureka.
         */
        eurekaClient.forInterest(Interests.forFullRegistry()).take(1).toBlocking().first();// Warm up eureka data

        return eureka2InterestManager.forInterest(Interests.forVips(vipAddress), instanceInfo -> {
            /**
             * Filtering out all non-IPv4 addresses.
             */
            String ipAddress = instanceInfo.getDataCenterInfo()
                                           .getAddresses().stream()
                                           .filter(na -> na.getProtocolType() == NetworkAddress.ProtocolType.IPv4)
                                           .collect(Collectors.toList()).get(0).getIpAddress();
            HashSet<ServicePort> servicePorts = instanceInfo.getPorts();
            ServicePort portToUse = servicePorts.iterator().next();
            return new InetSocketAddress(ipAddress, portToUse.getPort());
        });
    }
}

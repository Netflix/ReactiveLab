package io.reactivex.lab.tutorial;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.client.EurekaRegistrationClient;
import com.netflix.eureka2.client.Eurekas;
import com.netflix.eureka2.client.registration.RegistrationObservable;
import com.netflix.eureka2.client.resolver.ServerResolvers;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.registry.instance.NetworkAddress;
import com.netflix.eureka2.registry.instance.ServicePort;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServer;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This example builds over the basic {@link ClientServer} example by adding registration to eureka server.
 *
 * In order to be a standalone example, this also starts an embedded eureka server.
 */
public class ClientServerWithDiscovery {

    public static void main(String[] args) throws Exception {

        final int eurekaReadServerPort = 7005;
        final int eurekaWriteServerPort = 7006;

        /**
         * Starts an embedded eureka server with the defined read and write ports.
         */
        // TODO: Till Eureka2 moves to RxNetty 0.5.X, we can embedd eureka write server
        //startEurekaServer(eurekaReadServerPort, eurekaWriteServerPort);

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
        registerWithEureka(server.getServerPort(), eurekaRegistrationClient, vipAddress);

        /**
         * Retrieve the instance information of the registered server from eureka.
         * This is to demonstrate how to use eureka to fetch information about any server in your deployment.
         * In order to fetch information from eureka, one MUST know the VIP address of the server before hand.
         */
        InstanceInfo serverInfo = getServerInfo(eurekaInterestClient, vipAddress);

        /**
         * Retrieve IPAddress and port information from the instance information returned from eureka.
         */
        InetSocketAddress host = getServerHostAndPort(serverInfo);

        /**
         * Reuse {@link ClientServer} example to create an HTTP request to the server retrieved from eureka.
         */
        ClientServer.createRequest(host.getHostString(), host.getPort())
                    /* Block till you get the response. In a real world application, one should not be blocked but chained
                     * into a response to the caller. */
                    .toBlocking()
                    /**
                     * Print each content of the response.
                     */
                    .forEach(System.out::println);
    }

    public static InetSocketAddress getServerHostAndPort(InstanceInfo serverInfo) {
        String ipAddress = serverInfo.getDataCenterInfo()
                                       .getAddresses().stream()
                                       .filter(na -> na.getProtocolType() == NetworkAddress.ProtocolType.IPv4)
                                       .collect(Collectors.toList()).get(0).getIpAddress();

        Integer port = serverInfo.getPorts().iterator().next().getPort();

        return new InetSocketAddress(ipAddress, port);
    }

    public static InstanceInfo getServerInfo(EurekaInterestClient eurekaClient, String vipAddress) {
        return eurekaClient.forInterest(Interests.forVips(vipAddress))
                .filter(notification -> notification.getKind() == ChangeNotification.Kind.Add) /* Filter all notifications which are not add */
                .map(ChangeNotification::getData) /*Retrieve only the data*/
                .toBlocking()
                .first();
    }

    public static void registerWithEureka(int serverPort, EurekaRegistrationClient client, String vipAddress) {
        final HashSet<ServicePort> ports = new HashSet<>(Collections.singletonList(new ServicePort(serverPort, false)));

        InstanceInfo instance = new InstanceInfo.Builder()
                .withId(String.valueOf(serverPort))
                .withApp("mock_server")
                .withStatus(InstanceInfo.Status.UP)
                .withVipAddress(vipAddress)
                .withPorts(ports)
                .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
                .build();

        TestSubscriber<Void> subscriber = new TestSubscriber<>();

        RegistrationObservable register = client.register(Observable.just(instance));
        register.subscribe();
        register.initialRegistrationResult().subscribe(subscriber);

        System.out.println("Waiting for eureka registration to be completed.");

        try {
            subscriber.awaitTerminalEvent(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.out.println("Registration did not complete after 1 minute. Bailing out.");
            System.exit(-1);
        }
        subscriber.assertNoErrors();

        System.out.println("Registered with eureka server.");
    }

    public static EurekaRegistrationClient createEurekaRegistrationClient(int writeServerPort) {
        return Eurekas.newRegistrationClientBuilder()
                      .withServerResolver(ServerResolvers.fromHostname("127.0.0.1").withPort(writeServerPort))
                      .build();
    }

    public static EurekaInterestClient createEurekaInterestClient(int readServerPort) {
        return Eurekas.newInterestClientBuilder()
                      .withServerResolver(ServerResolvers.fromHostname("127.0.0.1").withPort(readServerPort))
                      .build();
    }

}

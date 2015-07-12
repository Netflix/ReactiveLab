package io.reactivex.lab.gateway.clients;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.registry.instance.ServicePort;
import com.netflix.numerus.NumerusProperty;
import com.netflix.numerus.NumerusRollingNumber;
import com.netflix.numerus.NumerusRollingNumberEvent;
import com.netflix.numerus.NumerusRollingPercentile;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.http.client.HttpClient;
import netflix.ocelli.Instance;
import netflix.ocelli.eureka2.Eureka2InterestManager;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import netflix.ocelli.rxnetty.protocol.http.WeightedHttpClientListener;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.netflix.eureka2.registry.instance.NetworkAddress.ProtocolType.*;
import static com.netflix.numerus.NumerusProperty.Factory.asProperty;
import static io.reactivex.lab.gateway.clients.ClientRegistryImpl.HostMetrics.EventType.*;

/**
 * A registry of clients for all middle-tier services.
 */
public final class ClientRegistryImpl implements ClientRegistry {

    private final EnumMap<Services, HttpClient<ByteBuf, ByteBuf>> serviceClients;

    public ClientRegistryImpl(final EurekaInterestClient interestClient) {
        Eureka2InterestManager eureka2InterestManager = new Eureka2InterestManager(interestClient);
        serviceClients = new EnumMap<>(Services.class);
        for (Services service : Services.values()) {
            Interest<InstanceInfo> interest = Interests.forVips(service.getVipAddress());
            Observable<Instance<SocketAddress>> hosts =
                    eureka2InterestManager.forInterest(interest, instanceInfo -> {
                        /**
                         * Filtering out all non-IPv4 addresses.
                         */
                        String ipAddress = instanceInfo.getDataCenterInfo()
                                                       .getAddresses().stream()
                                                       .filter(na -> na.getProtocolType() == IPv4)
                                                       .collect(Collectors.toList())
                                                       .get(0)
                                                       .getIpAddress();

                        HashSet<ServicePort> servicePorts = instanceInfo.getPorts();
                        ServicePort portToUse = servicePorts.iterator().next();

                        return new InetSocketAddress(ipAddress, portToUse.getPort());
                    });

            HttpLoadBalancer<ByteBuf, ByteBuf> loadBalancer =
                    HttpLoadBalancer.choiceOfTwo(hosts, failureListener -> new WeightedHttpClientListener() {

                        private final HostMetrics hostMetrics = new HostMetrics();

                        @Override
                        public int getWeight() {
                            /*Higher 95 percentile latency, lower weight*/
                            return Integer.MAX_VALUE - hostMetrics.getRollingLatencyPercentile().getPercentile(95);
                        }

                        @Override
                        public void onRequestWriteComplete(long duration, TimeUnit timeUnit) {
                            hostMetrics.getRollingCounter().add(PENDING_REQUESTS, 1);
                        }

                        @Override
                        public void onResponseReceiveComplete(long duration, TimeUnit timeUnit) {
                            hostMetrics.getRollingCounter().add(PENDING_REQUESTS, -1);
                        }

                        @Override
                        public void onResponseHeadersReceived(int responseCode, long duration, TimeUnit timeUnit) {
                            if (responseCode == 503) {
                                hostMetrics.getRollingCounter().add(SERVICE_UNAVAILABLE, 1);
                                long unavailableInCurrentWindow =
                                        hostMetrics.getRollingCounter().getValueOfLatestBucket(SERVICE_UNAVAILABLE);
                                if (unavailableInCurrentWindow > 3) {
                                    /*If we receive more than 3 throttled responses, then quarantine host*/
                                    failureListener.quarantine(1, TimeUnit.MINUTES);
                                }
                            }
                            hostMetrics.getRollingLatencyPercentile().addValue((int) duration);
                        }

                        @Override
                        public void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
                            hostMetrics.getRollingCounter().add(CONNECT_FAILURES, 1);
                            long connectFailedInCurrentWindow =
                                    hostMetrics.getRollingCounter().getValueOfLatestBucket(CONNECT_FAILURES);
                            if (connectFailedInCurrentWindow > 2) {
                                /*If more than two connect failed in the current window, then consider host unavailable.*/
                                failureListener.remove();
                            }
                        }
                    });

            serviceClients.put(service, HttpClient.newClient(loadBalancer.toConnectionProvider()));
        }
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getMockServiceClient() {
        return serviceClients.get(Services.Mock);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getBookmarksServiceClient() {
        return serviceClients.get(Services.Bookmarks);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getGeoServiceClient() {
        return serviceClients.get(Services.Geo);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getPersonalizedCatalogServiceClient() {
        return serviceClients.get(Services.PersonalizedCatalog);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getRatingsServiceClient() {
        return serviceClients.get(Services.Ratings);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getSocialServiceClient() {
        return serviceClients.get(Services.Social);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getUserServiceClient() {
        return serviceClients.get(Services.User);
    }

    @Override
    public HttpClient<ByteBuf, ByteBuf> getVideoMetadataServiceClient() {
        return serviceClients.get(Services.VideoMetadata);
    }

    public static final class HostMetrics {

        private static final NumerusProperty<Integer> latency_timeInMilliseconds = asProperty(60000);
        private static final NumerusProperty<Integer> latency_numberOfBuckets = asProperty(12); // 12 buckets at 5000ms each
        private static final NumerusProperty<Integer> latency_bucketDataLength = asProperty(1000);
        private static final NumerusProperty<Boolean> latency_enabled = asProperty(true);

        private static final NumerusProperty<Integer> count_timeInMilliseconds = asProperty(10000);
        private static final NumerusProperty<Integer> count_numberOfBuckets = asProperty(10); // 11 buckets at 1000ms each

        private final NumerusRollingPercentile p = new NumerusRollingPercentile(latency_timeInMilliseconds,
                                                                                latency_numberOfBuckets,
                                                                                latency_bucketDataLength,
                                                                                latency_enabled);
        private final NumerusRollingNumber n = new NumerusRollingNumber(EventType.BOOTSTRAP,
                                                                        count_timeInMilliseconds,
                                                                        count_numberOfBuckets);

        public NumerusRollingPercentile getRollingLatencyPercentile() {
            return p;
        }

        public NumerusRollingNumber getRollingCounter() {
            return n;
        }

        public enum EventType implements NumerusRollingNumberEvent {

            BOOTSTRAP(1), SUCCESS(1), FAILURE(1), SERVICE_UNAVAILABLE(1), PENDING_REQUESTS(1), CONNECT_FAILURES(1);

            private final int type;

            EventType(int type) {
                this.type = type;
            }

            public boolean isCounter() {
                return type == 1;
            }

            public boolean isMaxUpdater() {
                return type == 2;
            }

            @Override
            public EventType[] getValues() {
                return values();
            }

        }
    }
}

package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.GeoCommand.GeoIP;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.List;
import java.util.Map;

public class GeoCommand extends HystrixObservableCommand<GeoIP> {

    private final List<String> ips;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public GeoCommand(List<String> ips, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("GeoIP"));
        this.ips = ips;
        client = clientRegistry.getGeoServiceClient();
    }

    @Override
    protected Observable<GeoIP> construct() {
        return client.createGet("/geo?" + UrlGenerator.generate("ip", ips))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(GeoIP::fromSse));
    }

    public static class GeoIP {

        private final Map<String, Object> data;

        private GeoIP(Map<String, Object> data) {
            this.data = data;
        }

        public Map<String, Object> getData() {
            return data;
        }

        public static GeoIP fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new GeoIP(SimpleJson.jsonToMap(json));
        }
    }
}

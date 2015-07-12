package io.reactivex.lab.services.impls;

import com.netflix.eureka2.client.EurekaRegistrationClient;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.services.common.SimpleJson;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GeoService extends AbstractMiddleTierService {

    public GeoService(EurekaRegistrationClient registrationClient) {
        super("reactive-lab-geo-service", registrationClient);
    }

    @Override
    protected Observable<Void> handleRequest(HttpServerRequest<?> request, HttpServerResponse<ByteBuf> response) {

        List<String> ips = request.getQueryParameters().get("ip");
        if (ips == null || ips.size() == 0) {
            return writeError(request, response, "At least one parameter of 'ip' must be included.");
        }

        return response.transformToServerSentEvents()
                       .writeAndFlushOnEach(Observable.from(ips)
                                                      .map(ip -> {
                                                          Map<String, Object> ip_data = new HashMap<>();
                                                          ip_data.put("country_code", "GB");
                                                          ip_data.put("longitude", "-0.13");
                                                          ip_data.put("latitude", "51.5");
                                                          return ServerSentEvent
                                                                  .withData(SimpleJson.mapToJson(ip_data));
                                                      })
                                                      .delay(10, TimeUnit.MILLISECONDS)
                       );
    }
}

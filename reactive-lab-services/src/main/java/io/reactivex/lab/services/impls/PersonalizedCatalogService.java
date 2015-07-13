package io.reactivex.lab.services.impls;

import com.netflix.eureka2.client.EurekaRegistrationClient;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.services.common.SimpleJson;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PersonalizedCatalogService extends AbstractMiddleTierService {

    public PersonalizedCatalogService(EurekaRegistrationClient registrationClient) {
        super("reactive-lab-personalized-catalog-service", registrationClient);
    }

    @Override
    protected Observable<Void> handleRequest(HttpServerRequest<?> request, HttpServerResponse<ByteBuf> response) {
        List<String> userIds = request.getQueryParameters().get("userId");
        if (userIds == null || userIds.size() == 0) {
            return writeError(request, response, "At least one parameter of 'userId' must be included.");
        }

        return response.transformToServerSentEvents()
                       .writeAndFlushOnEach(Observable.from(userIds)
                                                      .map(userId -> {
                                                          Map<String, Object> uData = new HashMap<>();
                                                          uData.put("user_id", userId);
                                                          uData.put("list_title",
                                                                    "Really quirky and over detailed list title!");
                                                          uData.put("other_data", "goes_here");
                                                          uData.put("videos", Arrays.asList(12345, 23456, 34567, 45678,
                                                                                            56789, 67890));
                                                          return ServerSentEvent.withData(SimpleJson.mapToJson(uData));
                                                      })
                                                      .delay(((long) (Math.random() * 100) + 20), TimeUnit.MILLISECONDS)
                       );
    }
}

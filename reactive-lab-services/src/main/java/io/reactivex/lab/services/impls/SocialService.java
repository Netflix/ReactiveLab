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

public class SocialService extends AbstractMiddleTierService {

    public SocialService(EurekaRegistrationClient registrationClient) {
        super("reactive-lab-social-service", registrationClient);
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
                                                          Map<String, Object> user = new HashMap<>();
                                                          user.put("userId", userId);
                                                          user.put("friends", Arrays.asList(randomUser(), randomUser(),
                                                                                            randomUser(),
                                                                                            randomUser()));
                                                          return ServerSentEvent.withData(SimpleJson.mapToJson(user));
                                                      })
                                                      .delay(((long) (Math.random() * 100) + 20), TimeUnit.MILLISECONDS)
                       );
    }

    private static int randomUser() {
        return ((int) (Math.random() * 10000));
    }
}

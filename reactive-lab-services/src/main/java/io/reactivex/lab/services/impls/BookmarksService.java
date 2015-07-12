package io.reactivex.lab.services.impls;

import com.netflix.eureka2.client.EurekaRegistrationClient;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.services.common.Random;
import io.reactivex.lab.services.common.SimpleJson;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BookmarksService extends AbstractMiddleTierService {

    public BookmarksService(EurekaRegistrationClient registrationClient) {
        super("reactive-lab-bookmark-service", registrationClient);
    }

    @Override
    protected Observable<Void> handleRequest(HttpServerRequest<?> request, HttpServerResponse<ByteBuf> response) {

        List<String> videoIds = request.getQueryParameters().get("videoId");
        if (videoIds == null || videoIds.size() == 0) {
            return writeError(request, response, "At least one parameter of 'videoId' must be included.");
        }

        final int latency;
        if (Random.randomIntFrom0to100() > 80) {
            latency = 10;
        } else {
            latency = 1;
        }

        return response.transformToServerSentEvents()
                       .writeAndFlushOnEach(Observable.from(videoIds)
                                                      .map(videoId -> {
                                                          Map<String, Object> video = new HashMap<>();
                                                          video.put("videoId", videoId);
                                                          video.put("position", (int) (Math.random() * 5000));
                                                          return ServerSentEvent.withData(SimpleJson.mapToJson(video));
                                                      })
                                                      .delay(latency, TimeUnit.MILLISECONDS)
                       );
    }
}

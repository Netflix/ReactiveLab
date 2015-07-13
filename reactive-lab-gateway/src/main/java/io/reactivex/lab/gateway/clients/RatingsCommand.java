package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.lab.gateway.clients.RatingsCommand.Rating;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RatingsCommand extends HystrixObservableCommand<Rating> {

    private final List<Video> videos;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public RatingsCommand(Video video, ClientRegistry clientRegistry) {
        this(Collections.singletonList(video), clientRegistry);
        // replace with HystrixCollapser
    }

    public RatingsCommand(List<Video> videos, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("Ratings"));
        this.videos = videos;
        this.client = clientRegistry.getRatingsServiceClient();
    }

    @Override
    protected Observable<Rating> construct() {

        return client.createGet("/ratings?" + UrlGenerator.generate("videoId", videos))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(Rating::fromSse));
    }

    @Override
    protected Observable<Rating> resumeWithFallback() {
        Map<String, Object> video = new HashMap<>();
        video.put("videoId", videos.get(0).getId());
        video.put("estimated_user_rating", 3.5);
        video.put("actual_user_rating", 4);
        video.put("average_user_rating", 3.1);
        return Observable.just(new Rating(video));
    }
    
    public static class Rating {

        private final Map<String, Object> data;

        private Rating(Map<String, Object> data) {
            this.data = data;
        }

        public double getEstimatedUserRating() {
            return (double) data.get("estimated_user_rating");
        }

        public double getActualUserRating() {
            return (double) data.get("actual_user_rating");
        }

        public double getAverageUserRating() {
            return (double) data.get("average_user_rating");
        }

        public static Rating fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new Rating(SimpleJson.jsonToMap(json));
        }

    }
}

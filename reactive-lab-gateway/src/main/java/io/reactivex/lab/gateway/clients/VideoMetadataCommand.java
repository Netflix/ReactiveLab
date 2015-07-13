package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.lab.gateway.clients.VideoMetadataCommand.VideoMetadata;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VideoMetadataCommand extends HystrixObservableCommand<VideoMetadata> {

    private final List<Video> videos;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public VideoMetadataCommand(Video video, ClientRegistry clientRegistry) {
        this(Collections.singletonList(video), clientRegistry);
        // replace with HystrixCollapser
    }

    public VideoMetadataCommand(List<Video> videos, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("VideoMetadata"));
        this.videos = videos;
        client = clientRegistry.getVideoMetadataServiceClient();
    }

    @Override
    protected Observable<VideoMetadata> construct() {

        return client.createGet("/metadata?" + UrlGenerator.generate("videoId",
                                                                     videos))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(VideoMetadata::fromSse));
    }
    
    @Override
    protected Observable<VideoMetadata> resumeWithFallback() {
        Map<String, Object> video = new HashMap<>();
        video.put("videoId", videos.get(0).getId());
        video.put("title", "Fallback Video Title");
        video.put("other_data", "goes_here");
        return Observable.just(new VideoMetadata(video));
    }

    public static class VideoMetadata {

        private final Map<String, Object> data;

        public VideoMetadata(Map<String, Object> data) {
            this.data = data;
        }

        public static VideoMetadata fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new VideoMetadata(SimpleJson.jsonToMap(json));
        }

        public int getId() {
            return (int) data.get("videoId");
        }

        public String getTitle() {
            return (String) data.get("title");
        }

        public Map<String, Object> getDataAsMap() {
            return data;
        }

    }

}

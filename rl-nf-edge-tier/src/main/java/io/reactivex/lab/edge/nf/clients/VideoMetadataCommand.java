package io.reactivex.lab.edge.nf.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import io.reactivex.lab.edge.common.SimpleJson;
import io.reactivex.lab.edge.nf.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.lab.edge.nf.clients.VideoMetadataCommand.VideoMetadata;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import rx.Observable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class VideoMetadataCommand extends AbstractHystrixCommand<VideoMetadata> {

    private final List<Video> videos;

    public VideoMetadataCommand(Video video) {
        this(Arrays.asList(video));
        // replace with HystrixCollapser
    }

    public VideoMetadataCommand(List<Video> videos) {
        super(HystrixCommandGroupKey.Factory.asKey("VideoMetadata"));
        this.videos = videos;
    }

    @Override
    protected Observable<VideoMetadata> run() {
        return newClient("localhost", 9196)
                .submit(HttpClientRequest.createGet("/metadata?" + UrlGenerator.generate("videoId", videos)))
                .flatMap(r -> {
                    Observable<VideoMetadata> bytesToJson = r.getContent().map(sse -> {
                        return VideoMetadata.fromJson(sse.getEventData());
                    });
                    return bytesToJson;
                });
    }

    public static class VideoMetadata {

        private final Map<String, Object> data;

        public VideoMetadata(Map<String, Object> data) {
            this.data = data;
        }

        public static VideoMetadata fromJson(String json) {
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

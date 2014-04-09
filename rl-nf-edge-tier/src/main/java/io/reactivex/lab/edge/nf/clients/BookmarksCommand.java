package io.reactivex.lab.edge.nf.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import io.reactivex.lab.edge.common.SimpleJson;
import io.reactivex.lab.edge.nf.clients.BookmarksCommand.Bookmark;
import io.reactivex.lab.edge.nf.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import rx.Observable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BookmarksCommand extends AbstractHystrixCommand<Bookmark> {

    final List<Video> videos;

    public BookmarksCommand(Video video) {
        this(Arrays.asList(video));
        // replace with HystrixCollapser
    }

    public BookmarksCommand(List<Video> videos) {
        super(HystrixCommandGroupKey.Factory.asKey("GetBookmarks"));
        this.videos = videos;
    }

    @Override
    protected Observable<Bookmark> run() {
        return newClient("localhost", 9190)
                .submit(HttpClientRequest.createGet("/bookmarks?" + UrlGenerator.generate("videoId", videos)))
                .flatMap(r -> {
                    Observable<Bookmark> bytesToJson = r.getContent().map(sse -> {
                        return Bookmark.fromJson(sse.getEventData());
                    });
                    return bytesToJson;
                });
    }

    public static class Bookmark {

        private final Map<String, Object> data;

        private Bookmark(Map<String, Object> data) {
            this.data = data;
        }

        public static Bookmark fromJson(String json) {
            return new Bookmark(SimpleJson.jsonToMap(json));
        }

        public int getPosition() {
            return (int) data.get("position");
        }

    }
}

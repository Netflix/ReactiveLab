package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.BookmarksCommand.Bookmark;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BookmarksCommand extends HystrixObservableCommand<Bookmark> {

    private final List<Video> videos;
    private final String cacheKey;
    private final HttpClient<ByteBuf, ByteBuf> bookmarksClient;

    public BookmarksCommand(List<Video> videos, ClientRegistry registry) {
        super(HystrixCommandGroupKey.Factory.asKey("GetBookmarks"));
        this.videos = videos;
        this.bookmarksClient = registry.getBookmarksServiceClient();
        StringBuilder b = new StringBuilder();
        for (Video v : videos) {
            b.append(v.getId()).append("-");
        }
        this.cacheKey = b.toString();
    }

    @Override
    public Observable<Bookmark> construct() {

        return bookmarksClient.createGet("/bookmarks?" + UrlGenerator.generate("videoId", videos))
                              .retryWhen(new RetryWhenNoServersAvailable())
                              .flatMap(resp -> resp.getContentAsServerSentEvents().map(Bookmark::fromSse)
                              );
    }

    protected Observable<Bookmark> resumeWithFallback() {
        List<Bookmark> bs = new ArrayList<>();
        for (Video v : videos) {
            Map<String, Object> data = new HashMap<>();
            data.put("position", 0);
            data.put("videoId", v.getId());
            bs.add(new Bookmark(data));
        }
        return Observable.from(bs);
    }

    @Override
    protected String getCacheKey() {
        return cacheKey;
    }

    public static class Bookmark {

        private final Map<String, Object> data;

        Bookmark(Map<String, Object> data) {
            this.data = data;
        }

        public static Bookmark fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new Bookmark(SimpleJson.jsonToMap(json));
        }

        public int getPosition() {
            return (int) data.get("position");
        }

        public int getVideoId() {
            return Integer.parseInt(String.valueOf(data.get("videoId")));
        }
    }

}

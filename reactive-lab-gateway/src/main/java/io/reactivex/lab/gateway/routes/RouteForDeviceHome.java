package io.reactivex.lab.gateway.routes;

import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.StartGatewayServer;
import io.reactivex.lab.gateway.clients.BookmarkCommand;
import io.reactivex.lab.gateway.clients.BookmarksCommand.Bookmark;
import io.reactivex.lab.gateway.clients.ClientRegistry;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand.Video;
import io.reactivex.lab.gateway.clients.RatingsCommand;
import io.reactivex.lab.gateway.clients.RatingsCommand.Rating;
import io.reactivex.lab.gateway.clients.SocialCommand;
import io.reactivex.lab.gateway.clients.SocialCommand.Social;
import io.reactivex.lab.gateway.clients.UserCommand;
import io.reactivex.lab.gateway.clients.UserCommand.User;
import io.reactivex.lab.gateway.clients.VideoMetadataCommand;
import io.reactivex.lab.gateway.clients.VideoMetadataCommand.VideoMetadata;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

public class RouteForDeviceHome {

    public static Observable<Void> handle(ClientRegistry clientRegistry, HttpServerRequest<ByteBuf> request,
                                          HttpServerResponse<ByteBuf> response) {

        List<String> userId = request.getQueryParameters().get("userId");
        if (userId == null || userId.size() != 1) {
            return StartGatewayServer.writeError(request, response, "A single 'userId' is required.");
        }

        return response.transformToServerSentEvents()
                       .writeAndFlushOnEach(new UserCommand(userId, clientRegistry)
                                                    .observe()
                                                    .flatMap(user -> {
                                                        Observable<Map<String, Object>> catalog =
                                                                createCatalog(user, clientRegistry);
                                                        Observable<Map<String, Object>> social =
                                                                new SocialCommand(user, clientRegistry).observe()
                                                                                                       .map(Social::getDataAsMap);
                                                        return Observable.merge(catalog, social)
                                                                         .map(SimpleJson::mapToJson)
                                                                         .map(ServerSentEvent::withData);
                                                    }));
    }

    private static Observable<Map<String, Object>> createCatalog(User user, ClientRegistry clientRegistry) {
        return new PersonalizedCatalogCommand(user, clientRegistry).observe()
                          .flatMap(catalogList -> catalogList.videos()
                                                             .flatMap(video -> {
                                                                 Observable<Bookmark> bookmark = new BookmarkCommand(video, clientRegistry).observe();
                                                                 Observable<Rating> rating = new RatingsCommand(video, clientRegistry).observe();
                                                                 Observable<VideoMetadata> metadata = new VideoMetadataCommand(video, clientRegistry).observe();
                                                                 return Observable.zip(bookmark, rating, metadata,
                                                                                       (b, r, m) -> combineVideoData(video, b, r, m));
                                                             }));
    }

    private static Map<String, Object> combineVideoData(Video video, Bookmark b, Rating r, VideoMetadata m) {
        Map<String, Object> video_data = new HashMap<>();
        video_data.put("video_id", video.getId());
        video_data.put("bookmark", b.getPosition());
        video_data.put("estimated_user_rating", r.getEstimatedUserRating());
        video_data.put("metadata", m.getDataAsMap());
        return video_data;
    }
}

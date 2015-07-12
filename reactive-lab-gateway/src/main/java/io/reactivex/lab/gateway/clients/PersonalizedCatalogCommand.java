package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.PersonalizedCatalogCommand.Catalog;
import io.reactivex.lab.gateway.clients.UserCommand.User;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersonalizedCatalogCommand extends HystrixObservableCommand<Catalog> {

    private final List<User> users;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public PersonalizedCatalogCommand(User user, ClientRegistry clientRegistry) {
        this(Collections.singletonList(user), clientRegistry);
    }

    public PersonalizedCatalogCommand(List<User> users, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("PersonalizedCatalog"));
        this.users = users;
        client = clientRegistry.getPersonalizedCatalogServiceClient();
    }

    @Override
    protected Observable<Catalog> construct() {
        return client.createGet("/catalog?" + UrlGenerator.generate("userId", users))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(Catalog::fromSse));
    }
    
    @Override
    protected Observable<Catalog> resumeWithFallback() {
        return Observable.from(users).<Catalog>map(u -> {
            try {
                Map<String, Object> userData = new HashMap<>();
                userData.put("userId", u.getId());

                userData.put("list_title", "Really quirky and over detailed list title!");
                userData.put("other_data", "goes_here");
                userData.put("videos", Arrays.asList(12345, 23456, 34567, 45678, 56789, 67890));
                return new Catalog(userData);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    public static class Catalog {

        private final Map<String, Object> data;

        private Catalog(Map<String, Object> data) {
            this.data = data;
        }

        @SuppressWarnings("unchecked")
        public Observable<Video> videos() {
            try {
                return Observable.from((List<Integer>) data.get("videos")).map(Video::new);
            } catch (Exception e) {
                return Observable.error(e);
            }
        }

        public static Catalog fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new Catalog(SimpleJson.jsonToMap(json));
        }
    }

    public static class Video implements ID {

        private final int id;

        public Video(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

    }

}

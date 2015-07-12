package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.SocialCommand.Social;
import io.reactivex.lab.gateway.clients.UserCommand.User;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SocialCommand extends HystrixObservableCommand<Social> {

    private final List<User> users;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public SocialCommand(User user, ClientRegistry clientRegistry) {
        this(Collections.singletonList(user), clientRegistry);
        // replace with HystrixCollapser
    }

    public SocialCommand(List<User> users, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("Social"));
        this.users = users;
        client = clientRegistry.getSocialServiceClient();
    }

    @Override
    protected Observable<Social> construct() {

        return client.createGet("/social?" + UrlGenerator.generate("userId", users))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(Social::fromSse));
    }
    
    @Override
    protected Observable<Social> resumeWithFallback() {
        Map<String, Object> user = new HashMap<>();
        user.put("userId", users.get(0).getId());
        user.put("friends", Collections.emptyList());
        
        return Observable.just(new Social(user));
    }
    
    private static int randomUser() {
        return ((int) (Math.random() * 10000));
    }

    public static class Social {

        private final Map<String, Object> data;

        private Social(Map<String, Object> data) {
            this.data = data;
        }

        public Map<String, Object> getDataAsMap() {
            return data;
        }

        public static Social fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            return new Social(SimpleJson.jsonToMap(json));
        }
    }
}

package io.reactivex.lab.gateway.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.reactivex.lab.gateway.clients.UserCommand.User;
import io.reactivex.lab.gateway.common.SimpleJson;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserCommand extends HystrixObservableCommand<User> {

    private final List<String> userIds;
    private final HttpClient<ByteBuf, ByteBuf> client;

    public UserCommand(List<String> userIds, ClientRegistry clientRegistry) {
        super(HystrixCommandGroupKey.Factory.asKey("User"));
        this.userIds = userIds;
        client = clientRegistry.getUserServiceClient();
    }

    @Override
    protected Observable<User> construct() {

        return client.createGet("/user?" + UrlGenerator.generate("userId", userIds))
                     .retryWhen(new RetryWhenNoServersAvailable())
                     .flatMap(resp -> resp.getContentAsServerSentEvents().map(User::fromSse));
    }
    
    @Override
    protected Observable<User> resumeWithFallback() {
        return Observable.from(userIds).map(id -> {
            Map<String, Object> fallback = new HashMap<>();
            fallback.put("userId", id);
            fallback.put("name", "Fallback Name Here");
            fallback.put("other_data", "goes_here");
            return new User(fallback);
        });
    }

    public static class User implements ID {
        private final Map<String, Object> data;

        public User(Map<String, Object> jsonToMap) {
            this.data = jsonToMap;
        }

        public static User fromSse(ServerSentEvent serverSentEvent) {
            String json = serverSentEvent.contentAsString();
            serverSentEvent.release(); // Release ByteBuf as this is terminally consuming the buffer.
            Map<String, Object> data = SimpleJson.jsonToMap(json);
            if (!data.containsKey("userId")) {
                throw new IllegalArgumentException("A User object requires a 'userId'");
            } else {
                try {
                    Integer.parseInt(String.valueOf(data.get("userId")));
                } catch (Exception e) {
                    throw new IllegalArgumentException("The `userId` must be an Integer");
                }
            }
            return new User(data);
        }

        public int getId() {
            return Integer.parseInt(String.valueOf(data.get("userId")));
        }

        public String getName() {
            return (String) data.get("name");
        }

    }

}

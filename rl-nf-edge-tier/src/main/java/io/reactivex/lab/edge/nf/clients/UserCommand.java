package io.reactivex.lab.edge.nf.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import io.reactivex.lab.edge.common.SimpleJson;
import io.reactivex.lab.edge.nf.clients.UserCommand.User;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import rx.Observable;

import java.util.List;
import java.util.Map;

public class UserCommand extends AbstractHystrixCommand<User> {

    private final List<String> userIds;

    public UserCommand(List<String> userIds) {
        super(HystrixCommandGroupKey.Factory.asKey("User"));
        this.userIds = userIds;
    }

    @Override
    protected Observable<User> run() {
        return newClient("localhost", 9195)
                .submit(HttpClientRequest.createGet("/user?" + UrlGenerator.generate("userId", userIds)))
                .flatMap(r -> {
                    Observable<User> user = r.getContent().map(sse -> {
                        return User.fromJson(sse.getEventData());
                    });
                    return user;
                });
    }

    public static class User implements ID {
        private final Map<String, Object> data;

        public User(Map<String, Object> jsonToMap) {
            this.data = jsonToMap;
        }

        public static User fromJson(String json) {
            return new User(SimpleJson.jsonToMap(json));
        }

        public int getId() {
            return Integer.parseInt(String.valueOf(data.get("userId")));
        }

        public String getName() {
            return (String) data.get("name");
        }

    }

}

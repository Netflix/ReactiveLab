package io.reactivex.lab.services;

import com.netflix.eureka2.client.EurekaRegistrationClient;
import com.netflix.eureka2.client.Eurekas;
import com.netflix.eureka2.client.resolver.ServerResolvers;
import io.reactivex.lab.services.impls.BookmarksService;
import io.reactivex.lab.services.impls.GeoService;
import io.reactivex.lab.services.impls.MockService;
import io.reactivex.lab.services.impls.PersonalizedCatalogService;
import io.reactivex.lab.services.impls.RatingsService;
import io.reactivex.lab.services.impls.SocialService;
import io.reactivex.lab.services.impls.UserService;
import io.reactivex.lab.services.impls.VideoMetadataService;
import rx.Observable;

public class StartMiddleTierServices {

    public static void main(String... args) {

        EurekaRegistrationClient regClient = Eurekas.newRegistrationClientBuilder()
                                                    .withServerResolver(ServerResolvers.fromHostname("127.0.0.1")
                                                                                       .withPort(7006))
                                                             .build();
        /* what port we want to begin at for launching the services */
        int startingPort = 9190;
        if (args.length > 0) {
            startingPort = Integer.parseInt(args[0]);
        }

        System.out.println("Starting services ...");
        new MockService(regClient).start(startingPort);
        new BookmarksService(regClient).start(++startingPort);
        new GeoService(regClient).start(++startingPort);
        new PersonalizedCatalogService(regClient).start(++startingPort);
        new RatingsService(regClient).start(++startingPort);
        new SocialService(regClient).start(++startingPort);
        new UserService(regClient).start(++startingPort);
        new VideoMetadataService(regClient).start(++startingPort);

        // block forever
        Observable.never().toBlocking().single();
    }
}

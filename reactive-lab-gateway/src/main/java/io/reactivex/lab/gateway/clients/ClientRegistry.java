package io.reactivex.lab.gateway.clients;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;

/**
 * A registry for clients to various middle-tier services.
 */
public interface ClientRegistry {

    HttpClient<ByteBuf, ByteBuf> getMockServiceClient();

    HttpClient<ByteBuf, ByteBuf> getBookmarksServiceClient();

    HttpClient<ByteBuf, ByteBuf> getGeoServiceClient();

    HttpClient<ByteBuf, ByteBuf> getPersonalizedCatalogServiceClient();

    HttpClient<ByteBuf, ByteBuf> getRatingsServiceClient();

    HttpClient<ByteBuf, ByteBuf> getSocialServiceClient();

    HttpClient<ByteBuf, ByteBuf> getUserServiceClient();

    HttpClient<ByteBuf, ByteBuf> getVideoMetadataServiceClient();

    enum Services {

        Mock("reactive-lab-mock-service"),
        Bookmarks("reactive-lab-bookmark-service"),
        Geo("reactive-lab-geo-service"),
        PersonalizedCatalog("reactive-lab-personalized-catalog-service"),
        Ratings("reactive-lab-ratings-service"),
        Social("reactive-lab-social-service"),
        User("reactive-lab-user-service"),
        VideoMetadata("reactive-lab-vms-service");

        private final String vipAddress;

        Services(String vipAddress) {
            this.vipAddress = vipAddress;
        }

        public String getVipAddress() {
            return vipAddress;
        }
    }
}

package io.reactivex.lab.tutorial;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.server.HttpServer;
import rx.Observable;

import java.nio.charset.Charset;

/**
 * This example starts a simple HTTP server and client to demonstrate how to use RxNetty HTTP protocol.
 */
public class ClientServer {

    public static void main(String[] args) {

        /**
         * Start our HTTP server.
         */
        HttpServer<ByteBuf, ByteBuf> server = startServer(8088);

        /**
         * Submit the request.
         */
        createRequest("localhost", server.getServerPort())
                /* Block till you get the response. In a real world application, one should not be blocked but chained
                 * into a response to the caller. */
                .toBlocking()
                /**
                 * Print each content of the response.
                 */
                .forEach(System.out::println);
    }

    public static HttpServer<ByteBuf, ByteBuf> startServer(int port) {

        /**
         * Creates an HTTP server which returns "Hello World!" responses.
         */
        return HttpServer.newServer(port)
                         .start((request, response) -> {
                             /**
                              * In a real server, you would be writing different responses based on the
                              * URI of the request.
                              * This example just returns a "Hello World!!" string unconditionally.
                              */
                             return response.writeString(Observable.just("Hello World!!"));
                         });
    }

    public static Observable<String> createRequest(String host, int port) {

        /**
         * Creates an HTTP client bound to the provided host and port.
         */
        return HttpClient.newClient(host, port)
                /* Submit an HTTP GET request with uri "/hello" */
                .createGet("/hello")
                /* Print the HTTP initial line and headers. Return the content.*/
                .flatMap(response -> {
                    /**
                     * Printing the HTTP headers.
                     */
                    System.out.println(response);
                    return response.getContent();
                })
                /* Convert the ByteBuf for each content chunk into a string. */
                .map(byteBuf -> byteBuf.toString(Charset.defaultCharset()));

    }
}

package io.reactivex.lab.edge.nf.clients;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivex.netty.client.RxClientThreadFactory;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;

/**
 * @author Nitesh Kant
 */
abstract class AbstractHystrixCommand<T> extends HystrixObservableCommand<T> {

    private static final EventLoopGroup
            clientGroup = new NioEventLoopGroup(0 /*means default in netty*/, new RxClientThreadFactory());

    protected AbstractHystrixCommand(HystrixCommandGroupKey group) {
        super(group);
    }

    protected static HttpClient<ByteBuf, ServerSentEvent> newClient(String host, int port) {
        return new HttpClientBuilder<ByteBuf, ServerSentEvent>(host, port)
                .pipelineConfigurator(PipelineConfigurators.<ByteBuf>sseClientConfigurator())
                .eventloop(clientGroup)
                .build();
    }
}

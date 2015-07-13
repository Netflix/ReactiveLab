package io.reactivex.lab.servicediscovery;

import com.netflix.eureka2.codec.CodecType;
import com.netflix.eureka2.registry.datacenter.LocalDataCenterInfo;
import com.netflix.eureka2.server.EurekaWriteServer;
import com.netflix.eureka2.server.config.WriteServerConfig;

public class EurekaServer {

    public static final int READ_SERVER_PORT = 7005;
    public static final int WRITE_SERVER_PORT = 7006;

    public static void main(String[] args) throws Exception {

        WriteServerConfig.WriteServerConfigBuilder builder = new WriteServerConfig.WriteServerConfigBuilder();
        builder.withDiscoveryPort(READ_SERVER_PORT)
               .withRegistrationPort(WRITE_SERVER_PORT)
               .withHttpPort(8089)
               .withCodec(CodecType.Avro)
               .withDataCenterType(LocalDataCenterInfo.DataCenterType.Basic);
        EurekaWriteServer eurekaWriteServer = new EurekaWriteServer(builder.build());

        /* start the server */
        eurekaWriteServer.start();

        System.out.println("Started eureka server....");
    }
}

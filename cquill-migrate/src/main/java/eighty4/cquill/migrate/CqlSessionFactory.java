package eighty4.cquill.migrate;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;

public class CqlSessionFactory {

    public static CqlSession create() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(Config.cassandraHost(), Config.cassandraPort()))
                .withLocalDatacenter("datacenter1")
                .build();
    }
}

package com.github.aalobaidi.aerospike.reactor;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.ExecutionException;

public class AerospikeReactorClientIntegrationTest {
    Key key1 = new Key("test", null, "key1");
    private AerospikeReactorClient aerospikeReactorClient;
    private ReactorNettyEventLoops aerospikeLoopGroup;

    @Before
    public void setup() throws InterruptedException, ExecutionException {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(2);
        aerospikeLoopGroup = new ReactorNettyEventLoops(new NettyEventLoops(eventLoopGroup));
        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = aerospikeLoopGroup;
        IAerospikeClient aerospikeClient = new AerospikeClient(policy, "localhost", 3000);
        aerospikeClient.truncate(new InfoPolicy(), "test", null, null);
        Thread.sleep(100);
        aerospikeReactorClient = new AerospikeReactorClient(aerospikeClient);
    }

    @Test
    public void putGetIntegrationTest() {
        StepVerifier
                .create(get(key1))
                .expectNextMatches(keyRecord -> keyRecord.record == null)
                .expectComplete()
                .verify();

        put(key1, 1l).block();

        StepVerifier
                .create(get(key1))
                .expectNextMatches(keyRecord -> keyRecord.record.bins.values().iterator().next().equals(1l))
                .expectComplete()
                .verify();
    }

    private Mono<Key> put(Key key, Object value) {
        return aerospikeReactorClient
                .put(aerospikeLoopGroup.next(), new WritePolicy(), key, new Bin("bin", value));
    }

    private Mono<KeyRecord> get(Key key) {
        return aerospikeReactorClient
                .get(aerospikeLoopGroup.next(), null, key);
    }
}

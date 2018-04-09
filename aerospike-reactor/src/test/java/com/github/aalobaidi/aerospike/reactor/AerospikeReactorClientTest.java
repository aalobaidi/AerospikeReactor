package com.github.aalobaidi.aerospike.reactor;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;

public class AerospikeReactorClientTest {

    private EventLoop eventLoop = mock(EventLoop.class);
    private IAerospikeClient aerospikeClient = mock(IAerospikeClient.class);
    private Key key1 = new Key("namespace", "set", "key1");
    private Key key2 = new Key("namespace", "set", "key2");
    private Record record1 = new Record(ImmutableMap.of("bin", 1), 0, 0);
    private Record record2 = new Record(ImmutableMap.of("bin", 2), 0, 0);

    private AerospikeReactorClient subject = new AerospikeReactorClient(aerospikeClient);

    private List<BatchRead> keys = Arrays.asList(new BatchRead(key1, true), new BatchRead(key1, true));
    private BatchRead result1 = new BatchRead(key1, true);
    private BatchRead result2 = new BatchRead(key2, true);
    private List<BatchRead> result = Arrays.asList(result1, result2);

    @Before
    public void setup() {
        result1.record = record1;
        result2.record = record2;
    }


    @Test
    public void getMustPassCallBacks() {
        // GIVEN
        doAnswer(invc -> {
            RecordListener listener = invc.getArgument(1);
            listener.onSuccess(invc.getArgument(3), record1);
            return null;
        }).when(aerospikeClient).get(isA(EventLoop.class), isA(RecordListener.class), isA(Policy.class), isA(Key.class));

        // WHEN
        Mono<KeyRecord> recordMono = subject.get(eventLoop, new Policy(), key1);

        // THEN
        StepVerifier
                .create(recordMono)
                .expectNextMatches(kr -> kr.key.equals(key1) && kr.record.equals(record1))
                .verifyComplete();
    }

    @Test
    public void getMustPassExceptions() {
        // GIVEN
        doAnswer(invc -> {
            RecordListener listener = invc.getArgument(1);
            listener.onFailure(new AerospikeException.Timeout(10, false));
            return null;
        }).when(subject.getAerospikeClient()).get(isA(EventLoop.class), isA(RecordListener.class), isA(Policy.class), isA(Key.class));

        // WHEN
        Mono<KeyRecord> recordMono = subject.get(eventLoop, new Policy(), key1);

        // THEN
        StepVerifier
                .create(recordMono)
                .expectError(AerospikeException.Timeout.class);
    }

    @Test
    public void getBatchMustPassCallBacks() {
        // GIVEN

        doAnswer(invc -> {
            BatchListListener listener = invc.getArgument(1);
            listener.onSuccess(result);
            return null;
        }).when(aerospikeClient).get(isA(EventLoop.class), isA(BatchListListener.class), isA(BatchPolicy.class), any());

        // WHEN
        Mono<List<BatchRead>> recordMono = subject.get(eventLoop, new BatchPolicy(), keys);

        // THEN
        StepVerifier
                .create(recordMono)
                .expectNext(result)
                .verifyComplete();
    }

    @Test
    public void getBatchMustPassExceptions() {
        // GIVEN

        doAnswer(invc -> {
            BatchListListener listener = invc.getArgument(1);
            listener.onFailure(new AerospikeException.Timeout(10, false));
            return null;
        }).when(aerospikeClient).get(isA(EventLoop.class), isA(BatchListListener.class), isA(BatchPolicy.class), any());

        // WHEN
        Mono<List<BatchRead>> recordMono = subject.get(eventLoop, new BatchPolicy(), keys);

        // THEN
        StepVerifier
                .create(recordMono)
                .expectError(AerospikeException.Timeout.class);
    }

    @Test
    public void getBatchMustPassFluxCallBacks() {
        // GIVEN

        doAnswer(invc -> {
            RecordSequenceListener listener = invc.getArgument(1);
            listener.onRecord(key1, record1);
            listener.onRecord(key2, record2);
            listener.onSuccess();
            return null;
        }).when(aerospikeClient).get(isA(EventLoop.class), isA(RecordSequenceListener.class), isA(BatchPolicy.class), isA(Key[].class));

        // WHEN
        Flux<KeyRecord> recordMono = subject.get(eventLoop, new BatchPolicy(), new Key[]{key1, key2});

        // THEN
        StepVerifier
                .create(recordMono)
                .expectNextMatches(kr -> kr.key.equals(key1) && kr.record.equals(record1))
                .expectNextMatches(kr -> kr.key.equals(key2) && kr.record.equals(record2))
                .verifyComplete();
    }

    @Test
    public void getBatchMustPassFluxException() {
        // GIVEN

        doAnswer(invc -> {
            RecordSequenceListener listener = invc.getArgument(1);
            listener.onFailure(new AerospikeException.Timeout(10, false));
            return null;
        }).when(aerospikeClient).get(isA(EventLoop.class), isA(RecordSequenceListener.class), isA(BatchPolicy.class), isA(Key[].class));

        // WHEN
        Flux<KeyRecord> recordMono = subject.get(eventLoop, new BatchPolicy(), new Key[]{key1, key2});

        // THEN
        StepVerifier
                .create(recordMono)
                .expectError(AerospikeException.Timeout.class);
    }


    @Test
    public void putMustPassCallback() {
        // GIVEN

        doAnswer(invc -> {
            WriteListener listener = invc.getArgument(1);
            listener.onSuccess(key1);
            return null;
        }).when(aerospikeClient).put(isA(EventLoop.class), isA(WriteListener.class), isA(WritePolicy.class), isA(Key.class), isA(Bin.class));

        // WHEN
        Mono<Key> recordMono = subject.put(eventLoop, new WritePolicy(), key1, new Bin("bin", 1));

        // THEN
        StepVerifier
                .create(recordMono)
                .expectNext(key1)
                .expectComplete();
    }
}

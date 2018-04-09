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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class AerospikeReactorClient {

    private final IAerospikeClient aerospikeClient;

    public IAerospikeClient getAerospikeClient() {
        return aerospikeClient;
    }

    public AerospikeReactorClient(IAerospikeClient aerospikeClient) {
        this.aerospikeClient = aerospikeClient;
    }

    @Nonnull
    public Mono<KeyRecord> get(@Nonnull EventLoop eventLoop, @Nullable Policy policy, @Nonnull Key key, @Nonnull String... binNames) {
        return Mono.create(sink -> {
            RecordListener listener = new RecordListener() {
                @Override
                public void onSuccess(Key key, Record record) {
                    sink.success(new KeyRecord(key, record));
                }

                @Override
                public void onFailure(AerospikeException exception) {
                    sink.error(exception);
                }
            };
            if (binNames.length == 0) {
                aerospikeClient.get(eventLoop, listener, policy, key);
            } else {
                aerospikeClient.get(eventLoop, listener, policy, key, binNames);
            }
        });
    }

    @Nonnull
    public Mono<List<BatchRead>> get(EventLoop eventLoop, @Nullable BatchPolicy policy, @Nonnull List<BatchRead> keys) {
        return Mono.create(sink -> {
            BatchListListener listener = new BatchListListener() {

                @Override
                public void onSuccess(List<BatchRead> records) {
                    sink.success(records);
                }

                @Override
                public void onFailure(AerospikeException exception) {
                    sink.error(exception);
                }
            };
            aerospikeClient.get(eventLoop, listener, policy, keys);
        });
    }

    @Nonnull
    public Flux<KeyRecord> get(@Nonnull EventLoop eventLoop, @Nullable BatchPolicy policy, @Nonnull Key[] keys, @Nullable String... binNames) {
        return Flux.create(sink -> {
            RecordSequenceListener listener = new RecordSequenceListener() {
                @Override
                public void onRecord(Key key, Record record) throws AerospikeException {
                    sink.next(new KeyRecord(key, record));
                }

                @Override
                public void onSuccess() {
                    sink.complete();
                }

                @Override
                public void onFailure(AerospikeException exception) {
                    sink.error(exception);
                }
            };
            if (binNames.length == 0) {
                aerospikeClient.get(eventLoop, listener, policy, keys);
            } else {
                aerospikeClient.get(eventLoop, listener, policy, keys, binNames);
            }
        });
    }

    @Nonnull
    public Mono<Key> put(@Nonnull EventLoop eventLoop, @Nullable WritePolicy policy, @Nonnull Key key, @Nonnull Bin... bins) {
        return Mono.create(sink -> {
            WriteListener listener = new WriteListener() {
                @Override
                public void onSuccess(Key key) {
                    sink.success(key);
                }

                @Override
                public void onFailure(AerospikeException exception) {
                    sink.error(exception);
                }
            };
            aerospikeClient.put(eventLoop, listener, policy, key, bins);
        });
    }
}

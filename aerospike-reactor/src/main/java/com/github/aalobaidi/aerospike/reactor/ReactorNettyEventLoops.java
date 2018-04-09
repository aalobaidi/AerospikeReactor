package com.github.aalobaidi.aerospike.reactor;

import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoop;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.TlsPolicy;
import reactor.util.annotation.Nullable;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Nonnull
public class ReactorNettyEventLoops implements EventLoops {

    private final NettyEventLoops nettyEventLoops;
    private final Map<Thread, EventLoop> loopMap;

    public ReactorNettyEventLoops(NettyEventLoops nettyEventLoops) throws ExecutionException, InterruptedException {
        this.nettyEventLoops = nettyEventLoops;
        Map threadsMap = new HashMap<>(nettyEventLoops.getSize());
        for (NettyEventLoop eventLoop : nettyEventLoops.getArray()) {
            eventLoop.get().schedule(
                    () -> threadsMap.put(Thread.currentThread(), eventLoop),
                    0, TimeUnit.NANOSECONDS).get();
        }
        loopMap = Collections.unmodifiableMap(threadsMap);
    }

    @Nullable
    public EventLoop getCurrentEventLoop() {
        return loopMap.get(Thread.currentThread());
    }

    @Override
    public EventLoop[] getArray() {
        return nettyEventLoops.getArray();
    }

    @Override
    public int getSize() {
        return nettyEventLoops.getSize();
    }

    @Override
    public EventLoop get(int index) {
        return nettyEventLoops.get(index);
    }

    @Override
    public EventLoop next() {
        return nettyEventLoops.next();
    }

    @Override
    public void close() {
        nettyEventLoops.close();
    }

    @Override
    public void initTlsContext(TlsPolicy policy) {
        nettyEventLoops.initTlsContext(policy);
    }
}

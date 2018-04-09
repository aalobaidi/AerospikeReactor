package examples.kotlin

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.async.EventLoops
import com.aerospike.client.async.NettyEventLoops
import com.aerospike.client.policy.ClientPolicy
import com.github.aalobaidi.aerospike.reactor.AerospikeReactorClient
import io.netty.channel.nio.NioEventLoopGroup
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@SpringBootApplication
class SpringBootExample

fun main(args: Array<String>) {
    runApplication<SpringBootExample>(*args)
}

@Configuration
class Beans {
    @Bean
    fun eventLoopGroup() = NioEventLoopGroup(Runtime.getRuntime().availableProcessors())

    @Bean
    fun eventLoops(eventLoopGroup: NioEventLoopGroup) = NettyEventLoops(eventLoopGroup)

    @Bean
    fun aerospikeReactorClient(eventLoops: EventLoops): AerospikeReactorClient {
        val policy = ClientPolicy()
        policy.eventLoops = eventLoops
        return AerospikeReactorClient(AerospikeClient(policy, "localhost", 3000))
    }
}

@RestController
class Controller(val aerospikeReactorClient: AerospikeReactorClient,
                 val eventLoops: EventLoops) {

    @GetMapping("/{user}")
    fun helloUser(@PathVariable("user") user: String) =
            aerospikeReactorClient
                    // get record associated with user name
                    .get(eventLoops.next(), null, Key("test", null, user))
                    // get value as Long, default 0
                    .map { (it?.record?.bins?.values?.first() ?: 0L) as Long }
                    // add 1
                    .map { it + 1 }
                    // save value + 1 in Aerospike
                    .flatMap { number ->
                        aerospikeReactorClient
                                .put(eventLoops.next(), null, Key("test", null, user), Bin("", number))
                                .map { number }
                    }
                    .map { it.toString() }

}

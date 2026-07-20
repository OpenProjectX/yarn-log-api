package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import java.net.URI

class NodeManagerLogClient(
    private val webClient: WebClient,
    private val cookieProvider: HadoopSpnegoCookieProvider,
    private val defaultScheme: String = "http",
    private val parser: NodeManagerLogResponseParser = NodeManagerLogResponseParser(),
) {
    fun fetchTail(
        nodeHttpAddress: String,
        containerId: String,
        logType: String,
        windowBytes: Long,
    ): Mono<NodeManagerLogResponse> {
        val uri = buildUri(nodeHttpAddress, containerId, logType, windowBytes)
        return exchange(uri, allowAuthenticationRetry = true)
            .map { parser.parse(it, windowBytes) }
    }

    private fun exchange(uri: URI, allowAuthenticationRetry: Boolean): Mono<ByteArray> =
        cookieProvider.cookieFor(uri)
            .defaultIfEmpty("")
            .flatMap { cookie ->
                webClient.get()
                    .uri(uri)
                    .accept(MediaType.TEXT_PLAIN)
                    .headers { headers ->
                        if (cookie.isNotEmpty()) {
                            headers.set(HttpHeaders.COOKIE, "${AuthenticatedURL.AUTH_COOKIE}=$cookie")
                        }
                    }
                    .exchangeToMono { response ->
                        when {
                            response.statusCode().is2xxSuccessful -> response.bodyToMono(ByteArray::class.java)
                            response.statusCode() == HttpStatus.UNAUTHORIZED && allowAuthenticationRetry -> {
                                response.releaseBody().then(
                                    Mono.defer {
                                        cookieProvider.invalidate(uri)
                                        exchange(uri, allowAuthenticationRetry = false)
                                    },
                                )
                            }
                            else -> response.createException().flatMap { Mono.error(it) }
                        }
                    }
            }

    private fun buildUri(
        nodeHttpAddress: String,
        containerId: String,
        logType: String,
        windowBytes: Long,
    ): URI {
        val origin = if (nodeHttpAddress.contains("://")) nodeHttpAddress else "$defaultScheme://$nodeHttpAddress"
        return UriComponentsBuilder.fromUriString(origin)
            .pathSegment("ws", "v1", "node", "containers", containerId, "logs", logType)
            .queryParam("size", -windowBytes)
            .build()
            .encode()
            .toUri()
    }
}

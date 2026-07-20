package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class HadoopSpnegoCookieProvider(
    private val hadoopScheduler: Scheduler,
    private val cookieTtl: Duration = Duration.ofMinutes(10),
) {
    private data class Origin(val scheme: String, val host: String, val port: Int)
    private data class CachedCookie(val value: String, val expiresAt: Instant)

    private val cookies = ConcurrentHashMap<Origin, CachedCookie>()

    fun cookieFor(uri: URI): Mono<String> {
        if (!UserGroupInformation.isSecurityEnabled()) return Mono.empty()
        val origin = uri.origin()
        val cached = cookies[origin]
        if (cached != null && cached.expiresAt.isAfter(Instant.now())) {
            return Mono.just(cached.value)
        }
        return Mono.fromCallable {
            val ugi = UserGroupInformation.getLoginUser()
            if (ugi.isFromKeytab) ugi.checkTGTAndReloginFromKeytab()
            ugi.doAs(PrivilegedExceptionAction {
                val token = AuthenticatedURL.Token()
                val connection = AuthenticatedURL(KerberosAuthenticator())
                    .openConnection(uri.toURL(), token)
                connection.disconnect()
                token.toString().also {
                    require(it.isNotBlank()) { "SPNEGO completed without a hadoop.auth cookie" }
                }
            })
        }.subscribeOn(hadoopScheduler)
            .doOnNext { cookies[origin] = CachedCookie(it, Instant.now().plus(cookieTtl)) }
    }

    fun invalidate(uri: URI) {
        cookies.remove(uri.origin())
    }

    private fun URI.origin(): Origin {
        val effectivePort = when {
            port >= 0 -> port
            scheme.equals("https", ignoreCase = true) -> 443
            else -> 80
        }
        return Origin(scheme.lowercase(), requireNotNull(host) { "URI has no host: $this" }, effectivePort)
    }
}

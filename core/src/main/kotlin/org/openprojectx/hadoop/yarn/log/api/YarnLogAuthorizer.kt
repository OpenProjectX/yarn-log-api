package org.openprojectx.hadoop.yarn.log.api

import reactor.core.publisher.Mono

fun interface YarnLogAuthorizer {
    fun authorize(requester: String?, applicationId: String, applicationOwner: String): Mono<Boolean>
}

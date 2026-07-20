package org.openprojectx.hadoop.yarn.log.api

import reactor.core.publisher.Flux

fun interface YarnLogStreamService {
    fun stream(request: YarnLogStreamRequest): Flux<YarnLogEvent>
}

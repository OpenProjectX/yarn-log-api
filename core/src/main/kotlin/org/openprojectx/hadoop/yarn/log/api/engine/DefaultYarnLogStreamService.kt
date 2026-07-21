package org.openprojectx.hadoop.yarn.log.api.engine

import org.openprojectx.hadoop.yarn.log.api.YarnLogAuthorizer
import org.openprojectx.hadoop.yarn.log.api.YarnLogEvent
import org.openprojectx.hadoop.yarn.log.api.YarnLogEventType
import org.openprojectx.hadoop.yarn.log.api.YarnLogSource
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamRequest
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import org.apache.hadoop.security.AccessControlException
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class DefaultYarnLogStreamService(
    private val yarnGateway: HadoopYarnGateway,
    private val nodeManagerClient: NodeManagerLogClient,
    private val aggregatedLogSource: AggregatedLogSource,
    private val authorizer: YarnLogAuthorizer,
    private val settings: YarnLogEngineSettings,
) : YarnLogStreamService {
    override fun stream(request: YarnLogStreamRequest): Flux<YarnLogEvent> = Flux.defer {
        validate(request)
        val session = Session(request, settings)
        Flux.concat(
            Mono.just(session.event(YarnLogEventType.OPEN)),
            firstSnapshot(session),
        ).onErrorResume { error ->
            logger.error(
                "YARN log stream failed: applicationId={}, follow={}, logFiles={}, containers={}",
                request.applicationId,
                request.follow,
                request.logFiles,
                request.containerIds,
                error,
            )
            Flux.just(
                session.event(
                    type = YarnLogEventType.ERROR,
                    message = error.message ?: error.javaClass.simpleName,
                ),
            )
        }
    }

    private fun firstSnapshot(session: Session): Flux<YarnLogEvent> =
        yarnGateway.snapshot(session.request.applicationId).flatMapMany { snapshot ->
            authorizer.authorize(session.request.requester, snapshot.applicationId, snapshot.owner)
                .flatMapMany { allowed ->
                    if (!allowed) Flux.error(SecurityException("Not authorized to read ${snapshot.applicationId}"))
                    else cycle(session, snapshot)
                }
        }

    private fun nextCycle(session: Session): Flux<YarnLogEvent> =
        Mono.delay(session.request.pollInterval)
            .then(yarnGateway.snapshot(session.request.applicationId))
            .flatMapMany { cycle(session, it) }

    private fun cycle(session: Session, snapshot: YarnApplicationSnapshot): Flux<YarnLogEvent> {
        val events = mutableListOf<YarnLogEvent>()
        if (session.lastApplicationState != snapshot.state.name) {
            logger.info(
                "YARN application state changed: applicationId={}, state={}",
                snapshot.applicationId,
                snapshot.state.name,
            )
            session.lastApplicationState = snapshot.state.name
            events += session.event(YarnLogEventType.APPLICATION_STATE, state = snapshot.state.name)
        }
        snapshot.containers.forEach { container ->
            session.containers[container.containerId] = container
            if (session.discoveredContainers.add(container.containerId)) {
                logger.debug(
                    "Discovered YARN container: applicationId={}, containerId={}, nodeId={}, state={}",
                    snapshot.applicationId,
                    container.containerId,
                    container.nodeId,
                    container.state,
                )
                events += session.event(
                    type = YarnLogEventType.CONTAINER_DISCOVERED,
                    container = container,
                    state = container.state,
                )
            }
        }
        if (Duration.between(session.lastHeartbeat, Instant.now()) >= settings.heartbeatInterval) {
            session.lastHeartbeat = Instant.now()
            events += session.event(YarnLogEventType.HEARTBEAT)
        }

        val prefix = Flux.fromIterable(events)
        if (snapshot.isFinal) {
            return prefix.concatWith(streamAggregated(session, snapshot))
                .concatWith(Mono.just(session.event(YarnLogEventType.COMPLETE, state = snapshot.state.name)))
        }

        val selectedContainers = snapshot.containers.filter {
            session.request.containerIds.isEmpty() || it.containerId in session.request.containerIds
        }
        val tails = Flux.fromIterable(selectedContainers)
            .flatMap(
                { container ->
                    Flux.fromIterable(session.request.logFiles)
                        .flatMap({ logType -> tailOne(session, container, logType) }, 1)
                },
                settings.maxConcurrentNodeManagerRequests,
            )
        val current = prefix.concatWith(tails)
        return if (session.request.follow) current.concatWith(Flux.defer { nextCycle(session) })
        else current.concatWith(Mono.just(session.event(YarnLogEventType.COMPLETE, state = snapshot.state.name)))
    }

    private fun tailOne(
        session: Session,
        container: YarnContainerSnapshot,
        logType: String,
    ): Flux<YarnLogEvent> {
        val key = LogKey(container.containerId, logType)
        val cursor = session.cursors.computeIfAbsent(key) {
            Cursor(windowBytes = session.request.tailBytes)
        }
        return fetchAtWindow(session, container, logType, cursor)
            .onErrorResume { error ->
                logger.warn(
                    "Unable to read NodeManager log; the stream will continue: applicationId={}, " +
                        "containerId={}, nodeId={}, logType={}, cursorOffset={}, windowBytes={}",
                    session.request.applicationId,
                    container.containerId,
                    container.nodeId,
                    logType,
                    cursor.offset,
                    cursor.windowBytes,
                    error,
                )
                Mono.just(
                    session.event(
                        type = YarnLogEventType.WARNING,
                        container = container,
                        logType = logType,
                        source = YarnLogSource.NODE_MANAGER,
                        message = error.message ?: error.javaClass.simpleName,
                    ),
                )
            }
            .flux()
    }

    private fun fetchAtWindow(
        session: Session,
        container: YarnContainerSnapshot,
        logType: String,
        cursor: Cursor,
    ): Mono<YarnLogEvent> = nodeManagerClient.fetchTail(
        nodeHttpAddress = container.nodeHttpAddress,
        containerId = container.containerId,
        logType = logType,
        windowBytes = cursor.windowBytes,
    ).flatMap { response ->
        when {
            !cursor.initialized -> {
                cursor.initialized = true
                cursor.offset = response.fileLength
                emitBytes(session, container, logType, cursor, response.responseStartOffset, response.bytes)
            }
            response.fileLength < cursor.offset -> {
                val oldOffset = cursor.offset
                logger.warn(
                    "NodeManager log was truncated: applicationId={}, containerId={}, logType={}, " +
                        "oldOffset={}, newLength={}",
                    session.request.applicationId,
                    container.containerId,
                    logType,
                    oldOffset,
                    response.fileLength,
                )
                cursor.generation++
                cursor.offset = 0
                cursor.initialized = false
                Mono.just(
                    session.event(
                        type = YarnLogEventType.RESET,
                        container = container,
                        logType = logType,
                        source = YarnLogSource.NODE_MANAGER,
                        offset = oldOffset,
                        generation = cursor.generation,
                        message = "Log was truncated; new length is ${response.fileLength}",
                    ),
                )
            }
            response.responseStartOffset > cursor.offset -> {
                val required = response.fileLength - cursor.offset
                val expanded = maxOf(cursor.windowBytes * 2, required + session.request.tailBytes)
                    .coerceAtMost(settings.maxTailWindowBytes)
                if (expanded <= cursor.windowBytes) {
                    Mono.error(IllegalStateException("Log growth exceeded the maximum tail window"))
                } else {
                    logger.debug(
                        "Expanding NodeManager tail window: applicationId={}, containerId={}, logType={}, " +
                            "oldWindowBytes={}, newWindowBytes={}",
                        session.request.applicationId,
                        container.containerId,
                        logType,
                        cursor.windowBytes,
                        expanded,
                    )
                    cursor.windowBytes = expanded
                    fetchAtWindow(session, container, logType, cursor)
                }
            }
            response.fileLength == cursor.offset -> Mono.empty()
            else -> {
                val drop = (cursor.offset - response.responseStartOffset).toInt()
                val newBytes = response.bytes.copyOfRange(drop, response.bytes.size)
                val startOffset = cursor.offset
                cursor.offset = response.fileLength
                cursor.windowBytes = maxOf(session.request.tailBytes, newBytes.size.toLong() * 2)
                    .coerceAtMost(settings.maxTailWindowBytes)
                emitBytes(session, container, logType, cursor, startOffset, newBytes)
            }
        }
    }

    private fun emitBytes(
        session: Session,
        container: YarnContainerSnapshot,
        logType: String,
        cursor: Cursor,
        offset: Long,
        bytes: ByteArray,
    ): Mono<YarnLogEvent> {
        if (bytes.isEmpty()) return Mono.empty()
        return Mono.just(
            session.event(
                type = YarnLogEventType.LOG,
                container = container,
                logType = logType,
                source = YarnLogSource.NODE_MANAGER,
                offset = offset,
                generation = cursor.generation,
                encoding = "BASE64",
                data = Base64.getEncoder().encodeToString(bytes),
                text = bytes.toSingleLineLogText(),
            ),
        )
    }

    private fun streamAggregated(
        session: Session,
        snapshot: YarnApplicationSnapshot,
    ): Flux<YarnLogEvent> {
        if (!session.aggregatedStarted.compareAndSet(false, true)) return Flux.empty()
        val aggregated = if (session.request.containerIds.isEmpty()) {
            streamAggregatedContainer(session, snapshot, null)
        } else {
            Flux.fromIterable(session.request.containerIds)
                .concatMap { containerId -> streamAggregatedContainer(session, snapshot, containerId) }
        }
        return aggregated
            .onErrorResume { error ->
                logger.warn(
                    "Unable to read aggregated logs after application completion: applicationId={}, " +
                        "owner={}, logFiles={}, containers={}",
                    snapshot.applicationId,
                    snapshot.owner,
                    session.request.logFiles,
                    session.request.containerIds,
                    error,
                )
                Flux.just(
                    session.event(
                        type = YarnLogEventType.WARNING,
                        source = YarnLogSource.AGGREGATED,
                        message = error.message ?: error.javaClass.simpleName,
                    ),
                )
            }
    }

    private fun streamAggregatedContainer(
        session: Session,
        snapshot: YarnApplicationSnapshot,
        containerId: String?,
    ): Flux<YarnLogEvent> = aggregatedLogSource.stream(
        applicationId = snapshot.applicationId,
        owner = snapshot.owner,
        logFiles = session.request.logFiles,
        containerId = containerId,
    ).retryWhen(
        Retry.fixedDelay(
            maxOf(1, settings.aggregationWaitTimeout.dividedBy(settings.aggregationRetryInterval)),
            settings.aggregationRetryInterval,
        ).filter { error -> error is IOException && error !is AccessControlException },
    ).timeout(settings.aggregationWaitTimeout)
        .map { bytes ->
        session.event(
            type = YarnLogEventType.LOG,
            logType = if (session.request.logFiles.size == 1) session.request.logFiles.first() else null,
            source = YarnLogSource.AGGREGATED,
            encoding = "BASE64",
            data = Base64.getEncoder().encodeToString(bytes),
            text = bytes.toSingleLineLogText(),
        )
    }

    private fun validate(request: YarnLogStreamRequest) {
        require(request.applicationId.startsWith("application_")) { "Invalid YARN application ID" }
        require(request.logFiles.isNotEmpty()) { "At least one log file must be selected" }
        require(request.tailBytes > 0) { "tailBytes must be positive" }
        require(request.tailBytes <= settings.maxTailWindowBytes) { "tailBytes exceeds configured maximum" }
        require(!request.pollInterval.isNegative && !request.pollInterval.isZero) { "pollInterval must be positive" }
    }

    private data class LogKey(val containerId: String, val logType: String)
    private data class Cursor(
        var offset: Long = 0,
        var generation: Long = 1,
        var windowBytes: Long,
        var initialized: Boolean = false,
    )

    private class Session(
        val request: YarnLogStreamRequest,
        settings: YarnLogEngineSettings,
    ) {
        private val sequence = AtomicLong()
        val discoveredContainers = ConcurrentHashMap.newKeySet<String>()
        val containers = ConcurrentHashMap<String, YarnContainerSnapshot>()
        val cursors = ConcurrentHashMap<LogKey, Cursor>()
        val aggregatedStarted = java.util.concurrent.atomic.AtomicBoolean()
        var lastApplicationState: String? = null
        var lastHeartbeat: Instant = Instant.now().minus(settings.heartbeatInterval)

        fun event(
            type: YarnLogEventType,
            container: YarnContainerSnapshot? = null,
            logType: String? = null,
            source: YarnLogSource? = null,
            offset: Long? = null,
            generation: Long? = null,
            encoding: String? = null,
            data: String? = null,
            text: String? = null,
            state: String? = null,
            message: String? = null,
        ) = YarnLogEvent(
            type = type,
            sequence = sequence.incrementAndGet(),
            applicationId = request.applicationId,
            containerId = container?.containerId,
            nodeId = container?.nodeId,
            logType = logType,
            source = source,
            offset = offset,
            generation = generation,
            encoding = encoding,
            data = data,
            text = text,
            state = state,
            message = message,
        )
    }

    private companion object {
        val logger = LoggerFactory.getLogger(DefaultYarnLogStreamService::class.java)
    }
}

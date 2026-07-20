package org.openprojectx.hadoop.yarn.log.api.engine

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.io.IOException
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.util.concurrent.atomic.AtomicBoolean

class AggregatedLogSource(
    private val configuration: Configuration,
    private val hadoopScheduler: Scheduler,
) {
    fun stream(
        applicationId: String,
        owner: String,
        logFiles: Set<String>,
        containerId: String? = null,
    ): Flux<ByteArray> = Flux.using(
        {
            PipeBridge().also { bridge ->
                bridge.producer = hadoopScheduler.schedule {
                    try {
                        val request = ContainerLogsRequest().apply {
                            appId = ApplicationId.fromString(applicationId)
                            appOwner = owner
                            this.containerId = containerId
                            logTypes = logFiles
                            bytes = Long.MAX_VALUE
                            setAppFinished(true)
                        }
                        val controller = LogAggregationFileControllerFactory(configuration)
                            .getFileControllerForRead(request.appId, owner)
                        val found = controller.readAggregatedLogs(request, bridge.output)
                        if (!found) bridge.failure = NoAggregatedLogsException(applicationId)
                    } catch (error: Throwable) {
                        bridge.failure = error
                    } finally {
                        bridge.closeOutput()
                    }
                }
            }
        },
        { bridge ->
            Flux.generate<ByteArray> { sink ->
                try {
                    val buffer = ByteArray(DEFAULT_CHUNK_SIZE)
                    val read = bridge.input.read(buffer)
                    if (read < 0) {
                        bridge.failure?.let(sink::error) ?: sink.complete()
                    } else {
                        sink.next(if (read == buffer.size) buffer else buffer.copyOf(read))
                    }
                } catch (error: IOException) {
                    if (bridge.closed.get()) sink.complete() else sink.error(error)
                }
            }.subscribeOn(hadoopScheduler)
        },
        PipeBridge::close,
    )

    private class PipeBridge {
        val input = PipedInputStream(PIPE_CAPACITY)
        val output = PipedOutputStream(input)
        val closed = AtomicBoolean(false)
        @Volatile var failure: Throwable? = null
        @Volatile var producer: reactor.core.Disposable? = null

        fun closeOutput() {
            runCatching { output.close() }
        }

        fun close() {
            if (closed.compareAndSet(false, true)) {
                runCatching { input.close() }
                runCatching { output.close() }
                producer?.dispose()
            }
        }
    }

    class NoAggregatedLogsException(applicationId: String) :
        IOException("No aggregated logs are available for $applicationId")

    private companion object {
        const val DEFAULT_CHUNK_SIZE = 16 * 1024
        const val PIPE_CAPACITY = 64 * 1024
    }
}

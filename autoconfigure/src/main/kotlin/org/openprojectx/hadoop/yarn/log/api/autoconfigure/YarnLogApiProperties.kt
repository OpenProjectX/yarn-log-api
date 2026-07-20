package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize
import java.time.Duration

@ConfigurationProperties("yarn-log-api")
class YarnLogApiProperties {
    var enabled: Boolean = true
    var webSocketPath: String = "/api/v1/yarn/logs"
    var pollInterval: Duration = Duration.ofSeconds(1)
    var minimumPollInterval: Duration = Duration.ofMillis(250)
    var initialTailBytes: DataSize = DataSize.ofKilobytes(64)
    var maxTailWindow: DataSize = DataSize.ofMegabytes(8)
    var maxConcurrentNodeManagerRequests: Int = 16
    var heartbeatInterval: Duration = Duration.ofSeconds(15)
    var aggregationWaitTimeout: Duration = Duration.ofMinutes(2)
    var aggregationRetryInterval: Duration = Duration.ofSeconds(2)
    var spnegoCookieTtl: Duration = Duration.ofMinutes(10)
}

package org.openprojectx.hadoop.yarn.log.api.autoconfigure

import tools.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.util.WebAppUtils
import org.openprojectx.hadoop.yarn.log.api.YarnApplicationQueryService
import org.openprojectx.hadoop.yarn.log.api.YarnLogAuthorizer
import org.openprojectx.hadoop.yarn.log.api.YarnLogStreamService
import org.openprojectx.hadoop.yarn.log.api.engine.AggregatedLogSource
import org.openprojectx.hadoop.yarn.log.api.engine.DefaultYarnApplicationQueryService
import org.openprojectx.hadoop.yarn.log.api.engine.DefaultYarnLogStreamService
import org.openprojectx.hadoop.yarn.log.api.engine.HadoopSpnegoCookieProvider
import org.openprojectx.hadoop.yarn.log.api.engine.HadoopYarnGateway
import org.openprojectx.hadoop.yarn.log.api.engine.NodeManagerLogClient
import org.openprojectx.hadoop.yarn.log.api.engine.YarnLogEngineSettings
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.core.Ordered
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

@AutoConfiguration
@ConditionalOnClass(WebClient::class, YarnClient::class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnProperty(prefix = "yarn-log-api", name = ["enabled"], matchIfMissing = true)
@EnableConfigurationProperties(YarnLogApiProperties::class)
class YarnLogApiAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun yarnLogWebClientBuilder(): WebClient.Builder = WebClient.builder()

    @Bean
    @ConditionalOnMissingBean(Configuration::class)
    fun yarnConfiguration(): Configuration = YarnConfiguration()

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    fun yarnClient(configuration: Configuration): YarnClient =
        YarnClient.createYarnClient().apply {
            init(configuration)
            start()
        }

    @Bean(destroyMethod = "dispose")
    @ConditionalOnMissingBean(name = ["yarnLogHadoopScheduler"])
    fun yarnLogHadoopScheduler(properties: YarnLogApiProperties): Scheduler =
        Schedulers.newBoundedElastic(
            maxOf(4, properties.maxConcurrentNodeManagerRequests),
            10_000,
            "yarn-log-hadoop",
        )

    @Bean
    @ConditionalOnMissingBean
    fun yarnLogAuthorizer(): YarnLogAuthorizer = YarnLogAuthorizer { _, _, _ -> reactor.core.publisher.Mono.just(true) }

    @Bean
    @ConditionalOnMissingBean
    fun hadoopYarnGateway(
        yarnClient: YarnClient,
        @Qualifier("yarnLogHadoopScheduler") scheduler: Scheduler,
    ) = HadoopYarnGateway(yarnClient, scheduler)

    @Bean
    @ConditionalOnMissingBean
    fun yarnApplicationQueryService(
        yarnGateway: HadoopYarnGateway,
        authorizer: YarnLogAuthorizer,
    ): YarnApplicationQueryService = DefaultYarnApplicationQueryService(yarnGateway, authorizer)

    @Bean
    @ConditionalOnMissingBean
    fun yarnLogStreamService(
        configuration: Configuration,
        yarnGateway: HadoopYarnGateway,
        webClientBuilder: WebClient.Builder,
        @Qualifier("yarnLogHadoopScheduler") scheduler: Scheduler,
        authorizer: YarnLogAuthorizer,
        properties: YarnLogApiProperties,
    ): YarnLogStreamService {
        val settings = YarnLogEngineSettings(
            maxConcurrentNodeManagerRequests = properties.maxConcurrentNodeManagerRequests,
            maxTailWindowBytes = properties.maxTailWindow.toBytes(),
            heartbeatInterval = properties.heartbeatInterval,
            aggregationWaitTimeout = properties.aggregationWaitTimeout,
            aggregationRetryInterval = properties.aggregationRetryInterval,
        )
        val webClient = webClientBuilder.clone()
            .codecs {
                val maxBytes = minOf(Int.MAX_VALUE.toLong(), properties.maxTailWindow.toBytes() + 1024 * 1024)
                it.defaultCodecs().maxInMemorySize(maxBytes.toInt())
            }
            .build()
        val cookieProvider = HadoopSpnegoCookieProvider(scheduler, properties.spnegoCookieTtl)
        return DefaultYarnLogStreamService(
            yarnGateway = yarnGateway,
            nodeManagerClient = NodeManagerLogClient(
                webClient,
                cookieProvider,
                WebAppUtils.getHttpSchemePrefix(configuration).removeSuffix("://"),
            ),
            aggregatedLogSource = AggregatedLogSource(configuration, scheduler),
            authorizer = authorizer,
            settings = settings,
        )
    }

    @Bean
    @ConditionalOnMissingBean
    fun yarnLogSseController(service: YarnLogStreamService, properties: YarnLogApiProperties) =
        YarnLogSseController(service, properties)

    @Bean
    @ConditionalOnMissingBean
    fun yarnApplicationController(service: YarnApplicationQueryService) = YarnApplicationController(service)

    @Bean
    @ConditionalOnMissingBean(YarnLogWebSocketHandler::class)
    fun yarnLogWebSocketHandler(
        service: YarnLogStreamService,
        objectMapper: ObjectMapper,
        properties: YarnLogApiProperties,
    ) = YarnLogWebSocketHandler(service, objectMapper, properties)

    @Bean
    fun yarnLogWebSocketMapping(
        handler: YarnLogWebSocketHandler,
        properties: YarnLogApiProperties,
    ): HandlerMapping = SimpleUrlHandlerMapping(
        mapOf(properties.webSocketPath to handler),
        Ordered.HIGHEST_PRECEDENCE,
    )

    @Bean
    @ConditionalOnMissingBean
    fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()
}

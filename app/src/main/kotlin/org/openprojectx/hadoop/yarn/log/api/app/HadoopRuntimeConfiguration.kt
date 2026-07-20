package org.openprojectx.hadoop.yarn.log.api.app

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration as SpringConfiguration
import java.nio.file.Files
import java.nio.file.Paths

@ConfigurationProperties("yarn-log-api.app")
class YarnLogApiAppProperties {
    var hadoopConfigDirectory: String? = null
    var kerberosPrincipal: String? = null
    var kerberosKeytab: String? = null
}

@SpringConfiguration(proxyBeanMethods = false)
@EnableConfigurationProperties(YarnLogApiAppProperties::class)
class HadoopRuntimeConfiguration {
    @Bean
    fun yarnConfiguration(properties: YarnLogApiAppProperties): Configuration {
        val configuration = YarnConfiguration()
        properties.hadoopConfigDirectory
            ?.takeIf(String::isNotBlank)
            ?.let { loadHadoopConfiguration(configuration, it) }

        UserGroupInformation.setConfiguration(configuration)
        loginFromKeytabIfConfigured(properties)
        return configuration
    }

    private fun loadHadoopConfiguration(configuration: Configuration, directory: String) {
        val configDirectory = Paths.get(directory).toAbsolutePath().normalize()
        require(Files.isDirectory(configDirectory)) {
            "Hadoop configuration directory does not exist: $configDirectory"
        }
        HADOOP_CONFIG_FILES.forEach { fileName ->
            val configFile = configDirectory.resolve(fileName)
            if (Files.isRegularFile(configFile)) {
                configuration.addResource(Path(configFile.toUri()))
            }
        }
        configuration.reloadConfiguration()
    }

    private fun loginFromKeytabIfConfigured(properties: YarnLogApiAppProperties) {
        val principal = properties.kerberosPrincipal?.takeIf(String::isNotBlank)
        val keytab = properties.kerberosKeytab?.takeIf(String::isNotBlank)
        require((principal == null) == (keytab == null)) {
            "Both yarn-log-api.app.kerberos-principal and kerberos-keytab must be configured together"
        }
        if (principal != null && keytab != null) {
            val keytabPath = Paths.get(keytab).toAbsolutePath().normalize()
            require(Files.isRegularFile(keytabPath)) { "Kerberos keytab does not exist: $keytabPath" }
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath.toString())
        }
    }

    private companion object {
        val HADOOP_CONFIG_FILES = listOf("core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml")
    }
}

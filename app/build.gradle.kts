plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("buildsrc.convention.spring-kotlin")
    id("com.google.cloud.tools.jib") version "3.5.4"
}


dependencies {
    implementation(project(":yarn-log-api-spring-boot-starter"))
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation(libs.hadoopYarnCommon) {
        exclude(group = "com.sun.jersey")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation(libs.hadoopCommon) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
}

jib {
    from {
        image = providers.gradleProperty("jibFromImage")
            .orElse("eclipse-temurin:17-jre-jammy")
            .get()
    }
    to {
        image = providers.gradleProperty("jibToImage")
            .orElse(providers.environmentVariable("JIB_TO_IMAGE"))
            .orElse("ghcr.io/openprojectx/yarn-log-api")
            .get()
        tags = setOf(project.version.toString())

        val username = providers.gradleProperty("jibToUsername")
            .orElse(providers.environmentVariable("JIB_TO_USERNAME"))
            .orNull
        val password = providers.gradleProperty("jibToPassword")
            .orElse(providers.environmentVariable("JIB_TO_PASSWORD"))
            .orNull

        if (!username.isNullOrBlank() && !password.isNullOrBlank()) {
            auth {
                this.username = username
                this.password = password
            }
        }
    }
    container {
        mainClass = "org.openprojectx.hadoop.yarn.log.api.app.YarnLogApiApplicationKt"
        ports = listOf("8080")
        user = "10001:10001"
        workingDirectory = "/app"
        extraClasspath = listOf("/etc/hadoop/conf", "/app/extensions/*")
        volumes = listOf("/config", "/app/extensions", "/etc/hadoop/conf", "/etc/security/keytabs")
        environment = mapOf(
            "SPRING_CONFIG_ADDITIONAL_LOCATION" to "optional:file:/config/",
        )
        jvmFlags = listOf(
            "-XX:InitialRAMPercentage=25.0",
            "-XX:MaxRAMPercentage=75.0",
            "-XX:+ExitOnOutOfMemoryError",
            "-Djava.security.egd=file:/dev/urandom",
        )
        labels = mapOf(
            "org.opencontainers.image.title" to "YARN Log API",
            "org.opencontainers.image.description" to "Reactive SSE and WebSocket API for YARN application logs",
            "org.opencontainers.image.source" to "https://github.com/OpenProjectX/yarn-log-api",
            "org.opencontainers.image.version" to project.version.toString(),
            "org.opencontainers.image.licenses" to "Apache-2.0",
        )
    }
    extraDirectories {
        paths {
            path {
                setFrom(file("src/main/jib"))
                into = "/"
            }
        }
    }
}

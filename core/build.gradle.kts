plugins {
    id("buildsrc.convention.kotlin-jvm")
}


dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}"))

    api("io.projectreactor:reactor-core")
    implementation("org.springframework:spring-webflux")

    implementation(libs.hadoopYarnClient) {
        exclude(group = "com.sun.jersey")
        exclude(group = "org.glassfish.jersey")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation(libs.hadoopYarnCommon) {
        exclude(group = "com.sun.jersey")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation(libs.hadoopCommon) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.wiremock:wiremock-standalone:3.13.2")
}

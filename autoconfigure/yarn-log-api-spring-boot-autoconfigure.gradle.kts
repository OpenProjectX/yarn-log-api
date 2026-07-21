plugins {
    id("buildsrc.convention.kotlin-jvm")
    `kotlin-kapt`
}


dependencies {

    api(project(":core"))

    val bootBom = platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}")

    implementation(bootBom)
    kapt(bootBom)

    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation("org.springframework:spring-webflux")
    implementation(libs.hadoopYarnClient) {
        exclude(group = "com.sun.jersey")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation(libs.hadoopCommon) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation("tools.jackson.core:jackson-databind")
    api("org.springframework.boot:spring-boot-starter")
    kapt("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-webflux-test")
    testImplementation("com.ninja-squad:springmockk:5.0.1")
    testImplementation(kotlin("test"))
}

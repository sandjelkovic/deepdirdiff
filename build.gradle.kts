plugins {
    kotlin("jvm") version "1.9.20"
    kotlin("plugin.serialization") version "1.9.20"
    // Apply GraalVM Native Image plugin
    id("org.graalvm.buildtools.native") version "0.9.28"
    application
}

group = "org.sandjelkovic"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
    implementation("org.slf4j:slf4j-simple:2.0.3")
    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    implementation("org.apache.commons:commons-lang3:3.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("MainKt")
}
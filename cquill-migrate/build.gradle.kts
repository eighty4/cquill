plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.datastax.oss:java-driver-core:4.14.1")
    implementation("com.datastax.oss:java-driver-mapper-runtime:4.14.1")
    implementation("org.slf4j:slf4j-nop:1.7.9")
}

java {
    sourceCompatibility = org.gradle.api.JavaVersion.VERSION_17
    targetCompatibility = org.gradle.api.JavaVersion.VERSION_17
}

val migrateMainClassName = "eighty4.cquill.migrate.Migration"

application {
    applicationName = "CQuilL Migration"
    mainClass.set(migrateMainClassName)
}

tasks.create("deployableJar", Jar::class) {
    group = "build"
    description = "Creates a self-contained and runnable JAR of the application."
    manifest.attributes["Main-Class"] = migrateMainClassName
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    val dependencies = configurations
        .runtimeClasspath
        .get()
        .map(::zipTree)
    from(dependencies)
    with(tasks.jar.get())
}

tasks.create("buildDockerImage", Exec::class) {
    group = "build"
    description = "Builds a docker image to run a CQL migration."
    dependsOn("deployableJar")
    workingDir(".")
    commandLine("docker", "build", "-t", "cquill/migrate", ".")
}

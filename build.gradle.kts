plugins {
    kotlin("jvm") version "1.4.10"
}

group = "org.ggye"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("com.datastax.cassandra:cassandra-driver-core:3.0.8")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.6.2")
}

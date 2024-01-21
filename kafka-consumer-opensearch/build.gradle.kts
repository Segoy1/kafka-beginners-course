plugins {
    id("java")
}

group = "de.segoy.kafka.demos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.6.0")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.11")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.11")

    // https://mvnrepository.com/artifact/org.opensearch.client/opensearch-rest-high-level-client
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.11.1")

    // https://mvnrepository.com/artifact/com.google.code.gson/gson
    implementation("com.google.code.gson:gson:2.10.1")


}

tasks.test {
    useJUnitPlatform()
}

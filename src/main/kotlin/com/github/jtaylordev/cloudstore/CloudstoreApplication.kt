package com.github.jtaylordev.cloudstore

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity

@SpringBootApplication
@EnableWebFluxSecurity
class CloudstoreApplication

fun main(args: Array<String>) {
    runApplication<CloudstoreApplication>(*args)
}

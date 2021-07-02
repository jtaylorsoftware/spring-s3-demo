package com.github.jtaylordev.cloudstore

import com.github.jtaylordev.cloudstore.routes.S3Handler
import com.github.jtaylordev.cloudstore.routes.S3Routes
import com.github.jtaylordev.cloudstore.security.apiHttpSecurity
import com.github.jtaylordev.cloudstore.services.S3Service
import org.springframework.context.support.beans
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter

fun beans() = beans {
    bean<S3Service>()
    bean<S3Handler>()
    bean("s3Routes") {
        S3Routes(ref()).router()
    }
    bean("apiHttpSecurity") {
        apiHttpSecurity(ref())
    }
    profile("cors") {
        bean("corsFilter") {
            CorsWebFilter {
                CorsConfiguration().applyPermitDefaultValues()
            }
        }
    }
}
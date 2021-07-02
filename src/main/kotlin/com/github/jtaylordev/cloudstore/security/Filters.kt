package com.github.jtaylordev.cloudstore.security

import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.config.web.server.invoke
import org.springframework.security.web.server.SecurityWebFilterChain

fun apiHttpSecurity(http: ServerHttpSecurity): SecurityWebFilterChain {
    return http {
        authorizeExchange {
            authorize(anyExchange, authenticated)
        }
        oauth2ResourceServer {
            jwt { }
        }
        csrf { disable() }
    }
}

package com.egm.stellio.search.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders

/**
 * This configuration class was added since handlers tests (when importing a customSecurityConfiguration)
 * requires defining a bean of type ReactiveJwtDecoder.
 */
@TestConfiguration
class WebSecurityTestConfig : WebSecurityConfig() {

    @Value("\${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    val issuerUri: String? = null

    @Bean
    fun jwtDecoder(): ReactiveJwtDecoder {
        return ReactiveJwtDecoders.fromOidcIssuerLocation(issuerUri)
    }
}

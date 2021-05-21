package com.egm.stellio.search.config

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.SeekToCurrentErrorHandler

@Configuration
class KafkaConfig(
    private val applicationProperties: ApplicationProperties
) {

    @Bean
    fun kafkaListenerContainerFactory(
        configurer: ConcurrentKafkaListenerContainerFactoryConfigurer,
        kafkaConsumerFactory: ConsumerFactory<Any?, Any?>?
    ): ConcurrentKafkaListenerContainerFactory<*, *>? {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        configurer.configure(factory, kafkaConsumerFactory)
        factory.setErrorHandler(SeekToCurrentErrorHandler())
        return factory
    }

    @Bean
    fun batchFactory(
        kafkaConsumerFactory: ConsumerFactory<Any?, Any?>?
    ): ConcurrentKafkaListenerContainerFactory<*, *>? {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        factory.consumerFactory = kafkaConsumerFactory
        factory.setConcurrency(applicationProperties.batchListener.concurrency)
        factory.isBatchListener = true
        return factory
    }
}

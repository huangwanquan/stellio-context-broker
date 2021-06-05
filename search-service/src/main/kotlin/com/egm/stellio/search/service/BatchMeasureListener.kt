package com.egm.stellio.search.service

import com.egm.stellio.search.util.NgsiLdEventParsingUtils.prepareConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class BatchMeasureListener(
    private val temporalEntityAttributeService: TemporalEntityAttributeService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["cim.eqp.Transmitter.MSR", "cim.eqp.Transmitter.ALR"],
        groupId = "search-eqp-transmitter-msr",
        containerFactory = "batchFactory"
    )
    fun processBatchMsrTransmitter(
        @Payload contents: List<ConsumerRecord<String, String>>
    ) {
        val partitions = contents.map { it.partition() }.toSet()
        logger.info("Reading ${contents.size} messages from $partitions")
        val teaWithInstances = prepareConsumerRecord(contents)
        logger.info("Prepared ${teaWithInstances.size} TEAs to deal with")

        val createdInstances =
            temporalEntityAttributeService.handleBatchAttributeAppend(teaWithInstances)
                .block()
        logger.info("Created $createdInstances instances")
    }
}

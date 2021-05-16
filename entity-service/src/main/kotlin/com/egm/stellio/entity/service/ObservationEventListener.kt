package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.parseAndExpandAttributeFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class ObservationEventListener(
    private val entityService: EntityService,
    private val entityAttributeService: EntityAttributeService,
    private val entityEventService: EntityEventService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topicPattern = "cim.observation.*", groupId = "observations")
    fun processMessage(content: String) {
        when (val observationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityCreate(observationEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(observationEvent, true)
            is AttributeAppendEvent -> handleAttributeAppendEvent(observationEvent, true)
            else -> logger.warn("Observation event ${observationEvent.operationType} not handled.")
        }
    }

    @KafkaListener(topics = ["cim.eqp.Client"], groupId = "entity-eqp-client")
    fun processEqpClient(content: String, @Header(KafkaHeaders.OFFSET) offset: Long) {
        logger.debug("Reading at offset $offset on Client topic")
        processMessage(content)
    }

    @KafkaListener(topics = ["cim.eqp.ServicePoint"], groupId = "entity-eqp-servicePoint")
    fun processEqpServicePoint(content: String, @Header(KafkaHeaders.OFFSET) offset: Long) {
        logger.debug("Reading at offset $offset on ServicePoint topic")
        processMessage(content)
    }

    @KafkaListener(topics = ["cim.eqp.WaterMeter"], groupId = "entity-eqp-waterMeter")
    fun processEqpWaterMeter(content: String, @Header(KafkaHeaders.OFFSET) offset: Long) {
        logger.debug("Reading at offset $offset on WaterMeter topic")
        processMessage(content)
    }

    @KafkaListener(topics = ["cim.eqp.Transmitter"], groupId = "entity-eqp-transmitter")
    fun processEqpTransmitter(content: String, @Header(KafkaHeaders.OFFSET) offset: Long) {
        logger.debug("Reading at offset $offset on Transmitter topic")
        processMessage(content)
    }

    @KafkaListener(topics = ["cim.eqp.Transmitter.ALR"], groupId = "entity-eqp-transmitter-alr")
    fun processAlrTransmitter(content: String) {
        processMessage(content)
    }

    @KafkaListener(topics = ["cim.eqp.Transmitter.MSR"], groupId = "entity-eqp-transmitter-msr")
    fun processMsrTransmitter(content: String) {
        when (val observationEvent = deserializeAs<EntityEvent>(content)) {
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(observationEvent, false)
            is AttributeAppendEvent -> handleAttributeAppendEvent(observationEvent, false)
            else -> logger.warn("Observation event ${observationEvent.operationType} not handled.")
        }
    }

    fun handleEntityCreate(observationEvent: EntityCreateEvent) {
        val ngsiLdEntity = JsonLdUtils.expandJsonLdEntity(
            observationEvent.operationPayload,
            observationEvent.contexts
        ).toNgsiLdEntity()

        try {
            entityService.createEntity(ngsiLdEntity)
        } catch (e: AlreadyExistsException) {
            logger.warn("Entity ${ngsiLdEntity.id} already exists: ${e.message}")
            return
        }

        entityEventService.publishEntityEvent(
            observationEvent,
            ngsiLdEntity.type
        )
    }

    fun handleAttributeUpdateEvent(observationEvent: AttributeUpdateEvent, doPublish: Boolean) {
        val expandedPayload = parseAndExpandAttributeFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        try {
            entityAttributeService.partialUpdateEntityAttribute(
                observationEvent.entityId,
                expandedPayload,
                observationEvent.contexts
            )
        } catch (e: ResourceNotFoundException) {
            logger.error("Entity or attribute not found in observation : ${e.message}")
            return
        }

        if (doPublish) {
            val updatedEntity = entityService.getFullEntityById(observationEvent.entityId, true)
            if (updatedEntity == null)
                logger.warn("Unable to retrieve entity ${observationEvent.entityId} from DB, not sending to Kafka")
            else
                entityEventService.publishEntityEvent(
                    AttributeUpdateEvent(
                        observationEvent.entityId,
                        observationEvent.attributeName,
                        observationEvent.datasetId,
                        observationEvent.operationPayload,
                        compactAndSerialize(updatedEntity, observationEvent.contexts, MediaType.APPLICATION_JSON),
                        observationEvent.contexts
                    ),
                    updatedEntity.type
                )
        }
    }

    fun handleAttributeAppendEvent(observationEvent: AttributeAppendEvent, doPublish: Boolean) {
        val expandedPayload = parseAndExpandAttributeFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        try {
            val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
            val updateResult = entityService.appendEntityAttributes(
                observationEvent.entityId,
                ngsiLdAttributes,
                !observationEvent.overwrite
            )
            if (updateResult.notUpdated.isNotEmpty()) {
                logger.warn("Attribute could not be appended: ${updateResult.notUpdated}")
                return
            }

            if (doPublish) {
                entityEventService.publishAttributeAppend(
                    observationEvent, updateResult.updated[0].updateOperationResult
                )
            }
        } catch (e: BadRequestDataException) {
            logger.error(e.message)
            return
        }
    }
}

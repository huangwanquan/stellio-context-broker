package com.egm.stellio.search.util

import arrow.core.Invalid
import arrow.core.Valid
import arrow.core.Validated
import arrow.core.invalid
import arrow.core.valid
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.util.function.Tuples
import java.time.ZonedDateTime

object NgsiLdEventParsingUtils {
    private var knownAttributeNames = mutableMapOf<String, String>()
    private val objectMapper = jacksonObjectMapper()

    fun prepareConsumerRecord(
        consumerRecords: List<ConsumerRecord<String, String>>
    ): Map<TemporalEntityAttribute, List<AttributeInstance>> =
        consumerRecords.map { consumerRecord ->
            val observationEvent = JsonUtils.deserializeAs<EntityEvent>(consumerRecord.value()) as AttributeAppendEvent
            val expandedAttributeName =
                if (knownAttributeNames.containsKey(observationEvent.attributeName)) {
                    knownAttributeNames[observationEvent.attributeName]!!
                } else {
                    JsonLdUtils.expandJsonLdKey(observationEvent.attributeName, observationEvent.contexts)!!.also {
                        knownAttributeNames[observationEvent.attributeName] = it
                    }
                }
            val operationPayloadNode = objectMapper.readTree(observationEvent.operationPayload)
            Tuples.of(
                observationEvent,
                expandedAttributeName,
                operationPayloadNode,
                toTemporalAttributeMetadata(operationPayloadNode)
            )
        }.mapNotNull { preparedRecord ->
            when (val extractedAttributeMetadata = preparedRecord.t4) {
                is Invalid -> null
                is Valid -> {
                    val attributeMetadata = extractedAttributeMetadata.a
                    val attributeAppendEvent = preparedRecord.t1

                    val temporalEntityAttribute = TemporalEntityAttribute(
                        entityId = attributeAppendEvent.entityId,
                        type = "https://uri.fiware.org/ns/data-models#Device",
                        attributeName = preparedRecord.t2,
                        attributeType = attributeMetadata.type,
                        attributeValueType = attributeMetadata.valueType,
                        datasetId = attributeMetadata.datasetId
                    )

                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = temporalEntityAttribute.id,
                        observedAt = attributeMetadata.observedAt,
                        measuredValue = attributeMetadata.measuredValue,
                        value = attributeMetadata.value,
                        jsonNode = preparedRecord.t3
                    )

                    Pair(temporalEntityAttribute, attributeInstance)
                }
            }
        }.groupBy {
            it.first
        }.mapValues {
            it.value.map { it.second }
        }

    fun toTemporalAttributeMetadata(jsonNode: JsonNode): Validated<String, AttributeMetadata> {
        val attributeTypeAsText = jsonNode["type"].asText()
        val attributeType = kotlin.runCatching {
            TemporalEntityAttribute.AttributeType.valueOf(attributeTypeAsText)
        }.getOrNull() ?: return "Unsupported attribute type: $attributeTypeAsText".invalid()
        val attributeValue = when (attributeType) {
            TemporalEntityAttribute.AttributeType.Relationship -> Pair(jsonNode["object"].asText(), null)
            TemporalEntityAttribute.AttributeType.Property -> {
                val rawAttributeValue = jsonNode["value"]
                when {
                    rawAttributeValue == null -> return "Unable to get a value from attribute: $jsonNode".invalid()
                    rawAttributeValue.isNumber -> Pair(null, valueToDoubleOrNull(rawAttributeValue.asDouble()))
                    else -> Pair(valueToStringOrNull(rawAttributeValue.asText()), null)
                }
            }
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = jsonNode["datasetId"]?.asText()?.toUri(),
            type = attributeType,
            observedAt = ZonedDateTime.parse(jsonNode["observedAt"].asText())
        ).valid()
    }
}

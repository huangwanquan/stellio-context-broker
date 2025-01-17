package com.egm.stellio.search.model

import org.springframework.data.annotation.Id
import java.net.URI
import java.util.UUID

data class TemporalEntityAttribute(
    @Id
    val id: UUID = UUID.randomUUID(),
    val entityId: URI,
    val type: String,
    val attributeName: String,
    val attributeType: AttributeType = AttributeType.Property,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null,
    // FIXME it should be not null, but we have existing data where the payload is not present
    val entityPayload: String? = null
) {
    enum class AttributeValueType {
        MEASURE,
        ANY
    }

    enum class AttributeType {
        Property,
        Relationship
    }
}

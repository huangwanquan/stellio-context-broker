package com.egm.stellio.search.model

import java.net.URI
import java.util.UUID

data class TemporalEntityAttribute(
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TemporalEntityAttribute

        if (entityId != other.entityId) return false
        if (attributeName != other.attributeName) return false
        if (datasetId != other.datasetId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entityId.hashCode()
        result = 31 * result + attributeName.hashCode()
        result = 31 * result + (datasetId?.hashCode() ?: 0)
        return result
    }
}

package com.egm.stellio.search.service

import arrow.core.Validated
import arrow.core.invalid
import arrow.core.valid
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.NgsiLdAttributeInstance
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val attributeInstanceService: AttributeInstanceService,
    private val applicationProperties: ApplicationProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(temporalEntityAttribute: TemporalEntityAttribute): Mono<Int> =
        databaseClient.sql(
            """
            INSERT INTO temporal_entity_attribute
                (id, entity_id, type, attribute_name, attribute_type, attribute_value_type, dataset_id)
            VALUES (:id, :entity_id, :type, :attribute_name, :attribute_type, :attribute_value_type, :dataset_id)
            """.trimIndent()
        )
            .bind("id", temporalEntityAttribute.id)
            .bind("entity_id", temporalEntityAttribute.entityId)
            .bind("type", temporalEntityAttribute.type)
            .bind("attribute_name", temporalEntityAttribute.attributeName)
            .bind("attribute_type", temporalEntityAttribute.attributeType.toString())
            .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
            .bind("dataset_id", temporalEntityAttribute.datasetId)
            .fetch()
            .rowsUpdated()

    internal fun createEntityPayload(entityId: URI, entityPayload: String?): Mono<Int> =
        if (applicationProperties.entity.storePayloads)
            databaseClient.sql(
                """
                INSERT INTO entity_payload (entity_id, payload)
                VALUES (:entity_id, :payload)
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("payload", entityPayload?.let { Json.of(entityPayload) })
                .fetch()
                .rowsUpdated()
        else
            Mono.just(1)

    fun updateEntityPayload(entityId: URI, payload: String): Mono<Int> =
        if (applicationProperties.entity.storePayloads)
            databaseClient.sql(
                """
                UPDATE entity_payload SET payload = :payload WHERE entity_id = :entity_id
                """.trimIndent()
            )
                .bind("payload", Json.of(payload))
                .bind("entity_id", entityId)
                .fetch()
                .rowsUpdated()
        else
            Mono.just(1)

    fun deleteEntityPayload(entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()

    fun createEntityTemporalReferences(payload: String, contexts: List<String>): Mono<Int> {
        val entity = JsonLdUtils.expandJsonLdEntity(payload, contexts).toNgsiLdEntity()
        val parsedPayload = JsonUtils.deserializeObject(payload)

        logger.debug("Analyzing create event for entity ${entity.id}")

        val temporalAttributes = entity.attributes
            .flatMapTo(
                arrayListOf()
            ) {
                it.getAttributeInstances().map { instance ->
                    Pair(it.name, toTemporalAttributeMetadata(instance))
                }
            }.filter {
                it.second.isValid
            }.map {
                Pair(it.first, it.second.toEither().orNull()!!)
            }.ifEmpty {
                return Mono.just(0)
            }

        logger.debug("Found ${temporalAttributes.size} temporal attributes in entity: ${entity.id}")

        return Flux.fromIterable(temporalAttributes.asIterable())
            .map {
                val (expandedAttributeName, attributeMetadata) = it
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entity.id,
                    type = entity.type,
                    attributeName = expandedAttributeName,
                    attributeType = attributeMetadata.type,
                    attributeValueType = attributeMetadata.valueType,
                    datasetId = attributeMetadata.datasetId,
                    entityPayload = payload
                )

                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    observedAt = attributeMetadata.observedAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    payload = extractAttributeInstanceFromCompactedEntity(
                        parsedPayload,
                        compactTerm(expandedAttributeName, contexts),
                        attributeMetadata.datasetId
                    )
                )

                Pair(temporalEntityAttribute, attributeInstance)
            }
            .flatMap { temporalEntityAttributeAndInstance ->
                create(temporalEntityAttributeAndInstance.first).zipWhen {
                    attributeInstanceService.create(temporalEntityAttributeAndInstance.second)
                }
            }
            .collectList()
            .map { it.size }
            .zipWith(createEntityPayload(entity.id, payload))
            .map { it.t1 + it.t2 }
    }

    fun deleteTemporalEntityReferences(entityId: URI): Mono<Int> =
        attributeInstanceService.deleteAttributeInstancesOfEntity(entityId)
            .zipWith(deleteEntityPayload(entityId))
            .then(deleteTemporalAttributesOfEntity(entityId))

    fun deleteTemporalAttributesOfEntity(entityId: URI): Mono<Int> =
        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .matching(query(where("entity_id").`is`(entityId)))
            .all()

    fun deleteTemporalAttributeReferences(entityId: URI, attributeName: String, datasetId: URI?): Mono<Int> =
        attributeInstanceService.deleteAttributeInstancesOfTemporalAttribute(entityId, attributeName, datasetId)
            .zipWith(
                databaseClient.sql(
                    """
                    delete FROM temporal_entity_attribute WHERE 
                        entity_id = :entity_id
                        ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
                        AND attribute_name = :attribute_name
                    """.trimIndent()
                )
                    .bind("entity_id", entityId)
                    .bind("attribute_name", attributeName)
                    .let {
                        if (datasetId != null) it.bind("dataset_id", datasetId)
                        else it
                    }
                    .fetch()
                    .rowsUpdated()
            )
            .map { it.t1 + it.t2 }

    fun deleteTemporalAttributeAllInstancesReferences(entityId: URI, attributeName: String): Mono<Int> =
        attributeInstanceService.deleteAllAttributeInstancesOfTemporalAttribute(entityId, attributeName)
            .zipWith(
                databaseClient.sql(
                    """
                    DELETE FROM temporal_entity_attribute
                    WHERE entity_id = :entity_id
                    AND attribute_name = :attribute_name
                    """.trimIndent()
                )
                    .bind("entity_id", entityId)
                    .bind("attribute_name", attributeName)
                    .fetch()
                    .rowsUpdated()
            )
            .map { it.t1 + it.t2 }

    internal fun toTemporalAttributeMetadata(
        ngsiLdAttributeInstance: NgsiLdAttributeInstance
    ): Validated<String, AttributeMetadata> {
        // for now, let's say that if the 1st instance is temporal, all instances are temporal
        // let's also consider that a temporal property is one having an observedAt property
        if (!ngsiLdAttributeInstance.isTemporalAttribute())
            return "Ignoring attribute $ngsiLdAttributeInstance, it has no observedAt information".invalid()
        val attributeType =
            when (ngsiLdAttributeInstance) {
                is NgsiLdPropertyInstance -> TemporalEntityAttribute.AttributeType.Property
                is NgsiLdRelationshipInstance -> TemporalEntityAttribute.AttributeType.Relationship
                else -> return "Unsupported attribute type ${ngsiLdAttributeInstance.javaClass}".invalid()
            }
        val attributeValue = when (ngsiLdAttributeInstance) {
            is NgsiLdRelationshipInstance -> Pair(ngsiLdAttributeInstance.objectId.toString(), null)
            is NgsiLdPropertyInstance ->
                Pair(
                    valueToStringOrNull(ngsiLdAttributeInstance.value),
                    valueToDoubleOrNull(ngsiLdAttributeInstance.value)
                )
            is NgsiLdGeoPropertyInstance -> Pair(null, null)
        }
        if (attributeValue == Pair(null, null)) {
            return "Unable to get a value from attribute: $ngsiLdAttributeInstance".invalid()
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = ngsiLdAttributeInstance.datasetId,
            type = attributeType,
            observedAt = ngsiLdAttributeInstance.observedAt!!
        ).valid()
    }

    fun getForEntities(
        limit: Int,
        offset: Int,
        ids: Set<URI>,
        types: Set<String>,
        attrs: Set<String>,
        accessRightFilter: () -> String?
    ): Mono<List<TemporalEntityAttribute>> {
        val selectQuery =
            """
                SELECT id, entity_id, type, attribute_name, attribute_type, attribute_value_type, dataset_id
                FROM temporal_entity_attribute            
                WHERE
            """.trimIndent()

        val filterQuery = buildEntitiesQueryFilter(ids, types, attrs, accessRightFilter)
        val finalQuery = """
            $selectQuery
            $filterQuery
            ORDER BY entity_id
            limit :limit
            offset :offset
        """.trimIndent()
        return databaseClient
            .sql(finalQuery)
            .bind("limit", limit)
            .bind("offset", offset)
            .fetch()
            .all()
            .map { rowToTemporalEntityAttribute(it) }
            .collectList()
    }

    fun getCountForEntities(
        ids: Set<URI>,
        types: Set<String>,
        attrs: Set<String>,
        accessRightFilter: () -> String?
    ): Mono<Int> {
        val selectStatement =
            """
            SELECT count(distinct(entity_id)) as count_entity from temporal_entity_attribute
            WHERE
            """.trimIndent()

        val filterQuery = buildEntitiesQueryFilter(ids, types, attrs, accessRightFilter)
        return databaseClient
            .sql("$selectStatement $filterQuery")
            .map(rowToTemporalCount)
            .one()
    }

    fun buildEntitiesQueryFilter(
        ids: Set<URI>,
        types: Set<String>,
        attrs: Set<String>,
        accessRightFilter: () -> String?,
    ): String {
        val formattedIds =
            if (ids.isNotEmpty())
                ids.joinToString(separator = ",", prefix = "entity_id in(", postfix = ")") { "'$it'" }
            else null
        val formattedTypes =
            if (types.isNotEmpty())
                types.joinToString(separator = ",", prefix = "type in (", postfix = ")") { "'$it'" }
            else null
        val formattedAttrs =
            if (attrs.isNotEmpty())
                attrs.joinToString(separator = ",", prefix = "attribute_name in (", postfix = ")") { "'$it'" }
            else null

        return listOfNotNull(formattedIds, formattedTypes, formattedAttrs, accessRightFilter())
            .joinToString(" AND ")
    }

    fun getForEntity(id: URI, attrs: Set<String>): Flux<TemporalEntityAttribute> {
        val selectQuery =
            """
                SELECT id, entity_id, type, attribute_name, attribute_type, attribute_value_type, dataset_id
                FROM temporal_entity_attribute            
                WHERE temporal_entity_attribute.entity_id = :entity_id
            """.trimIndent()

        val expandedAttrsList = attrs.joinToString(",") { "'$it'" }
        val finalQuery =
            if (attrs.isNotEmpty())
                "$selectQuery AND attribute_name in ($expandedAttrsList)"
            else
                selectQuery

        return databaseClient
            .sql(finalQuery)
            .bind("entity_id", id)
            .fetch()
            .all()
            .map { rowToTemporalEntityAttribute(it) }
    }

    fun getFirstForEntity(id: URI): Mono<UUID> {
        val selectQuery =
            """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .map(rowToId)
            .first()
    }

    fun getForEntityAndAttribute(id: URI, attributeName: String, datasetId: URI? = null): Mono<UUID> {
        val selectQuery =
            """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            ${if (datasetId != null) "AND dataset_id = :dataset_id" else ""}
            AND attribute_name = :attribute_name
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .map(rowToId)
            .one()
    }

    private fun rowToTemporalEntityAttribute(row: Map<String, Any>) =
        TemporalEntityAttribute(
            id = row["id"] as UUID,
            entityId = (row["entity_id"] as String).toUri(),
            type = row["type"] as String,
            attributeName = row["attribute_name"] as String,
            attributeType = TemporalEntityAttribute.AttributeType.valueOf(row["attribute_type"] as String),
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(
                row["attribute_value_type"] as String
            ),
            datasetId = (row["dataset_id"] as String?)?.toUri(),
            entityPayload = row["payload"] as String?
        )

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }

    private var rowToTemporalCount: ((Row) -> Int) = { row ->
        row.get("count_entity", Integer::class.java)!!.toInt()
    }
}

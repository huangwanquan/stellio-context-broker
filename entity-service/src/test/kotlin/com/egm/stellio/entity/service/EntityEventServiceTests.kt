package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.parseAndExpandAttributeFragment
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.concurrent.SettableListenableFuture

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    private val breedingServiceUri = "urn:ngsi-ld:BreedingService:0214".toUri()
    private val breedingServiceType = "https://ontology.eglobalmark.com/aquac#BreedingService"
    private val fishNameAttribute = "https://ontology.eglobalmark.com/aquac#fishName"
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishName1DatasetUri = "urn:ngsi-ld:Dataset:fishName:1".toUri()
    private val fishName2DatasetUri = "urn:ngsi-ld:Dataset:fishName:2".toUri()

    @Test
    fun `it should not validate a topic name with characters not supported by Kafka`() {
        assertTrue(
            entityEventService
                .composeTopicName("https://some.host/type")
                .isInvalid
        )
    }

    @Test
    fun `it should validate a topic name with characters supported by Kafka`() {
        assertTrue(
            entityEventService
                .composeTopicName("Specie")
                .isValid
        )
    }

    @Test
    fun `it should not publish an event if topic name is invalid`() {
        entityEventService.publishEntityCreateEvent(
            breedingServiceUri,
            "https://some.host/type",
            "operationPayload",
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify { kafkaTemplate.send(any(), any(), any()) wasNot Called }
    }

    @Test
    fun `it should publish an ENTITY_CREATE event`() {
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        entityEventService.publishEntityCreateEvent(
            breedingServiceUri, breedingServiceType, "operationPayload", listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify { kafkaTemplate.send("cim.entity.BreedingService", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_REPLACE event`() {
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        entityEventService.publishEntityReplaceEvent(
            breedingServiceUri, breedingServiceType, "operationPayload", listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify { kafkaTemplate.send("cim.entity.BreedingService", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_DELETE event`() {
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        entityEventService.publishEntityDeleteEvent(
            breedingServiceUri, breedingServiceType, listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify { kafkaTemplate.send("cim.entity.BreedingService", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_APPEND event if an attribute was appended`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val fishNumberAttributeFragment =
            """
            {
                "type": "Property",
                "value": 120
            }            
            """.trimIndent()

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvent(
            breedingServiceUri,
            "BreedingService",
            "fishNumber",
            null,
            true,
            fishNumberAttributeFragment,
            UpdateOperationResult.APPENDED,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeAppendEvent> {
                    it.operationType == EventsType.ATTRIBUTE_APPEND &&
                        it.entityId == breedingServiceUri &&
                        it.entityType == "BreedingService" &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(fishNumberAttributeFragment) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish a single ATTRIBUTE_REPLACE event if an attribute was replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val fishNumberAttributeFragment =
            """
            {
                "type": "Property",
                "value": 120
            }            
            """.trimIndent()

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvent(
            breedingServiceUri,
            "BreedingService",
            "fishNumber",
            null,
            true,
            fishNumberAttributeFragment,
            UpdateOperationResult.REPLACED,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> {
                    it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                        it.entityId == breedingServiceUri &&
                        it.entityType == "BreedingService" &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(fishNumberAttributeFragment) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish an ATTRIBUTE_APPEND event if an attribute was appended`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val expectedOperationPayload =
            """
                { "type": "Property", "value": 120 }
            """.trimIndent()
        val fishNumberAttributeFragment =
            """
                { "fishNumber": $expectedOperationPayload }            
            """.trimIndent()
        val jsonLdAttributes = expandJsonLdFragment(fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        val appendResult = UpdateResult(
            listOf(UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED)),
            emptyList()
        )

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvents(
            breedingServiceUri,
            jsonLdAttributes,
            appendResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeAppendEvent> {
                    it.operationType == EventsType.ATTRIBUTE_APPEND &&
                        it.entityId == breedingServiceUri &&
                        it.entityType == "BreedingService" &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(expectedOperationPayload) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish a ATTRIBUTE_REPLACE event if an attribute was updated`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val expectedOperationPayload =
            """
                { "type": "Property", "value": 120 }
            """.trimIndent()
        val fishNumberAttributeFragment =
            """
                { "fishNumber": $expectedOperationPayload }
            """.trimIndent()
        val jsonLdAttributes = expandJsonLdFragment(fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        val appendResult = UpdateResult(
            listOf(UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED)),
            emptyList()
        )

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeUpdateEvents(
            breedingServiceUri,
            jsonLdAttributes,
            appendResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> {
                    it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                        it.entityId == breedingServiceUri &&
                        it.entityType == "BreedingService" &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(expectedOperationPayload) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_APPEND and ATTRIBUTE_REPLACE events if attributes were appended and replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val expectedFishNumberOperationPayload =
            """
                { "type": "Property", "value": 120 }
            """.trimIndent()
        val fishNumberAttributeFragment =
            """
                "fishNumber": $expectedFishNumberOperationPayload
            """.trimIndent()
        val expectedFishNameOperationPayload =
            """
                { "type": "Property", "datasetId": "$fishName1DatasetUri", "value": 50 }
            """.trimIndent()
        val fishNameAttributeFragment =
            """
                "fishName": $expectedFishNameOperationPayload
            """.trimIndent()
        val attributesFragment = "{ $fishNumberAttributeFragment, $fishNameAttributeFragment }"
        val jsonLdAttributes = expandJsonLdFragment(attributesFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        val appendResult = UpdateResult(
            listOf(
                UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED),
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED)
            ),
            emptyList()
        )

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvents(
            breedingServiceUri,
            jsonLdAttributes,
            appendResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    when (entityEvent) {
                        is AttributeAppendEvent -> {
                            listOf(entityEvent).any {
                                it.entityId == breedingServiceUri &&
                                    it.entityType == "BreedingService" &&
                                    it.attributeName == "fishNumber" &&
                                    it.datasetId == null &&
                                    it.operationPayload.matchContent(expectedFishNumberOperationPayload) &&
                                    it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                            }
                        }
                        is AttributeReplaceEvent -> {
                            listOf(entityEvent).any {
                                it.entityId == breedingServiceUri &&
                                    it.entityType == "BreedingService" &&
                                    it.attributeName == "fishName" &&
                                    it.datasetId == fishName1DatasetUri &&
                                    it.operationPayload.matchContent(expectedFishNameOperationPayload) &&
                                    it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                            }
                        }
                        else -> false
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if two attributes are replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val expectedFishNumberOperationPayload =
            """
                { "type":"Property", "value":600 }
            """.trimIndent()
        val fishNumberPayload =
            """
                "fishNumber": $expectedFishNumberOperationPayload
            """.trimIndent()
        val expectedFishNameOperationPayload =
            """
                { "type":"Property", "datasetId": "$fishName1DatasetUri", "value":"Salmon", "unitCode": "C1" }
            """.trimIndent()
        val fishNamePayload =
            """
                "fishName": $expectedFishNameOperationPayload
            """.trimIndent()
        val attributePayload = "{ $fishNumberPayload, $fishNamePayload }"
        val jsonLdAttributes = expandJsonLdFragment(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            it.entityType == "BreedingService" &&
                            (it.attributeName == "fishName" || it.attributeName == "fishNumber") &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == null) &&
                            (
                                it.operationPayload.matchContent(expectedFishNumberOperationPayload) ||
                                    it.operationPayload.matchContent(expectedFishNameOperationPayload)
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if a multi-attribute is replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val fishNamePayload1 =
            """
            {
                "type":"Property",
                "datasetId":"urn:ngsi-ld:Dataset:fishName:1",
                "value":"Salmon",
                "unitCode":"C1"
            }
            """.trimIndent()
        val fishNamePayload2 =
            """
            {
                "type":"Property",
                "datasetId":"urn:ngsi-ld:Dataset:fishName:2",
                "value":"Salmon2",
                "unitCode":"C1"
            }            
            """.trimIndent()
        val attributePayload = "{ \"fishName\": [$fishNamePayload1,$fishNamePayload2] }"
        val jsonLdAttributes = expandJsonLdFragment(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNameAttribute, fishName2DatasetUri, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeAppendEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            it.entityType == "BreedingService" &&
                            it.attributeName == "fishName" &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == fishName2DatasetUri) &&
                            (
                                it.operationPayload.matchContent(fishNamePayload1) ||
                                    it.operationPayload.matchContent(fishNamePayload2)
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE event if a property is updated`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)
        val fishNamePayload =
            """
            {
                "value":"Salmon",
                "unitCode":"C1"
            }
            """.trimIndent()

        val jsonLdAttributes = parseAndExpandAttributeFragment(
            "fishName",
            fishNamePayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val updatedDetails = listOf(UpdatedDetails(fishNameAttribute, null, UpdateOperationResult.UPDATED))

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishPartialAttributeUpdateEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updatedDetails,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeUpdateEvent> {
                    it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                        it.entityId == breedingServiceUri &&
                        it.entityType == "BreedingService" &&
                        it.attributeName == "fishName" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(fishNamePayload) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE events if multi instance relationship is updated`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val attributeName = "https://ontology.eglobalmark.com/egm#connectsTo"
        val firstRelationshipPayload =
            """
            {"datasetId":"urn:ngsi-ld:Dataset:connectsTo:1","object":"urn:ngsi-ld:Feeder:018"}
            """.trimIndent()
        val secondRelationshipPayload =
            """
            {"name":{"value":"Salmon"},"object":"urn:ngsi-ld:Feeder:012"}
            """.trimIndent()
        val connectsToPayload =
            """
            [
                $firstRelationshipPayload,
                $secondRelationshipPayload
            ]
            """.trimIndent()
        val jsonLdAttributes = parseAndExpandAttributeFragment(
            "connectsTo",
            connectsToPayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val updatedDetails = listOf(
            UpdatedDetails(attributeName, "urn:ngsi-ld:Dataset:connectsTo:1".toUri(), UpdateOperationResult.UPDATED),
            UpdatedDetails(attributeName, null, UpdateOperationResult.UPDATED)
        )

        entityEventService.publishPartialAttributeUpdateEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updatedDetails,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it as AttributeUpdateEvent
                        it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                            it.entityId == breedingServiceUri &&
                            it.attributeName == "connectsTo" &&
                            (
                                it.datasetId == null || it.datasetId == "urn:ngsi-ld:Dataset:connectsTo:1".toUri()
                                ) &&
                            (
                                it.operationPayload.matchContent(firstRelationshipPayload) ||
                                    it.operationPayload.matchContent(secondRelationshipPayload)
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE_ALL_INSTANCE event if all instances of an attribute are deleted`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeDeleteEvent(
            breedingServiceUri,
            "fishName",
            null,
            true,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeDeleteAllInstancesEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES &&
                            it.entityId == breedingServiceUri &&
                            it.entityType == "BreedingService" &&
                            it.attributeName == "fishName" &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE event if an instances of an attribute is deleted`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val jsonLdEntity = mockk<JsonLdEntity>(relaxed = true)

        every { entityService.getFullEntityById(breedingServiceUri, true) } returns jsonLdEntity
        every { jsonLdEntity.type } returns breedingServiceType

        entityEventService.publishAttributeDeleteEvent(
            breedingServiceUri,
            "fishName",
            "urn:ngsi-ld:Dataset:1".toUri(),
            false,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeDeleteEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_DELETE &&
                            it.entityId == breedingServiceUri &&
                            it.entityType == "BreedingService" &&
                            it.attributeName == "fishName" &&
                            it.datasetId == "urn:ngsi-ld:Dataset:1".toUri() &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }
}

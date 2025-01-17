package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.ZonedDateTime
import java.util.Optional
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityService::class])
@ActiveProfiles("test")
class EntityServiceTests {

    @Autowired
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean(relaxed = true)
    private lateinit var entityRepository: EntityRepository

    @MockkBean
    private lateinit var partialEntityRepository: PartialEntityRepository

    @MockkBean
    private lateinit var searchRepository: SearchRepository

    private val mortalityRemovalServiceUri = "urn:ngsi-ld:MortalityRemovalService:014YFA9Z".toUri()
    private val fishContainmentUri = "urn:ngsi-ld:FishContainment:1234".toUri()

    @Test
    fun `it should create an entity with a property having a property`() {
        val sampleDataWithContext =
            parseSampleDataToNgsiLd("aquac/BreedingService_propWithProp.json")
        val breedingServiceUri = "urn:ngsi-ld:BreedingService:PropWithProp".toUri()

        val mockedBreedingService = mockkClass(Entity::class)
        every { mockedBreedingService.id } returns breedingServiceUri

        every { entityRepository.existsById(eq(breedingServiceUri)) } returns false
        every { partialEntityRepository.existsById(breedingServiceUri) } returns false
        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns true
        every { entityRepository.getEntityCoreById(any()) } returns mockedBreedingService
        every { mockedBreedingService.serializeCoreProperties(true) } returns mutableMapOf(
            "@id" to mortalityRemovalServiceUri.toString(),
            "@type" to listOf("MortalityRemovalService")
        )
        every { mockedBreedingService.contexts } returns sampleDataWithContext.contexts

        entityService.createEntity(sampleDataWithContext)

        confirmVerified()
    }

    @Test
    fun `it should replace an existing default relationship`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:92033f60-bb8b-4640-9464-bca23199ac".toUri()
        val relationshipTargetId = "urn:ngsi-ld:FishContainment:8792".toUri()

        val payload =
            """
            {
              "filledIn": {
                "type": "Relationship",
                "object": "$fishContainmentUri"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedRelationshipTarget = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")
        every { mockkedRelationship.type } returns listOf("Relationship")
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedRelationshipTarget.id } returns relationshipTargetId
        every { mockkedRelationship setProperty "observedAt" value any<ZonedDateTime>() } answers { value }

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any()) } returns 1
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns true

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasRelationshipInstance(any(), eq("filledIn"), null) }
        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == sensorId
                },
                eq("filledIn"), null, false
            )
        }
        verify {
            neo4jRepository.createRelationshipOfSubject(
                any(),
                any(),
                eq(fishContainmentUri)
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace a multi attribute relationship`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:92033f60-bb8b-4640-9464-bca23199ac".toUri()
        val relationshipTargetId = "urn:ngsi-ld:FishContainment:8792".toUri()
        val secondRelationshipTargetId = "urn:ngsi-ld:FishContainment:9792".toUri()

        val payload =
            """
            {
                "filledIn": [{
                    "type":"Relationship",
                    "object":"$relationshipTargetId",
                    "datasetId": "urn:ngsi-ld:Dataset:filledIn:1"
                },
                {
                   "type":"Relationship",
                   "object":"$secondRelationshipTargetId"
                }
            ]}
            """.trimIndent()

        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedRelationshipTarget = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        val datasetSetIds = slot<URI>()
        val createdRelationships = mutableListOf<Relationship>()

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")
        every { mockkedRelationship.type } returns listOf("Relationship")
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedRelationshipTarget.id } returns relationshipTargetId
        every { mockkedRelationship setProperty "observedAt" value any<ZonedDateTime>() } answers { value }

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { neo4jRepository.hasRelationshipInstance(any(), any(), capture(datasetSetIds)) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any(), any()) } returns 1
        every { neo4jRepository.createRelationshipOfSubject(any(), capture(createdRelationships), any()) } returns true

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        assertEquals(datasetSetIds.captured, "urn:ngsi-ld:Dataset:filledIn:1".toUri())
        assertTrue(
            createdRelationships.any {
                it.type == listOf("https://ontology.eglobalmark.com/aquac#filledIn") &&
                    it.datasetId == "urn:ngsi-ld:Dataset:filledIn:1".toUri()
            }.and(
                createdRelationships.any {
                    it.type == listOf("https://ontology.eglobalmark.com/aquac#filledIn") &&
                        it.datasetId == null
                }
            )
        )
        verify { neo4jRepository.hasRelationshipInstance(any(), eq("filledIn"), any()) }
        verify(exactly = 2) {
            neo4jRepository.deleteEntityRelationship(match { it.id == sensorId }, eq("filledIn"), any())
        }
        verify(exactly = 2) {
            neo4jRepository.createRelationshipOfSubject(match { it.id == sensorId }, any(), any())
        }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing default property`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val payload =
            """
            {
              "fishAge": {
                "type": "Property",
                "value": 5,
                "unitCode": "months"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasPropertyInstance(any(), any(), any()) }

        confirmVerified()
    }

    @Test
    fun `it should replace a multi attribute property`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val payload =
            """
            {
                "fishAge": [{
                    "type":"Property",
                    "value": 5,
                    "datasetId": "urn:ngsi-ld:Dataset:fishAge:1"
                },
                {
                   "type":"Property",
                   "value": 9
                }
            ]}
            """.trimIndent()
        val datasetSetIds = slot<URI>()
        val updatedInstances = mutableListOf<Property>()

        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { neo4jRepository.hasPropertyInstance(any(), any(), capture(datasetSetIds)) } returns true
        every { neo4jRepository.createPropertyOfSubject(any(), capture(updatedInstances)) } returns true

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        assertEquals(datasetSetIds.captured, "urn:ngsi-ld:Dataset:fishAge:1".toUri())
        assertTrue(
            updatedInstances.any {
                it.value == 5 &&
                    it.datasetId == "urn:ngsi-ld:Dataset:fishAge:1".toUri()
            }.and(
                updatedInstances.any {
                    it.value == 9 &&
                        it.datasetId == null
                }
            )
        )

        verify { neo4jRepository.hasPropertyInstance(any(), any(), any()) }
        verify {
            neo4jRepository.createPropertyOfSubject(
                match { it.id == sensorId && it.label == "Entity" },
                any()
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing property having the given datasetId`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val payload =
            """
            {
              "fishAge": {
                "type": "Property",
                "value": 5,
                "unitCode": "months",
                "datasetId": "urn:ngsi-ld:Dataset:fishAge:1"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasPropertyInstance(any(), any(), "urn:ngsi-ld:Dataset:fishAge:1".toUri()) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing geoProperty`() {
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ".toUri()
        val payload =
            """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      9.30623,
                      8.07966
                    ]
                  }
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, AQUAC_COMPOUND_CONTEXT))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify {
            neo4jRepository.updateLocationPropertyOfEntity(
                sensorId,
                match {
                    it.coordinates == listOf(9.30623, 8.07966)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should correctly parse Point location property for an entity`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(23.45, 67.87)

        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        entityService.createLocationProperty(entityId, "location", ngsiLdGeoProperty.instances[0])

        verify {
            neo4jRepository.addLocationPropertyToEntity(
                entityId,
                match {
                    it.coordinates == listOf(23.45, 67.87)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should correctly parse Polygon location property for an entity`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val coordinates = listOf(
            listOf(23.25, 67.80),
            listOf(83.49, 17.87),
            listOf(13.55, 63.37),
            listOf(21.45, 60.87)
        )
        val ngsiLdGeoProperty = parseLocationFragmentToPolygonGeoProperty(coordinates)

        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        entityService.createLocationProperty(entityId, "location", ngsiLdGeoProperty.instances[0])

        verify {
            neo4jRepository.addLocationPropertyToEntity(
                entityId,
                match {
                    it.geoPropertyType == GeoPropertyType.Polygon &&
                        it.coordinates == coordinates
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a temporal property with all provided attributes`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val ngsiLdPropertyInstance = NgsiLdPropertyInstance(
            "temperature",
            mapOf(
                NGSILD_PROPERTY_VALUE to listOf(
                    mapOf(
                        "@type" to listOf(NGSILD_PROPERTY_TYPE),
                        "@value" to 250
                    )
                ),
                NGSILD_UNIT_CODE_PROPERTY to listOf(
                    mapOf("@value" to "kg")
                ),
                NGSILD_OBSERVED_AT_PROPERTY to listOf(
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to "2019-12-18T10:45:44.248755Z"
                    )
                )
            )
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns true

        entityService.createEntityProperty(entityId, "temperature", ngsiLdPropertyInstance)

        verify {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                match {
                    it.name == "temperature" &&
                        it.value == 250 &&
                        it.unitCode == "kg" &&
                        it.observedAt?.toNgsiLdFormat() == "2019-12-18T10:45:44.248755Z"
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a new relationship`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val targetEntityId = "urn:ngsi-ld:Beekeeper:654321".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:${UUID.randomUUID()}".toUri()
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify {
            neo4jRepository.hasRelationshipInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo",
                null
            )
        }
        verify {
            neo4jRepository.createRelationshipOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any(), eq(targetEntityId)
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a new multi attribute relationship`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val targetEntityId = "urn:ngsi-ld:Beekeeper:654321".toUri()
        val secondTargetEntityId = "urn:ngsi-ld:Beekeeper:754321".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:${UUID.randomUUID()}".toUri()
        val newRelationship =
            """
            {
                "connectsTo": [{
                    "type":"Relationship",
                    "object":"$targetEntityId",
                    "datasetId": "urn:ngsi-ld:Dataset:connectsTo:1"
                },
                {
                   "type":"Relationship",
                   "object":"$secondTargetEntityId"
                }
            ]}
            """.trimIndent()

        val expandedNewRelationship =
            parseToNgsiLdAttributes(expandJsonLdFragment(newRelationship, AQUAC_COMPOUND_CONTEXT))

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)
        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId
        val datasetSetIds = mutableListOf<URI>()
        val createdRelationships = mutableListOf<Relationship>()
        every { neo4jRepository.hasRelationshipInstance(any(), any(), capture(datasetSetIds)) } returns false
        every {
            neo4jRepository.createRelationshipOfSubject(any(), capture(createdRelationships), any())
        } returns true

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        assertTrue(datasetSetIds.contains("urn:ngsi-ld:Dataset:connectsTo:1".toUri()))
        assertTrue(
            createdRelationships.any {
                it.type == listOf("https://ontology.eglobalmark.com/egm#connectsTo") &&
                    it.datasetId == "urn:ngsi-ld:Dataset:connectsTo:1".toUri()
            }.and(
                createdRelationships.any {
                    it.type == listOf("https://ontology.eglobalmark.com/egm#connectsTo") &&
                        it.datasetId == null
                }
            )
        )

        verify(exactly = 2) {
            neo4jRepository.hasRelationshipInstance(match { it.id == entityId && it.label == "Entity" }, any(), any())
        }
        verify(exactly = 2) {
            neo4jRepository.createRelationshipOfSubject(
                match { it.id == entityId && it.label == "Entity" },
                any(),
                any()
            )
        }
        verify { neo4jRepository.updateEntityModifiedDate(eq(entityId)) }
        confirmVerified()
    }

    @Test
    fun `it should not override the default relationship instance if overwrite is disallowed`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"urn:ngsi-ld:Beekeeper:654321"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, AQUAC_COMPOUND_CONTEXT)
        )

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, true)

        verify {
            neo4jRepository.hasRelationshipInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo",
                null
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should overwrite the relationship instance with given datasetId`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val targetEntityId = "urn:ngsi-ld:Beekeeper:654321".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:${UUID.randomUUID()}".toUri()
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId",
                    "datasetId": "urn:ngsi-ld:Dataset:connectsTo:1"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any(), any()) } returns 1
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify {
            neo4jRepository.hasRelationshipInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo",
                "urn:ngsi-ld:Dataset:connectsTo:1".toUri()
            )
        }
        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo", "urn:ngsi-ld:Dataset:connectsTo:1".toUri(), false
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a new property`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newProperty =
            """
            {
              "fishNumber": {
                "type": "Property",
                "value": 500
              }
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()

        every { neo4jRepository.hasPropertyInstance(any(), any()) } returns false
        every { mockkedEntity.id } returns entityId
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewProperty, false)

        verify {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        verify {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                match {
                    it.value == 500 &&
                        it.name == "https://ontology.eglobalmark.com/aquac#fishNumber"
                }
            )
        }
        verify { neo4jRepository.updateEntityModifiedDate(eq(entityId)) }

        confirmVerified()
    }

    @Test
    fun `it should create a new multi attribute property`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newProperty =
            """
            {
              "fishNumber": [{
                "type": "Property",
                "value": 500,
                "datasetId": "urn:ngsi-ld:Dataset:fishNumber:1"
              },
              {
                "type": "Property",
                "value": 600
              }]
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()

        val datasetSetIds = mutableListOf<URI>()
        val createdProperties = mutableListOf<Property>()

        every { neo4jRepository.hasPropertyInstance(any(), any(), capture(datasetSetIds)) } returns false
        every {
            neo4jRepository.createPropertyOfSubject(any(), capture(createdProperties))
        } returns true

        entityService.appendEntityAttributes(entityId, expandedNewProperty, false)

        assertTrue(datasetSetIds.contains("urn:ngsi-ld:Dataset:fishNumber:1".toUri()))

        assertTrue(
            createdProperties.any {
                it.value == 500 &&
                    it.name == "https://ontology.eglobalmark.com/aquac#fishNumber" &&
                    it.datasetId == "urn:ngsi-ld:Dataset:fishNumber:1".toUri()
            }.and(
                createdProperties.any {
                    it.value == 600 &&
                        it.name == "https://ontology.eglobalmark.com/aquac#fishNumber" &&
                        it.datasetId == null
                }
            )
        )

        verify(exactly = 2) {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber",
                datasetId = any()
            )
        }

        verify(exactly = 2) {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any()
            )
        }

        verify { neo4jRepository.updateEntityModifiedDate(eq(entityId)) }

        confirmVerified()
    }

    @Test
    fun `it should overwrite the property instance with given datasetId`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()

        val newProperty =
            """
            {
              "fishNumber": {
                "type": "Property",
                "value": 500,
                "datasetId": "urn:ngsi-ld:Dataset:fishNumber:1"
              }
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, AQUAC_COMPOUND_CONTEXT)
        )

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { neo4jRepository.deleteEntityProperty(any(), any(), any()) } returns 1
        every {
            neo4jRepository.createPropertyOfSubject(any(), any())
        } returns true

        entityService.appendEntityAttributes(entityId, expandedNewProperty, false)

        verify {
            neo4jRepository.deleteEntityProperty(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber",
                datasetId = "urn:ngsi-ld:Dataset:fishNumber:1".toUri()
            )
        }

        verify {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                match {
                    it.name == "https://ontology.eglobalmark.com/aquac#fishNumber" &&
                        it.value == 500 &&
                        it.datasetId == "urn:ngsi-ld:Dataset:fishNumber:1".toUri()
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not override the default property instance if overwrite is disallowed`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newProperty =
            """
            {
              "fishNumber": [{
                "type": "Property",
                "value": 600
              }]
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)

        every { neo4jRepository.hasPropertyInstance(any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewProperty, true)

        verify {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber",
                datasetId = null
            )
        }

        verify(inverse = true) {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any()
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should create a new geoproperty`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newGeoProperty =
            """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      29.30623,
                      83.07966
                    ]
                  }
              }
            }
            """.trimIndent()

        val expandedNewGeoProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newGeoProperty, AQUAC_COMPOUND_CONTEXT)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns false
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity
        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        entityService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify {
            neo4jRepository.addLocationPropertyToEntity(
                entityId,
                match {
                    it.coordinates == listOf(29.30623, 83.07966)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace a geoproperty if overwrite is allowed`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()
        val newGeoProperty =
            """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      29.30623,
                      83.07966
                    ]
                  }
              }
            }
            """.trimIndent()

        val expandedNewGeoProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newGeoProperty, AQUAC_COMPOUND_CONTEXT)
        )

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1

        entityService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify {
            neo4jRepository.updateLocationPropertyOfEntity(
                entityId,
                match {
                    it.coordinates == listOf(29.30623, 83.07966)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should delete all entity property instances`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.deleteEntityProperty(any(), any(), any()) } returns 1

        entityService.deleteEntityAttribute(entityId, "https://ontology.eglobalmark.com/aquac#fishNumber")

        verify {
            neo4jRepository.hasPropertyOfName(
                match {
                    it.id == "urn:ngsi-ld:Beehive:123456".toUri() &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        verify {
            neo4jRepository.deleteEntityProperty(
                match {
                    it.id == "urn:ngsi-ld:Beehive:123456".toUri() &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber", null, true
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should delete an entity relationship instance with the provided datasetId`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns false
        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any(), any(), any()) } returns 1

        entityService.deleteEntityAttributeInstance(
            entityId,
            "https://ontology.eglobalmark.com/aquac#connectsTo",
            "urn:ngsi-ld:Dataset:connectsTo:01".toUri()
        )

        verify {
            neo4jRepository.hasRelationshipInstance(
                match {
                    it.id == "urn:ngsi-ld:Beehive:123456".toUri() &&
                        it.label == "Entity"
                },
                any(), "urn:ngsi-ld:Dataset:connectsTo:01".toUri()
            )
        }

        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == "urn:ngsi-ld:Beehive:123456".toUri() &&
                        it.label == "Entity"
                },
                any(), "urn:ngsi-ld:Dataset:connectsTo:01".toUri(), false
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not delete all entity attribute instances if the attribute is not found`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns false
        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns false

        val exception = assertThrows<ResourceNotFoundException>(
            "Attribute fishNumber not found in entity urn:ngsi-ld:Beehive:123456"
        ) {
            entityService.deleteEntityAttribute(
                entityId, "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        assertEquals(
            "Attribute https://ontology.eglobalmark.com/aquac#fishNumber not found " +
                "in entity urn:ngsi-ld:Beehive:123456",
            exception.message
        )
        confirmVerified()
    }

    @Test
    fun `it should not delete the default property instance if not found`() {
        val entityId = "urn:ngsi-ld:Beehive:123456".toUri()

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false

        val exception = assertThrows<ResourceNotFoundException>(
            "Default instance of fishNumber not found in entity urn:ngsi-ld:Beehive:123456"
        ) {
            entityService.deleteEntityAttributeInstance(
                entityId, "https://ontology.eglobalmark.com/aquac#fishNumber", null
            )
        }
        assertEquals(
            "Default instance of https://ontology.eglobalmark.com/aquac#fishNumber not found " +
                "in entity urn:ngsi-ld:Beehive:123456",
            exception.message
        )
        confirmVerified()
    }
}

package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [IAMListener::class])
@ActiveProfiles("test")
class IAMListenerTests {

    @Autowired
    private lateinit var iamListener: IAMListener

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @Test
    fun `it should parse and transmit user creation event`() {
        val userCreateEvent = loadSampleData("events/authorization/UserCreateEvent.json")

        iamListener.processMessage(userCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUri() &&
                        it.properties.size == 1 &&
                        it.properties[0].compactName == "username" &&
                        it.properties[0].instances.size == 1 &&
                        it.properties[0].instances[0].value == "stellio"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit user deletion event`() {
        val userDeleteEvent = loadSampleData("events/authorization/UserDeleteEvent.json")

        iamListener.processMessage(userDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUri()) }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group creation event`() {
        val groupCreateEvent = loadSampleData("events/authorization/GroupCreateEvent.json")

        iamListener.processMessage(groupCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group update event`() {
        val groupUpdateEvent = loadSampleData("events/authorization/GroupUpdateEvent.json")

        iamListener.processMessage(groupUpdateEvent)

        verify {
            entityService.updateEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri(),
                match {
                    it.size == 1 &&
                        it[0].compactName == "name" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value == "EGM Team"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group deletion event`() {
        val groupDeleteEvent = loadSampleData("events/authorization/GroupDeleteEvent.json")

        iamListener.processMessage(groupDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:Group:a11c00f9-43bc-47a8-9d23-13d67696bdb8".toUri()) }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit client creation event`() {
        val clientCreateEvent = loadSampleData("events/authorization/ClientCreateEvent.json")

        iamListener.processMessage(clientCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:Client:191a6f0d-df07-4697-afde-da9d8a91d954".toUri() &&
                        it.properties.size == 1 &&
                        it.properties[0].compactName == "clientId" &&
                        it.properties[0].instances.size == 1 &&
                        it.properties[0].instances[0].value == "stellio-client"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit client deletion event`() {
        val clientDeleteEvent = loadSampleData("events/authorization/ClientDeleteEvent.json")

        iamListener.processMessage(clientDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:Client:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUri()) }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group membership append event`() {
        val groupMembershipAppendEvent = loadSampleData("events/authorization/GroupMembershipAppendEvent.json")

        iamListener.processMessage(groupMembershipAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:User:96e1f1e9-d798-48d7-820e-59f5a9a2abf5".toUri(),
                match {
                    it.size == 1 &&
                        it[0].name == "https://ontology.eglobalmark.com/authorization#isMemberOf" &&
                        it[0] is NgsiLdRelationship &&
                        (it[0] as NgsiLdRelationship).instances[0].datasetId ==
                        "urn:ngsi-ld:Dataset:7cdad168-96ee-4649-b768-a060ac2ef435".toUri() &&
                        (it[0] as NgsiLdRelationship).instances[0].objectId ==
                        "urn:ngsi-ld:Group:7cdad168-96ee-4649-b768-a060ac2ef435".toUri()
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group membership deletion event`() {
        val groupMembershipDeleteEvent = loadSampleData("events/authorization/GroupMembershipDeleteEvent.json")

        iamListener.processMessage(groupMembershipDeleteEvent)

        verify {
            entityService.deleteEntityAttributeInstance(
                "urn:ngsi-ld:User:96e1f1e9-d798-48d7-820e-59f5a9a2abf5".toUri(),
                "https://ontology.eglobalmark.com/authorization#isMemberOf",
                "urn:ngsi-ld:Dataset:7cdad168-96ee-4649-b768-a060ac2ef435".toUri()
            )
        }
    }

    @Test
    fun `it should parse and transmit role update event with two roles`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventTwoRoles.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri(),
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is List<*> &&
                        ((it[0] as NgsiLdProperty).instances[0].value as List<*>)
                            .containsAll(setOf(GlobalRole.STELLIO_ADMIN.key, GlobalRole.STELLIO_CREATOR.key))
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event with one role`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventOneRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri(),
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is String &&
                        (it[0] as NgsiLdProperty).instances[0].value == GlobalRole.STELLIO_ADMIN.key
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event with no roles`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventNoRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri(),
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is List<*> &&
                        ((it[0] as NgsiLdProperty).instances[0].value as List<*>).isEmpty()
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event for a client`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendToClient.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Client:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri(),
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles"
                },
                false
            )
        }
        confirmVerified()
    }
}

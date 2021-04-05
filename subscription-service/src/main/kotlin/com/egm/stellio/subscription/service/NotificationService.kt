package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.toKeyValues
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.egm.stellio.subscription.firebase.FCMService
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import java.net.URI

@Service
class NotificationService(
    private val subscriptionService: SubscriptionService,
    private val subscriptionEventService: SubscriptionEventService,
    private val fcmService: FCMService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun notifyMatchingSubscribers(
        rawEntity: String,
        ngsiLdEntity: NgsiLdEntity,
        updatedAttributes: Set<String>
    ): Mono<List<Triple<Subscription, Notification, Boolean>>> {
        val id = ngsiLdEntity.id
        val type = ngsiLdEntity.type
        return subscriptionService.getMatchingSubscriptions(id, type, updatedAttributes.joinToString(separator = ","))
            .filter {
                subscriptionService.isMatchingQuery(it.q, rawEntity)
            }
            .filterWhen {
                subscriptionService.isMatchingGeoQuery(it.id, ngsiLdEntity.getLocation())
            }
            .flatMap {
                callSubscriber(it, id, expandJsonLdEntity(rawEntity, ngsiLdEntity.contexts))
            }
            .collectList()
    }

    fun callSubscriber(
        subscription: Subscription,
        entityId: URI,
        entity: JsonLdEntity
    ): Mono<Triple<Subscription, Notification, Boolean>> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val notification = Notification(
            subscriptionId = subscription.id,
            data = buildNotificationData(entity, subscription.notification, mediaType)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")
        if (uri == "urn:embedded:firebase") {
            val fcmDeviceToken = subscription.notification.endpoint.getInfoValue("deviceToken")
            return callFCMSubscriber(entityId, subscription, notification, fcmDeviceToken)
        } else {
            // TODO if mediaType is JSON, add a Link header pointing to the entity's contexts
            // TODO if entity has more than one context, we need to generate a compound link
            val request =
                WebClient.create(uri).post().contentType(mediaType).headers {
                    if (mediaType == MediaType.APPLICATION_JSON)
                        it.set(HttpHeaders.LINK, buildContextLinkHeader(entity.contexts[0]))
                    subscription.notification.endpoint.info?.forEach { endpointInfo ->
                        it.set(endpointInfo.key, endpointInfo.value)
                    }
                }
            return request
                .bodyValue(serializeObject(notification))
                .exchange()
                .doOnError { e -> logger.error("Failed to send notification to $uri: ${e.message}") }
                .map {
                    val success = it.statusCode().is2xxSuccessful
                    logger.info("The notification has been sent with ${if (success) "success" else "failure"}")
                    if (!success) {
                        it.bodyToMono<String>().subscribe { body ->
                            logger.warn("Got error response status: ${it.statusCode()}")
                            logger.warn("Got error response body: $body")
                        }
                    }
                    Triple(subscription, notification, success)
                }
                .doOnNext {
                    subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third).subscribe()
                }
                .doOnNext {
                    subscriptionEventService.publishNotificationEvent(
                        EntityCreateEvent(
                            it.second.id,
                            serializeObject(it.second),
                            listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
                        )
                    )
                }
        }
    }

    private fun buildNotificationData(
        entity: JsonLdEntity,
        params: NotificationParams,
        mediaType: MediaType
    ): List<Map<String, Any>> {
        val filteredEntity =
            filterJsonLdEntityOnAttributes(entity, params.attributes?.toSet() ?: emptySet())
        val filteredCompactedEntity =
            compact(JsonLdEntity(filteredEntity, entity.contexts), entity.contexts, mediaType)
        val processedEntity = if (params.format == NotificationParams.FormatType.KEY_VALUES)
            filteredCompactedEntity.toKeyValues()
        else
            filteredCompactedEntity

        return listOf(processedEntity)
    }

    fun callFCMSubscriber(
        entityId: URI,
        subscription: Subscription,
        notification: Notification,
        fcmDeviceToken: String?
    ): Mono<Triple<Subscription, Notification, Boolean>> {
        if (fcmDeviceToken == null) {
            return subscriptionService.updateSubscriptionNotification(subscription, notification, false)
                .map {
                    Triple(subscription, notification, false)
                }
        }

        val response = fcmService.sendMessage(
            mapOf("title" to subscription.name, "body" to subscription.description),
            mapOf(
                "id_alert" to notification.id.toString(),
                "id_subscription" to subscription.id.toString(),
                "timestamp" to notification.notifiedAt.toNgsiLdFormat(),
                "id_beehive" to entityId.toString()
            ),
            fcmDeviceToken
        )

        val success = response != null
        return subscriptionService.updateSubscriptionNotification(subscription, notification, success)
            .doOnNext {
                subscriptionEventService.publishNotificationEvent(
                    EntityCreateEvent(
                        notification.id,
                        serializeObject(notification),
                        listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
                    )
                )
            }
            .map {
                Triple(subscription, notification, success)
            }
    }
}

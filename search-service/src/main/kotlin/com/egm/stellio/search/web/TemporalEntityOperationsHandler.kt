package com.egm.stellio.search.web

import com.egm.stellio.search.service.EntityAccessRightsService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
class TemporalEntityOperationsHandler(
    private val queryService: QueryService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val entityAccessRightsService: EntityAccessRightsService
) {

    /**
     * Partial implementation of 6.24.3.1 - Query Temporal Evolution of Entities With POST
     */
    @PostMapping("/query", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val body = requestBody.awaitFirst()
        val params = JsonUtils.deserializeObject(body)
        val queryParams = LinkedMultiValueMap<String, String>()
        params.forEach {
            if (it.value is List<*>)
                queryParams.add(it.key, (it.value as List<*>).joinToString(","))
            else
                queryParams.add(it.key, it.value.toString())
        }

        val temporalEntitiesQuery = queryService.parseAndCheckQueryParams(queryParams, contextLink)

        val accessRightFilter = entityAccessRightsService.computeAccessRightFilter(sub)
        val temporalEntities = queryService.queryTemporalEntities(
            temporalEntitiesQuery,
            contextLink,
            accessRightFilter
        )
        val temporalEntityCount = temporalEntityAttributeService.getCountForEntities(
            temporalEntitiesQuery.ids,
            temporalEntitiesQuery.types,
            temporalEntitiesQuery.temporalQuery.expandedAttrs,
            accessRightFilter
        ).awaitFirst()

        val prevAndNextLinks = PagingUtils.getPagingLinks(
            "/ngsi-ld/v1/temporal/entities",
            queryParams,
            temporalEntityCount,
            temporalEntitiesQuery.offset,
            temporalEntitiesQuery.limit
        )

        return PagingUtils.buildPaginationResponse(
            (serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) })),
            temporalEntityCount,
            temporalEntitiesQuery.count,
            prevAndNextLinks,
            mediaType,
            contextLink
        )
    }
}

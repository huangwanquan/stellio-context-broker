package com.egm.stellio.entity.model

import java.net.URI

data class AttributeDetails(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val typeNames: List<String>
)

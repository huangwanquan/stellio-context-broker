package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import java.net.URI
import java.net.URISyntaxException

fun String.toUri(): URI =
    try {
        URI(this)
    } catch (e: URISyntaxException) {
        throw BadRequestDataException("The supplied identifier was expected to be an URI but it is not: $this")
    }

fun List<String>.toListOfUri(): List<URI> =
    this.map { it.toUri() }

fun List<URI>.toListOfString(): List<String> =
    this.map { it.toString() }
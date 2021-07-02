package com.github.jtaylordev.cloudstore

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import org.springframework.http.codec.multipart.Part
import java.io.ByteArrayOutputStream

class MultipartFile(
    val filename: String,
    private val parts: List<Part>,
) {
    /**
     * Reads each part of the file into ByteArrays
     *
     * @return Flow of part ByteArrays.
     */
    fun bytes(): Flow<ByteArray> {
        return parts.asFlow().transform { part ->
            val out = ByteArrayOutputStream()
            part.content().asFlow().collect {
                if (out.size() >= MAX_PART_SIZE_MB) {
                    emit(out.toByteArray())
                    out.reset()
                }
                out.writeBytes(it.asByteBuffer().array())
            }
            emit(out.toByteArray())
        }.flowOn(Dispatchers.IO)
    }

    companion object {
        private const val MAX_PART_SIZE_MB = 10 * 1024 * 1024
    }
}

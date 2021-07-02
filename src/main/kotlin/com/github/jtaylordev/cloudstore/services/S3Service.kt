package com.github.jtaylordev.cloudstore.services

import com.github.jtaylordev.cloudstore.MultipartFile
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import java.util.concurrent.CompletableFuture

class S3Service {
    @Value("\${s3.bucketName}")
    private val bucketName: String = ""

    @Value("\${s3.maxListKeys}")
    private val maxListKeys: Int = 0

    private val client: S3AsyncClient = S3AsyncClient.create()

    private val logger = LoggerFactory.getLogger(S3Service::class.java)

    /**
     * @param prefix
     * Limits keys to those beginning with this value
     *
     * @return Returns Result<List<String>> of object keys, or Result.Failure if exception occurs
     */
    suspend fun listObjectKeys(prefix: String): Result<List<String>> {
        var req = ListObjectsV2Request.builder().apply {
            bucket(bucketName)
            prefix(prefix)
            maxKeys(maxListKeys)
        }.build()

        lateinit var result: ListObjectsV2Response
        val keys = mutableListOf<String>()

        return try {
            do {
                result = client.listObjectsV2(req).await()

                keys.addAll(result.contents().map {
                    it.key().substring(prefix.length)
                })

                req = req.toBuilder().continuationToken(result.continuationToken()).build()
            } while (result.isTruncated)

            Result.success(keys)
        } catch (e: SdkException) {
            logger.error("listObjectKeys got SdkException: $e")
            Result.failure(e)
        }
    }

    /**
     * @param prefix
     * Value to prepend to start of filename
     *
     * @param file
     * File to upload
     *
     * @return Returns Result<String> with file.filename on success, or Result.Failure if exception occurs
     */
    suspend fun uploadObject(prefix: String, file: MultipartFile): Result<String> {
        val key = "$prefix/${file.filename}"

        val uploadId = try {
            createMultipartUpload(key).uploadId()
        } catch (e: Exception) {
            return Result.failure(e)
        }

        try {
            val parts = mutableListOf<CompletedPart>()

            // Read ByteArray corresponding to each FilePart, create a multipart UploadPartRequest for each,
            // and lastly await and collect the CompletedPart
            file.bytes().withIndex().map { (index, partBytes) ->
                val partNumber = index + 1

                getCompletedPart(partNumber, uploadPart(key, uploadId, partNumber, partBytes))
            }.collect {
                parts.add(it)
            }

            completeMultipartUpload(key, uploadId, parts)
        } catch (e: Exception) {
            logger.error("Failed to do a part of upload: $e")
            withContext(NonCancellable) {
                abortMultipartUpload(key, uploadId)
            }
            return Result.failure(e)
        }

        return Result.success(key.substring(prefix.length + 1))
    }

    private suspend fun createMultipartUpload(key: String): CreateMultipartUploadResponse {
        val createRequest = CreateMultipartUploadRequest.builder().apply {
            bucket(bucketName)
            key(key)
        }.build()

        return client.createMultipartUpload(createRequest).await()
    }

    private suspend fun uploadPart(
        key: String,
        uploadId: String,
        partNumber: Int,
        partBytes: ByteArray
    ): UploadPartResponse {
        // Create one UploadPart for the current ByteArray
        logger.info("Uploading part #$partNumber of $key on thread ${Thread.currentThread()}")
        val partRequest = UploadPartRequest.builder().apply {
            bucket(bucketName)
            key(key)
            contentLength(partBytes.size.toLong())
            uploadId(uploadId)
            partNumber(partNumber)
        }.build()

        return client.uploadPart(partRequest, AsyncRequestBody.fromBytes(partBytes)).await()
    }

    private fun getCompletedPart(
        partNumber: Int,
        partResponse: UploadPartResponse
    ): CompletedPart {
        logger.info("Completing part #$partNumber")
        return CompletedPart.builder().apply {
            partNumber(partNumber)
            eTag(partResponse.eTag())
        }.build()
    }

    private suspend fun completeMultipartUpload(key: String, uploadId: String, parts: List<CompletedPart>) {
        val completedUpload = CompletedMultipartUpload.builder().apply {
            parts(parts)
        }.build()

        val completedRequest = CompleteMultipartUploadRequest.builder().apply {
            bucket(bucketName)
            key(key)
            uploadId(uploadId)
            multipartUpload(completedUpload)
        }.build()

        logger.info("Completing multipart upload for $key with ${parts.size} parts")
        client.completeMultipartUpload(completedRequest).await()
    }

    private suspend fun abortMultipartUpload(key: String, uploadId: String) {
        val abortRequest = AbortMultipartUploadRequest.builder().apply {
            bucket(bucketName)
            key(key)
            uploadId(uploadId)
        }.build()
        client.abortMultipartUpload(abortRequest).await()
        logger.error("Aborted multipart upload")
    }
}
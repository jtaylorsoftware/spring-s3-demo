package com.github.jtaylordev.cloudstore.routes

import com.github.jtaylordev.cloudstore.MultipartFile
import com.github.jtaylordev.cloudstore.services.S3Service
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.codec.multipart.FilePart
import org.springframework.http.codec.multipart.Part
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.web.reactive.function.server.*
import java.net.URI

class S3Handler(private val s3Service: S3Service) {
    private val logger = LoggerFactory.getLogger(S3Service::class.java)

    /**
     * GET /objects
     *
     * @return List of object keys belonging to a user at a given path
     */
    suspend fun getObjectKeysForUser(request: ServerRequest): ServerResponse {
        val username = request.jwt()?.let {
            it.tokenAttributes["cognito:username"]?.toString()
        } ?: return internalServerError()

        return s3Service.listObjectKeys(username).getOrNull()?.let {
            ServerResponse.ok().bodyValueAndAwait(it)
        } ?: internalServerError()
    }


    /**
     * POST /objects
     *
     * Uploads a file for a user
     */
    suspend fun uploadObject(request: ServerRequest): ServerResponse {
        val username = request.jwt()?.let {
            it.tokenAttributes["cognito:username"]?.toString()
        } ?: return badRequest()

        val fileParts: List<Part> =
            request.awaitMultipartData()["file"] ?: return badRequest()
        val filename = (fileParts[0] as? FilePart)?.filename() ?: return badRequest()
        val file = MultipartFile(filename, fileParts)

        logger.info("Uploading file $filename with ${fileParts.size} part(s)")

        return s3Service.uploadObject(username, file).getOrNull()?.let {
            ServerResponse.created(URI.create("${request.path()}/$it")).buildAndAwait()
        } ?: internalServerError()
    }

}

suspend fun internalServerError() = ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).buildAndAwait()
suspend fun badRequest() = ServerResponse.badRequest().buildAndAwait()

suspend fun ServerRequest.jwt(): JwtAuthenticationToken? =
    awaitPrincipal()?.let { principal ->
        principal as? JwtAuthenticationToken
    }


class S3Routes(private val handler: S3Handler) {
    fun router() = coRouter {
        GET("/objects", handler::getObjectKeysForUser)
        accept(MediaType.MULTIPART_FORM_DATA).nest {
            POST("/objects", handler::uploadObject)
        }
    }
}
package com.cachepulse.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Request;
import feign.RequestTemplate;
import feign.Response;
import feign.Target;
import feign.codec.ErrorDecoder;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FeignSupportTest {
    private final ErrorDecoder decoder = FeignSupport.errorDecoder(new ObjectMapper());

    @Test
    void problemDetailBodyBecomesRemoteCallExceptionWithStatusCodeAndTraceId() {
        String body = """
                {"type":"about:blank","title":"Not Found","status":404,"detail":"no event 7",
                 "code":"NOT_FOUND","traceId":"abc123"}
                """;

        Exception error = decoder.decode("ArchiveClient#event(String)", response(404, body));

        assertThat(error).isInstanceOfSatisfying(RemoteCallException.class, remote -> {
            assertThat(remote.service()).isEqualTo("archive-service");
            assertThat(remote.remoteStatus()).isEqualTo(404);
            assertThat(remote.code()).isEqualTo("NOT_FOUND");
            assertThat(remote.getMessage()).isEqualTo("no event 7");
            assertThat(remote.remoteTraceId()).isEqualTo("abc123");
            assertThat(remote.clientError()).isTrue();
            assertThat(remote.status()).isEqualTo(HttpStatus.BAD_GATEWAY);
        });
    }

    @Test
    void serverErrorMapsTo503() {
        Exception error = decoder.decode("ArchiveClient#event(String)",
                response(500, "{\"status\":500,\"code\":\"INTERNAL\",\"detail\":\"boom\"}"));

        assertThat(error).isInstanceOfSatisfying(RemoteCallException.class, remote -> {
            assertThat(remote.remoteStatus()).isEqualTo(500);
            assertThat(remote.code()).isEqualTo("INTERNAL");
            assertThat(remote.clientError()).isFalse();
            assertThat(remote.status()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        });
    }

    @Test
    void nonJsonBodyKeepsStatusWithGenericCode() {
        Exception error = decoder.decode("ArchiveClient#event(String)", response(503, "<html>down</html>"));

        assertThat(error).isInstanceOfSatisfying(RemoteCallException.class, remote -> {
            assertThat(remote.remoteStatus()).isEqualTo(503);
            assertThat(remote.code()).isEqualTo("REMOTE_ERROR");
            assertThat(remote.getMessage()).isEqualTo("HTTP 503 from archive-service");
            assertThat(remote.remoteTraceId()).isNull();
        });
    }

    @Test
    void frameworkMapperWritesProblemDetailFieldsAtTopLevelSoTheDecoderCanReadThem() throws Exception {
        ObjectMapper mapper = FrameworkAutoConfiguration.objectMapperWithProblemDetail();
        org.springframework.http.ProblemDetail problem =
                org.springframework.http.ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "no event 7");
        problem.setProperty("code", "NOT_FOUND");
        problem.setProperty("traceId", "abc123");

        String body = mapper.writeValueAsString(problem);

        assertThat(mapper.readTree(body).has("properties")).isFalse();
        Exception error = FeignSupport.errorDecoder(mapper).decode("ArchiveClient#event(String)", response(404, body));
        assertThat(error).isInstanceOfSatisfying(RemoteCallException.class, remote -> {
            assertThat(remote.code()).isEqualTo("NOT_FOUND");
            assertThat(remote.remoteTraceId()).isEqualTo("abc123");
        });
    }

    @Test
    void frameworkMapperKeepsOtherJsonUnchanged() throws Exception {
        assertThat(FrameworkAutoConfiguration.objectMapperWithProblemDetail().writeValueAsString(Map.of("a", 1)))
                .isEqualTo(new ObjectMapper().writeValueAsString(Map.of("a", 1)));
    }

    private static Response response(int status, String body) {
        RequestTemplate template = new RequestTemplate();
        template.feignTarget(new Target.HardCodedTarget<>(Object.class, "archive-service", "http://archive-service"));
        Request request = Request.create(Request.HttpMethod.GET, "http://archive-service/internal/events/7",
                Map.of(), null, StandardCharsets.UTF_8, template);
        return Response.builder().status(status).reason("error").request(request).headers(Map.of())
                .body(body, StandardCharsets.UTF_8).build();
    }
}

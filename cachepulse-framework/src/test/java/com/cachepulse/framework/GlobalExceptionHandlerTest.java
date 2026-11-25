package com.cachepulse.framework;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerTest {
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        TraceIds traceIds = mock(TraceIds.class);
        when(traceIds.current()).thenReturn("4bf92f3577b34da6a3ce929d0e0e4736");
        mvc = MockMvcBuilders.standaloneSetup(new FailingController())
                .setControllerAdvice(new GlobalExceptionHandler(traceIds))
                .build();
    }

    @Test
    void illegalArgumentIsBadRequest() throws Exception {
        expect("argument", 400, "BAD_REQUEST", "bad id");
    }

    @Test
    void noSuchElementIsNotFound() throws Exception {
        expect("missing", 404, "NOT_FOUND", "no event 7");
    }

    @Test
    void illegalStateIsConflict() throws Exception {
        expect("state", 409, "CONFLICT", "job already running");
    }

    @Test
    void remoteServerErrorOrUnavailableIs503AndRemoteClientErrorIs502() throws Exception {
        expect("remote?status=500", 503, "ARCHIVE_DOWN", "archive failed");
        expect("remote?status=0", 503, "ARCHIVE_DOWN", "archive failed");
        expect("remote?status=404", 502, "ARCHIVE_DOWN", "archive failed");
    }

    @Test
    void frameworkExceptionsKeepTheirOwnStatusAndCode() throws Exception {
        expect("client", 404, "NOT_FOUND", "no such subscription");
        expect("service", 503, "UNAVAILABLE", "kafka down");
    }

    @Test
    void responseStatusExceptionThrownByControllersAlsoGetsCodeAndTraceId() throws Exception {
        expect("status?code=404", 404, "NOT_FOUND", "Unknown event");
        expect("status?code=400", 400, "BAD_REQUEST", "Unknown event");
        expect("status?code=409", 409, "CONFLICT", "Unknown event");
    }

    private void expect(String path, int status, String code, String detail) throws Exception {
        mvc.perform(get("/fail/" + path))
                .andExpect(status().is(status))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(status))
                .andExpect(jsonPath("$.detail").value(detail))
                .andExpect(jsonPath("$.code").value(code))
                .andExpect(jsonPath("$.traceId").value("4bf92f3577b34da6a3ce929d0e0e4736"));
    }

    @RestController
    static class FailingController {
        @GetMapping("/fail/argument")
        String argument() {
            throw new IllegalArgumentException("bad id");
        }

        @GetMapping("/fail/missing")
        String missing() {
            throw new NoSuchElementException("no event 7");
        }

        @GetMapping("/fail/state")
        String state() {
            throw new IllegalStateException("job already running");
        }

        @GetMapping("/fail/remote")
        String remote(@RequestParam("status") int status) {
            throw new RemoteCallException("archive-service", status, "ARCHIVE_DOWN", "archive failed", "remote-trace", null);
        }

        @GetMapping("/fail/client")
        String client() {
            throw ClientException.notFound("no such subscription");
        }

        @GetMapping("/fail/service")
        String service() {
            throw ServiceException.unavailable("kafka down", null);
        }

        @GetMapping("/fail/status")
        String status(@RequestParam("code") int code) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatusCode.valueOf(code), "Unknown event");
        }
    }
}

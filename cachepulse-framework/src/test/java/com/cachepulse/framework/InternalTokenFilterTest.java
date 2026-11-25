package com.cachepulse.framework;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class InternalTokenFilterTest {
    private final InternalTokenFilter filter = new InternalTokenFilter("s3cret-token");

    @Test
    void internalPathWithoutTokenIsRejected() throws Exception {
        Result result = call(filter, "/internal/events/7", null);

        assertThat(result.response.getStatus()).isEqualTo(401);
        assertThat(result.response.getContentType()).startsWith("application/problem+json");
        assertThat(result.response.getContentAsString()).contains("INTERNAL_TOKEN_REQUIRED");
        assertThat(result.chain.getRequest()).isNull();
    }

    @Test
    void internalPathWithWrongTokenIsRejected() throws Exception {
        Result result = call(filter, "/internal/events/7", "s3cret-tokeN");

        assertThat(result.response.getStatus()).isEqualTo(401);
        assertThat(result.chain.getRequest()).isNull();
    }

    @Test
    void internalPathWithTheRightTokenPasses() throws Exception {
        Result result = call(filter, "/internal/events/7", "s3cret-token");

        assertThat(result.response.getStatus()).isEqualTo(200);
        assertThat(result.chain.getRequest()).isNotNull();
    }

    @Test
    void otherPathsPassWithoutToken() throws Exception {
        for (String path : new String[]{"/users/1", "/platform/events", "/internalish"}) {
            Result result = call(filter, path, null);
            assertThat(result.response.getStatus()).as(path).isEqualTo(200);
            assertThat(result.chain.getRequest()).as(path).isNotNull();
        }
    }

    @Test
    void withoutAConfiguredTokenNothingIsChecked() throws Exception {
        Result result = call(new InternalTokenFilter(""), "/internal/events/7", null);

        assertThat(result.chain.getRequest()).isNotNull();
    }

    private static Result call(InternalTokenFilter filter, String path, String token) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", path);
        if (token != null) {
            request.addHeader(InternalTokenFilter.HEADER, token);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, response, chain);
        return new Result(response, chain);
    }

    private record Result(MockHttpServletResponse response, MockFilterChain chain) {
    }
}

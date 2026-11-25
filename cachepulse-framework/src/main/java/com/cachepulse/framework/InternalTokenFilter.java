package com.cachepulse.framework;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * /internal/** 只给服务间调用：网关不转发这些路径，这里再校验共享的内部令牌，
 * 防止内部网络里的其他容器误调用。未配置令牌时不校验（本机开发）。
 */
public class InternalTokenFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Internal-Token";
    private final byte[] token;

    public InternalTokenFilter(String token) {
        this.token = token == null ? new byte[0] : token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return token.length == 0 || !request.getRequestURI().startsWith("/internal/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String presented = request.getHeader(HEADER);
        if (presented == null || !MessageDigest.isEqual(token, presented.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            response.getWriter().write("{\"status\":401,\"title\":\"Unauthorized\",\"code\":\"INTERNAL_TOKEN_REQUIRED\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}

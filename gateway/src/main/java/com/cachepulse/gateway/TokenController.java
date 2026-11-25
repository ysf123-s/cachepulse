package com.cachepulse.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;

/** 管理页面登录：用单个管理员账号换 30 分钟有效的 HS256 令牌。 */
@RestController
public class TokenController {
    private final GatewayProperties properties;
    private final PasswordEncoder passwords;
    private final JwtEncoder encoder;
    private final String passwordHash;
    private final TraceIdHeaderFilter traceIds;

    public TokenController(GatewayProperties properties, PasswordEncoder passwords, JwtEncoder encoder,
                           TraceIdHeaderFilter traceIds) {
        this.properties = properties;
        this.passwords = passwords;
        this.encoder = encoder;
        this.passwordHash = properties.requiredPasswordHash();
        this.traceIds = traceIds;
    }

    public record TokenRequest(String username, String password) { }

    public record TokenResponse(String accessToken, String tokenType, long expiresIn) { }

    @PostMapping("/auth/token")
    public Mono<ResponseEntity<Object>> token(@RequestBody TokenRequest request, ServerWebExchange exchange) {
        // BCrypt 校验耗 CPU，不能占用 Netty 的事件循环线程
        return Mono.fromCallable(() -> issue(request, exchange)).subscribeOn(Schedulers.boundedElastic());
    }

    private ResponseEntity<Object> issue(TokenRequest request, ServerWebExchange exchange) {
        String username = request == null || request.username() == null ? "" : request.username();
        String password = request == null || request.password() == null ? "" : request.password();
        // 账号不对也照样跑一次 BCrypt，响应时间不泄露账号是否存在
        boolean passwordMatches = passwords.matches(password, passwordHash);
        boolean userMatches = MessageDigest.isEqual(username.getBytes(StandardCharsets.UTF_8),
                properties.adminUsername().getBytes(StandardCharsets.UTF_8));
        if (!passwordMatches || !userMatches) {
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED, "Invalid username or password");
            problem.setProperty("code", "LOGIN_FAILED");
            problem.setProperty("traceId", traceIds.traceId(exchange));
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(problem);
        }
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("cachepulse-gateway")
                .subject(username)
                .issuedAt(now)
                .expiresAt(now.plus(properties.tokenTtl()))
                .build();
        String token = encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims))
                .getTokenValue();
        return ResponseEntity.ok(new TokenResponse(token, "Bearer", properties.tokenTtl().toSeconds()));
    }
}

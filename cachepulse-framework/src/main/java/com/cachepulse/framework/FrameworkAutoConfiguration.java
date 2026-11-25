package com.cachepulse.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import feign.RequestInterceptor;
import feign.codec.ErrorDecoder;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.json.ProblemDetailJacksonMixin;

/** 所有 Servlet 服务共用的基础设施。网关是 WebFlux，不引用这个模块。 */
@AutoConfiguration(before = JacksonAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class FrameworkAutoConfiguration {

    /**
     * 与原单体相同的 ObjectMapper（无额外特性，注册 JavaTimeModule）。表结构快照 ID 由它序列化后计算，
     * 缓存值、归档事件的 JSON 格式也依赖它，换成 Spring Boot 默认配置会让迁移过来的数据对不上。
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return objectMapperWithProblemDetail();
    }

    /**
     * 另加 ProblemDetail 的 mixin：Spring Boot 默认的 ObjectMapper 自带它，手工 new 的没有，
     * 少了它 code、traceId 会套在 "properties" 里，FeignSupport 的错误解码就读不到。只影响 ProblemDetail 本身。
     */
    static ObjectMapper objectMapperWithProblemDetail() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.addMixIn(ProblemDetail.class, ProblemDetailJacksonMixin.class);
        return mapper;
    }

    @Bean
    public TraceIds traceIds(ObjectProvider<Tracer> tracer) {
        return new TraceIds(tracer);
    }

    @Bean
    public GlobalExceptionHandler cachePulseExceptionHandler(TraceIds traceIds) {
        return new GlobalExceptionHandler(traceIds);
    }

    @Bean
    public FilterRegistrationBean<TraceIdResponseFilter> traceIdResponseFilter(TraceIds traceIds) {
        FilterRegistrationBean<TraceIdResponseFilter> bean = new FilterRegistrationBean<>(new TraceIdResponseFilter(traceIds));
        bean.setOrder(Ordered.LOWEST_PRECEDENCE - 10);
        return bean;
    }

    @Bean
    public FilterRegistrationBean<InternalTokenFilter> internalTokenFilter(
            @Value("${cachepulse.internal-token:}") String token) {
        FilterRegistrationBean<InternalTokenFilter> bean = new FilterRegistrationBean<>(new InternalTokenFilter(token));
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE + 100);
        return bean;
    }

    @Bean
    public RequestInterceptor cachePulseFeignHeaders(@Value("${cachepulse.internal-token:}") String token) {
        return FeignSupport.headers(token);
    }

    @Bean
    public ErrorDecoder cachePulseFeignErrorDecoder(ObjectMapper json) {
        return FeignSupport.errorDecoder(json);
    }
}

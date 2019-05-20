package org.openplacereviews.opendb.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Wraps each HTTP request with following aspects:
 * 1) setup RID in thread-local
 * 2) log HTTP request
 * 3) measure request timing
 */
@Configuration
public class HttpRequestFilter {
    private static final Logger log = LogManager.getLogger(HttpRequestFilter.class);

    public static final ThreadLocal<Long> START_MS = new ThreadLocal<>();

    @Bean
    public FilterRegistrationBean<FilterImpl> mdcFilter() {
        FilterRegistrationBean<FilterImpl> reg = new FilterRegistrationBean<>();
        FilterImpl filter = new FilterImpl();
        reg.setFilter(filter);
        reg.setOrder(Integer.MIN_VALUE);
        return reg;
    }

    public static class FilterImpl implements Filter {

        public FilterImpl() {
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            long startMs = System.currentTimeMillis();
            START_MS.set(startMs);
            String rid = httpRequest.getParameter("rid");
            if (rid == null) {
                rid = new StringBuilder(String.valueOf(startMs)).reverse().substring(0, 9);
            }

            ThreadContext.put("rid", rid);
            request.setAttribute("rid", rid);
            httpResponse.setHeader("rid", rid);

            logRequest(request);

            //complete processing
            chain.doFilter(request, response);

            long took = System.currentTimeMillis() - startMs;

            if (httpRequest.isAsyncStarted()) {
                log.info("[{} ms] HTTP thread released", took);
            } else {
                //sync request completion
                long tookMs = System.currentTimeMillis() - startMs;
                StringBuilder b = LoggingUtils.buildRequestTimingLogLine(tookMs);
                b.append("Request completed ~RESP");
                log.info(b);
            }

            ThreadContext.clearMap();
        }

        static void logRequest(ServletRequest request) {
            try {
                HttpServletRequest req = (HttpServletRequest) request;
                log.info(new StringBuilder("--> ").append(req.getMethod()).append(" ").append(req.getRequestURI())
                        .append(" (").append(request.getContentType()).append(") ~REQ"));
            } catch (Exception e) {
                log.error("Failed to log request/response pair", e);
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public void init(FilterConfig filterConfig) throws ServletException {
        }
    }
}
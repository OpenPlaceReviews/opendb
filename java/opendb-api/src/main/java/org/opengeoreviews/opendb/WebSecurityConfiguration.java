package org.opengeoreviews.opendb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	
    
    protected static final Log LOG = LogFactory.getLog(WebSecurityConfiguration.class);
    
    public static final String ROLE_ADMIN = "ROLE_ADMIN";
    public static final String ROLE_USER = "ROLE_USER";
    
    
    @Override
	protected void configure(HttpSecurity http) throws Exception {
    	http.csrf().disable().antMatcher("/**");
    	// all top level are accessible without login
    	http.authorizeRequests().antMatchers("/actuator/**", "/admin/**").hasAuthority(ROLE_ADMIN)
    							.antMatchers("/u/**").hasAuthority(ROLE_ADMIN) // user
//    							.antMatchers("/", "/*", "/login/**", "/webjars/**", "/error/**",
    							.anyRequest().permitAll();
		http.logout().logoutSuccessUrl("/").permitAll();
//		http.addFilterBefore(ssoFilter(), BasicAuthenticationFilter.class);
	}

	
	
	
}
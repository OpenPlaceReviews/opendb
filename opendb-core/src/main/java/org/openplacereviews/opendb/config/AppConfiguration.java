package org.openplacereviews.opendb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AppConfiguration {

    @Value("${spring.servlet.multipart.max-file-size}")
    public String maxUploadSize;

    @Value("${spring.servlet.multipart.max-request-size}")
    public String maxRequestSize;

    @Value("${ipfs.host}")
    public String ipfsHost;

    @Value("${ipfs.port}")
    public int ipfsPort;

    @Value("${ipfs.directory}")
    public String ipfsDirectory;

    @Value("${ipfs.timeout}")
    public int ipfsTimeout;

}
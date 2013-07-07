package io.cloudsoft.bootstrap;

import io.cloudsoft.cloudera.SampleClouderaManagedCluster;

import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import brooklyn.catalog.Catalog;
import brooklyn.catalog.CatalogConfig;
import brooklyn.config.ConfigKey;
import brooklyn.config.StringConfigMap;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.brooklynnode.BrooklynNode;
import brooklyn.entity.proxying.EntitySpecs;
import brooklyn.event.feed.http.HttpPollValue;
import brooklyn.util.exceptions.Exceptions;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Catalog(name = "Cloudera CDH4 Bootstrap", description = "Bootstrapper for launching Cloudera CDH in a cloud", iconUrl = "classpath://io/cloudsoft/cloudera/cloudera.jpg")
public class ClouderaBootstrapLauncher extends AbstractApplication {

    // FIXME Don't want this in final commit!
    private static final String REPOS_DIR = "/Users/aled/repos";
    private static final String M2_REPOS_DIR = "/Users/aled/.m2/repository";

    /*
     * Config for installing brooklyn...
     */
    
    @CatalogConfig(label="brooklyn distro")
    public static final ConfigKey<String> BROOKLYN_UPLOAD_URL = ConfigKeys.newStringConfigKey(
            "brooklyn.uploadUrl", "The file for brooklyn tar.gz to upload", 
            REPOS_DIR + "/brooklyncentral/brooklyn/usage/dist/target/brooklyn-0.6.0-SNAPSHOT-dist.tar.gz");
    
    @CatalogConfig(label="brooklyn download")
    public static final ConfigKey<String> BROOKLYN_DOWNLOAD_URL = ConfigKeys.newStringConfigKey(
            "brooklyn.downloadUrl", 
            "URL for downloading brooklyn (if upload url not supplied)", null);
    
    @CatalogConfig(label="classpath uploads")
    public static final ConfigKey<String> CLASSPATH_UPLOADS = ConfigKeys.newStringConfigKey(
            "brooklyn.classpathUploads", 
            "Colon-separated list of files to be uploaded onto the brooklyn classpath", 
            M2_REPOS_DIR + "/io/cloudsoft/amp/locations/vcloud-director/0.6.0-SNAPSHOT/vcloud-director-0.6.0-SNAPSHOT.jar" + ":" +
                    M2_REPOS_DIR + "/io/cloudsoft/amp/locations/ibm-smartcloud/0.6.0-SNAPSHOT/ibm-smartcloud-0.6.0-SNAPSHOT.jar" + ":" +
                    REPOS_DIR + "/cloudsoft/brooklyn-cdh/target/brooklyn-cdh-1.1.0-SNAPSHOT.jar" + ":" +
                    M2_REPOS_DIR + "/com/vmware/vcloud/vcloud-java-sdk/5.1.0/vcloud-java-sdk-5.1.0.jar" + ":" +
                    M2_REPOS_DIR + "/com/vmware/vcloud/rest-api-schemas/5.1.0/rest-api-schemas-5.1.0.jar");
    
    @CatalogConfig(label="brooklyn username")
    public static final ConfigKey<String> BROOKLYN_USERNAME = ConfigKeys.newStringConfigKey("brooklyn.username", "Username for brooklyn management console", "admin");

    @CatalogConfig(label="brooklyn password")
    public static final ConfigKey<String> BROOKLYN_PASSWORD = ConfigKeys.newStringConfigKey("brooklyn.password", "Password for brooklyn management console", "password");

    
    /*
     * Config for configuring brooklyn.properties (e.g. location credentials etc)...
     */
    
    @CatalogConfig(label="vcloud endpoint URL")
    public static final ConfigKey<String> VCLOUD_ENDPOINT = ConfigKeys.newStringConfigKey("cdh.vcloudEndpoint", "VCloud Endpoint", "https://vcloud.octocloud.org");

    @CatalogConfig(label="vcloud identity")
    public static final ConfigKey<String> VCLOUD_IDENTITY = ConfigKeys.newStringConfigKey("cdh.vcloudIdentity", "VCloud login identity", "cloudsoftcorp@paas");

    @CatalogConfig(label="vcloud credential")
    public static final ConfigKey<String> VCLOUD_CREDENTIAL = ConfigKeys.newStringConfigKey("cdh.vcloudCredential", "VCloud login credential", null);

    
    private StringConfigMap config;

    private BrooklynNode brooklynNode;
    
    @Override
    public void init() {
        super.init();
        
        config = getManagementContext().getConfig();

        String brooklynUploadUrl = getConfigOrProperty(BROOKLYN_UPLOAD_URL);
        String brooklynDownloadUrl = getConfigOrProperty(BROOKLYN_DOWNLOAD_URL);
        String brooklynUsername = getConfigOrProperty(BROOKLYN_USERNAME);
        String brooklynPassword = getConfigOrProperty(BROOKLYN_PASSWORD);
        String vcloudEndpoint = getConfigOrProperty(VCLOUD_ENDPOINT);
        String vcloudIdentity = getConfigOrProperty(VCLOUD_IDENTITY);
        String vcloudCredential = getConfigOrProperty(VCLOUD_CREDENTIAL);
        String classpathUploads = getConfigOrProperty(CLASSPATH_UPLOADS);
        List<String> classpathUploadsList = ImmutableList.copyOf(Splitter.on(":").split(classpathUploads));
        
        String catalogContents =
                "<catalog>" + "\n" +
                    "<name>Cloudsoft Brooklyn CDH</name>" + "\n" +

                    "<template type=\""+SampleClouderaManagedCluster.class.getName()+"\" name=\"Cloudera CDH4\">" + "\n" +
                        "<description>Launches Cloudera Distribution for Hadoop Manager with a Cloudera Manager and an initial cluster of 4 CDH nodes" + "\n" +
                        "(resizable) and default services including HDFS, MapReduce, and HBase</description>" + "\n" +
                        "<iconUrl>http://blog.scalar.ca/Portals/74388/images/cloudera-desktop-a-new-hadoop-management-tool-2.jpg</iconUrl>" + "\n" +
                    "</template>" + "\n" +
                "</catalog>" + "\n";

        // FIXME How to supply yum.mirror.url when don't know what IP will be yet?
        // Should we use attributeWhenReady?
        String propertiesContents = 
                "brooklyn.location.named.cloudera=vcloud-director" + "\n" +
                "brooklyn.location.named.cloudera.identity=" + vcloudIdentity + "\n" +
                "brooklyn.location.named.cloudera.credential=" + vcloudCredential + "\n" +
                "brooklyn.location.named.cloudera.user=root" + "\n" +
                "brooklyn.location.named.cloudera.endpoint=" + vcloudEndpoint + "\n" +
                "brooklyn.location.named.cloudera.ssh.reachable.timeout=1200000" + "\n" +
                "#brooklyn.location.named.cloudera.cloudera.manager.yum.mirror.url=172.16.1.102" + "\n";

        brooklynNode = addChild(EntitySpecs.spec(BrooklynNode.class)
                .configure(BrooklynNode.DISTRO_UPLOAD_URL, brooklynUploadUrl)
                .configure(BrooklynNode.DOWNLOAD_URL, brooklynDownloadUrl)
                .configure(BrooklynNode.MANAGEMENT_USER, brooklynUsername)
                .configure(BrooklynNode.MANAGEMENT_PASSWORD, brooklynPassword)
                .configure(BrooklynNode.BROOKLYN_CATALOG_CONTENTS, catalogContents)
                .configure(BrooklynNode.BROOKLYN_PROPERTIES_CONTENTS, propertiesContents)
                .configure(BrooklynNode.CLASSPATH, classpathUploadsList));
    }
    
    // FIXME how to get location id?
    // FIXME Doesn't work yet: need to get headers / uri etc right
    @Effector(description="Launches CDH via the child brooklyn")
    public void launchCdh(
            @EffectorParam(name="setupDns") boolean setupDns, 
            @EffectorParam(name="clusterSize") int clusterSize, 
            @EffectorParam(name="cpuCount") int cpuCount, 
            @EffectorParam(name="memorySize") long memorySize, 
            @EffectorParam(name="locationId") String locationId) {
        
        URI brooklynUri = brooklynNode.getAttribute(BrooklynNode.WEB_CONSOLE_URI);
        String brooklynUsername = brooklynNode.getConfig(BrooklynNode.MANAGEMENT_USER);
        String brooklynPassword = brooklynNode.getConfig(BrooklynNode.MANAGEMENT_PASSWORD);
        
        String json = 
                "{" + "\n" +
                    "\"type\": \""+SampleClouderaManagedCluster.class.getName()+"\"," + "\n" +
                    "\"locations\": [" + "\n" +
                        "\"/v1/locations/"+locationId+"\"" + "\n" +
                    "]," + "\n" +
                    "\"config\": {" + "\n" +
                        "\"setupDns\": \""+setupDns+"\"," + "\n" +
                        "\"memorySize\": \""+memorySize+"\"," + "\n" +
                        "\"cdh.clusterSize\": \""+clusterSize+"\"," + "\n" +
                        "\"cpuCount\": \""+cpuCount+"\"" + "\n" +
                    "}" + "\n" +
                "}" + "\n";

        Map<String,String> headers = ImmutableMap.of("Content-Type", "text/json");
        
        HttpClient httpClient = createHttpClient(brooklynUri, Optional.of(new UsernamePasswordCredentials(brooklynUsername, brooklynPassword)));
        httpPost(httpClient, URI.create(brooklynUri.toString() + "/v1/applications"), headers, json.getBytes());
    }
    
    private <T> T getConfigOrProperty(ConfigKey<T> key) {
        Object val = getConfigMap().getRawConfig(key);
        if (val == null) val = config.getFirst(key.getName());
        if (val == null) val = getConfig(key);
        return (T) val;
    }
    
    // TODO Code copied from HttpFeed; move to a common utility?
    private HttpClient createHttpClient(URI uri, Optional<? extends Credentials> credentials) {
        final DefaultHttpClient httpClient = new DefaultHttpClient();

        // TODO if supplier returns null, we may wish to defer initialization until url available?
        if (uri != null && "https".equalsIgnoreCase(uri.getScheme())) {
            try {
                int port = (uri.getPort() >= 0) ? uri.getPort() : 443;
                SSLSocketFactory socketFactory = new SSLSocketFactory(
                        new TrustAllStrategy(), SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
                Scheme sch = new Scheme("https", port, socketFactory);
                httpClient.getConnectionManager().getSchemeRegistry().register(sch);
            } catch (Exception e) {
                log.warn("Error in {}, setting trust for uri {}", this, uri);
                throw Exceptions.propagate(e);
            }
        }

        // Set credentials
        if (uri != null && credentials.isPresent()) {
            String hostname = uri.getHost();
            int port = uri.getPort();
            httpClient.getCredentialsProvider().setCredentials(new AuthScope(hostname, port), credentials.get());
        }

        return httpClient;
    }
    
    // TODO Code copied from HttpFeed; move to a common utility?
    private HttpPollValue httpPost(HttpClient httpClient, URI uri, Map<String,String> headers, byte[] body) {
        try {
            HttpPost httpPost = new HttpPost(uri);
            for (Map.Entry<String,String> entry : headers.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
            if (body != null) {
                HttpEntity httpEntity = new ByteArrayEntity(body);
                httpPost.setEntity(httpEntity);
            }
            
            long startTime = System.currentTimeMillis();
            HttpResponse httpResponse = httpClient.execute(httpPost);
            
            try {
                return new HttpPollValue(httpResponse, startTime);
            } finally {
                EntityUtils.consume(httpResponse.getEntity());
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    // TODO Code copied from HttpFeed; move to a common utility?
    private static class TrustAllStrategy implements TrustStrategy {
        @Override
        public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            return true;
        }
    }
}

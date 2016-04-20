/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.trustedanalytics.servicebroker.hive.plans.binding;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.trustedanalytics.servicebroker.framework.Credentials;
import org.trustedanalytics.servicebroker.hive.DbNameNormalizer;
import org.trustedanalytics.servicebroker.hive.config.ExternalConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class HiveBindingClient
    implements HiveSharedBindingOperations, HiveSpecificOrgBindingOperations {

  private static final String HIVE_SERVER_PRINCIPAL_PROPERTY_NAME =
      "hive.server2.authentication.kerberos.principal";

  private static final String AUTHENTICATION_METHOD = "kerberos";

  private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

  private static final String NO_NEEDED_KRB_PROPERTIES = "";

  private Configuration hiveConfig;

  private Credentials credentials;

  private String hiveServerHost;

  private int hiveServerPort;

  @Autowired
  public HiveBindingClient(ExternalConfiguration configuration) throws IOException {
    ImmutableMap.Builder<String, Object> credentialsBuilder =
        new ImmutableMap.Builder<String, Object>().putAll(configuration.hiveConfigAsMap());
    this.credentials = new Credentials(credentialsBuilder.build());
    this.hiveServerHost = configuration.getHiveServerHost();
    this.hiveServerPort = Integer.decode(configuration.getHiveServerPort());
    this.hiveConfig = configuration.hiveConfigAsHadoopConfig();
  }

  @Override
  public Map<String, Object> createCredentialsMap() {
    Map<String, Object> credentialsCopy = new HashMap<>(credentials.getCredentialsMap());
    String connectionUrl = String.format("jdbc:hive2://%s:%d/%%{organization}%s",
                                         this.hiveServerHost,
                                         this.hiveServerPort,
                                         kerberosSpecific());
    credentialsCopy.put("connectionUrl", connectionUrl);
    return credentialsCopy;
  }

  @Override
  public Map<String, Object> createCredentialsMap(UUID serviceInstanceId) {
    Map<String, Object> credentialsCopy = new HashMap<>(credentials.getCredentialsMap());
    String dbName = DbNameNormalizer.create().normalize(serviceInstanceId.toString());
    String connectionUrl = String.format("jdbc:hive2://%s:%d/%s%s",
                                         this.hiveServerHost,
                                         this.hiveServerPort,
                                         dbName,
                                         kerberosSpecific());
    credentialsCopy.put("connectionUrl", connectionUrl);
    return credentialsCopy;
  }

  public String getConnectionUrl() {
    return String.format("jdbc:hive2://%s:%d/%s",
                         this.hiveServerHost,
                         this.hiveServerPort,
                         kerberosSpecific());
  }

  public boolean isKerberosEnabled() {
    return AUTHENTICATION_METHOD.equals(hiveConfig.get(AUTHENTICATION_METHOD_PROPERTY));
  }

  String kerberosSpecific() {
    if (!isKerberosEnabled()) {
      return NO_NEEDED_KRB_PROPERTIES;
    }
    String hivePrincipal = hiveConfig.get(HIVE_SERVER_PRINCIPAL_PROPERTY_NAME);
    Preconditions.checkNotNull(hivePrincipal,
                               String.format("Can't find %s property in hive client config, "
                                             + "provided in HADOOP_PROVIDED_ZIP!",
                                             HIVE_SERVER_PRINCIPAL_PROPERTY_NAME));
    return String.format(";principal=%s;auth=kerberos",
                         hivePrincipal.replaceAll("_HOST", this.hiveServerHost));
  }
}
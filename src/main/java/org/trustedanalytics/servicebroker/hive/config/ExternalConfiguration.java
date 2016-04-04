/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.servicebroker.hive.config;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.trustedanalytics.cfbroker.config.HadoopZipConfiguration;
import org.trustedanalytics.servicebroker.hive.PrincipalNameNormalizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

import javax.validation.constraints.NotNull;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Configuration
@NoArgsConstructor
@Getter
@Setter
public class ExternalConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalConfiguration.class);

  @Value("${store.user}")
  @NotNull
  private String user;

  @Value("${store.password}")
  @NotNull
  private String password;

  @Value("${hive.provided.zip}")
  @NotNull
  private String hiveProvidedZip;

  @Value("${hive.server.host}")
  @NotNull
  private String hiveServerHost;

  @Value("${hive.server.port}")
  @NotNull
  private String hiveServerPort;

  @Value("${hive.superuser.name}")
  private String hiveSuperUser;

  @Value("${hive.superuser.keytab}")
  private String hiveSuperUserKeyTab;

  @Setter
  private String keyTabsDir = "/tmp";

  public String getKeyTabLocation() throws IOException {

    String keyTabFilePath = String.format("%s/%s.keytab",
                                          keyTabsDir,
                                          PrincipalNameNormalizer.create().normalize(getHiveSuperUser()));
    LOGGER.info("Trying to write keytab file" + keyTabFilePath);
    Path path = Paths.get(keyTabFilePath);
    if (Files.notExists(path)) {
      Files.write(path,
                  Base64.getDecoder().decode(getHiveSuperUserKeyTab()),
                  StandardOpenOption.CREATE_NEW);
    } else if (Files.isDirectory(path)) {
      throw new IOException(
          String.format("Under path %s exists directory. It's path where hive superuser keytab "
                        + "is stored. Please move or delete this directory", keyTabFilePath));
    }
    return keyTabFilePath;
  }

  public org.apache.hadoop.conf.Configuration hiveConfigAsHadoopConfig() throws IOException {
    return  HadoopZipConfiguration.createHadoopZipConfiguration(getHiveProvidedZip())
        .getAsHadoopConfiguration();
  }

  public ImmutableMap hiveConfigAsMap() throws IOException {
    return  HadoopZipConfiguration.createHadoopZipConfiguration(getHiveProvidedZip())
        .getBrokerCredentials();
  }
}
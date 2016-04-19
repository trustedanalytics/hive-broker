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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;
import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.ServiceType;
import org.trustedanalytics.servicebroker.hive.MockedJdbcDriver;
import org.trustedanalytics.servicebroker.hive.plans.binding.HiveBindingClient;

import java.io.IOException;
import java.sql.Driver;
import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {ServiceInstanceProvisioningConfig.class,
                                           ServiceInstanceProvisioningConfigTest.UnitTestsConfig.class})
public class ServiceInstanceProvisioningConfigTest {

  @Autowired
  private HiveBindingClient hiveBindingClient;

  @Autowired
  private JdbcOperations toTest;

  @Test
  public void CreatingJdbcOperationsTest() {
    Assert.isInstanceOf(JdbcTemplate.class, toTest);
  }

  public static class UnitTestsConfig {

    @Bean
    public HiveBindingClient hiveBindingClient() {
      HiveBindingClient mocked = Mockito.mock(HiveBindingClient.class);
      when(mocked.getConnectionUrl()).thenReturn("jdbc:hive2//localhost:10000/default");
      when(mocked.isKerberosEnabled()).thenReturn(true);
      return mocked;
    }

    @Bean
    public ExternalConfiguration externalConfiguration() throws IOException {
      ExternalConfiguration mocked = Mockito.mock(ExternalConfiguration.class);
      when(mocked.getHiveSuperUser()).thenReturn("jojo");
      when(mocked.getKeyTabLocation()).thenReturn("/tmp/jojo.keytab");
      when(mocked.hiveConfigAsHadoopConfig()).thenReturn(new Configuration(false));
      return mocked;
    }

    @Bean
    public AppConfiguration appConfiguration() {
      AppConfiguration mocked = Mockito.mock(AppConfiguration.class);
      ServiceInstanceConfiguration mockedKrbConfiguration =
          Mockito.mock(ServiceInstanceConfiguration.class);
      when(mockedKrbConfiguration.getProperty(Property.KRB_KDC))
          .thenReturn(Optional.of("localhost"));
      when(mockedKrbConfiguration.getProperty(Property.KRB_REALM))
          .thenReturn(Optional.of("CLOUDERA"));
      when(mocked.getServiceConfig(ServiceType.KERBEROS_TYPE))
          .thenReturn(mockedKrbConfiguration);
      return mocked;
    }

    @Bean
    public Driver driver() {
      return new MockedJdbcDriver();
    }

  }

}
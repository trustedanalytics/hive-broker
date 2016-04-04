/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.servicebroker.hive.plans;

import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceExistsException;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcOperations;
import org.trustedanalytics.servicebroker.hive.plans.binding.HiveBindingClient;

import java.util.UUID;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HiveSharedTest {

  @Mock
  JdbcOperations jdbcOperations;

  @Mock
  HiveBindingClient hiveBindingClient;

  @Test
  public void bindTest_createsCredentialsMap() throws ServiceBrokerException {
    //given
    HiveShared toTest = new HiveShared(hiveBindingClient, jdbcOperations);

    //when
    toTest.bind(getServiceInstance("04d4e5d2-0568-11e6-8d01-00155d3d2c21"));

    //then
    verify(hiveBindingClient).createCredentialsMap(
        UUID.fromString("04d4e5d2-0568-11e6-8d01-00155d3d2c21"));
  }

  @Test
  public void provisionTest_kerberosEnabled_createsDBAddGrants()
      throws ServiceBrokerException, ServiceInstanceExistsException {
    //given
    when(hiveBindingClient.isKerberosEnabled()).thenReturn(true);
    HiveShared toTest = new HiveShared(hiveBindingClient, jdbcOperations);

    //when
    toTest.provision(getServiceInstance("04d4e5d2-0568-11e6-8d01-00155d3d2c21"));

    //then
    InOrder inOrder = inOrder(jdbcOperations);
    inOrder.verify(jdbcOperations)
        .execute("create database if not exists `04d4e5d2_0568_11e6_8d01_00155d3d2c21`");
    inOrder.verify(jdbcOperations)
        .execute("GRANT ALL ON DATABASE `04d4e5d2_0568_11e6_8d01_00155d3d2c21` "
                 + "TO ROLE `f0487d90-fde6-4da1-a933-03f38776115d`");
  }

  @Test
  public void provisionTest_kerberosDisabled_createsDB()
      throws ServiceBrokerException, ServiceInstanceExistsException {
    //given
    when(hiveBindingClient.isKerberosEnabled()).thenReturn(false);
    HiveShared toTest = new HiveShared(hiveBindingClient, jdbcOperations);

    //when
    toTest.provision(getServiceInstance("04d4e5d2-0568-11e6-8d01-00155d3d2c21"));

    //then
    verify(jdbcOperations, only())
        .execute("create database if not exists `04d4e5d2_0568_11e6_8d01_00155d3d2c21`");
  }

  private ServiceInstance getServiceInstance(String id) {
    return new ServiceInstance(
        new CreateServiceInstanceRequest(id,
                                         "FAKE-BASE_GUID-shared-plan",
                                         "f0487d90-fde6-4da1-a933-03f38776115d",
                                         "spaceGuid")
            .withServiceInstanceId(id)
    );
  }

}

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

package org.trustedanalytics.servicebroker.hive.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.DeleteServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.cloudfoundry.community.servicebroker.model.ServiceInstanceBinding;
import org.cloudfoundry.community.servicebroker.service.ServiceInstanceBindingService;
import org.cloudfoundry.community.servicebroker.service.ServiceInstanceService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.servicebroker.hive.config.Application;
import org.trustedanalytics.servicebroker.hive.config.ExternalConfiguration;
import org.trustedanalytics.servicebroker.hive.integration.config.kerberos.IntegrationTestsConfiguration;
import org.trustedanalytics.servicebroker.hive.integration.config.store.ZkLocalConfiguration;

import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {Application.class,
                                           IntegrationTestsConfiguration.class,
                                           ZkLocalConfiguration.class})
@IntegrationTest("server.port=0")
@ActiveProfiles("integration-test")
public class HiveBrokerIntegrationTest {

  @Autowired
  private ExternalConfiguration conf;

  @Autowired
  private ServiceInstanceService instanceService;

  @Autowired
  private ServiceInstanceBindingService instanceBindingService;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreateServiceInstance_success_shouldReturnCreatedInstance() throws Exception {
    CreateServiceInstanceRequest request = getCreateInstanceRequest("instanceId",
                                                                    "multitenant-plan");
    instanceService.createServiceInstance(request);
    ServiceInstance instance = instanceService.getServiceInstance(request.getServiceInstanceId());
    assertThat(instance.getServiceInstanceId(), equalTo("instanceId"));
  }

  @Test
  public void testDeleteServiceInstance_success_shouldReturnRemovedInstance() throws Exception {
    ServiceInstance instance =
        instanceService.createServiceInstance(getCreateInstanceRequest("instanceId3",
                                                                       "multitenant-plan"));
    ServiceInstance removedInstance =
        instanceService.deleteServiceInstance(
            new DeleteServiceInstanceRequest(instance.getServiceInstanceId(),
                                             instance.getServiceDefinitionId(),
                                             instance.getPlanId())
        );
    assertThat(instance.getServiceInstanceId(), equalTo(removedInstance.getServiceInstanceId()));
  }

  @Test
  public void testCreateInstanceBinding_success_shouldReturnBinding() throws Exception {
    CreateServiceInstanceRequest request = getCreateInstanceRequest("instanceId4",
                                                                    "multitenant-plan");
    instanceService.createServiceInstance(request);
    CreateServiceInstanceBindingRequest bindingRequest =
        new CreateServiceInstanceBindingRequest(
            getServiceInstance("instanceId4").getServiceDefinitionId(),
            "FAKE-BASE_GUID-multitenant-plan",
            "appGuid")
            .withBindingId("bindingId").withServiceInstanceId("instanceId4");
    ServiceInstanceBinding binding =
        instanceBindingService.createServiceInstanceBinding(bindingRequest);
    assertThat(binding.getServiceInstanceId(), equalTo("instanceId4"));
  }

  @Test
  public void testCreateInstanceBindingPlanShared_success_shouldReturnBinding() throws Exception {
    String instanceId = UUID.randomUUID().toString();
    CreateServiceInstanceRequest request = getCreateInstanceRequest(instanceId,
                                                                    "shared-plan");
    instanceService.createServiceInstance(request);
    CreateServiceInstanceBindingRequest bindingRequest =
        new CreateServiceInstanceBindingRequest(getServiceInstance(instanceId)
                                                    .getServiceDefinitionId(),
                                                "FAKE-BASE_GUID-shared-plan",
                                                "appGuid").withBindingId("bindingId")
            .withServiceInstanceId(instanceId);
    ServiceInstanceBinding binding = instanceBindingService.createServiceInstanceBinding(
        bindingRequest);
    assertThat(binding.getServiceInstanceId(), equalTo(instanceId));
  }

  private ServiceInstance getServiceInstance(String id) {
    return new ServiceInstance(
        new CreateServiceInstanceRequest(id,
                                         "FAKE-BASE_GUID-multitenant-plan",
                                         "f0487d90-fde6-4da1-a933-03f38776115d",
                                         "spaceGuid")
    );
  }

  private CreateServiceInstanceRequest getCreateInstanceRequest(String serviceId,
                                                                final String planName) {
    return new CreateServiceInstanceRequest("serviceDefinitionId",
                                            "FAKE-BASE_GUID-" + planName,
                                            "f0487d90-fde6-4da1-a933-03f38776115d",
                                            "spaceGuid").withServiceInstanceId(serviceId);
  }
}

/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.trustedanalytics.servicebroker.hive.plans;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.trustedanalytics.servicebroker.test.cloudfoundry.CfModelsFactory.getServiceInstance;

import java.util.Map;

import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.junit.Test;
import org.trustedanalytics.cfbroker.config.HadoopZipConfiguration;
import org.trustedanalytics.servicebroker.framework.Credentials;
import org.trustedanalytics.servicebroker.hive.config.ExternalConfiguration;
import org.trustedanalytics.servicebroker.hive.plans.binding.HiveBindingClientFactory;

import com.google.common.collect.ImmutableMap;

public class HivePlanMultitenantTest {

  String hadoopConf = "UEsDBAoAAAAAAM1LhkgAAAAAAAAAAAAAAAAKABwAaGl2ZS1jb25mL1VUCQADEbsEV6jBBFd1eAsA"
                      + "AQTNSN5eBAEC2F5QSwMEFAAAAAgAxTKESJ14/pIiAgAAbAQAABUAHABoaXZlLWNvbmYvaGl2ZS"
                      + "1lbnYuc2hVVAkAA/LrAVdJQQJXdXgLAAEEzUjeXgQBAthehVPbbtpAEH3nK6aOFTCy2UJpG1m4"
                      + "khOIICIBcYmoELWMPYBT37o2lBTcb++uMU2JqfKyu3N2Lmf2zF5Au/PYMvTxxLjTB0Ojr4/a2m"
                      + "6XB5OkcAF3+qNudDvXA33w9eiaw5gnbsOAxtDWm71e37jpPdwazc5AE8TdK0hVBLEElg18sx3q"
                      + "mx6y8+5aH7aNYW88uGlN388SASQBLi8h/GmzUyIU2syhZaQsm61bfdwdcaaaWFo4vg1kHVHiOn"
                      + "OycjbID6CkibmprOZmxFbTt12kSrnyZFKofQFi44b4a9eFPcSm47IQqEqyWIqQsYtUUGV1KUCj"
                      + "0YBcFZ4RFIpL3MbPIUIYRM5WwSXFMINBOPUmpXTbH8hYroN+nBkR0g3SzAhpEAdW4B4dA88L/P"
                      + "2xBTsIQo6FZnyC1Y5gKS0YU9PisRQrZUniDQsnHUtS4cwQiKXplD+CmL+D2YyrgdYqOHctS0xM"
                      + "Hv0OitkoHfRiHklSBO03nIJZvkPCfIQE+31WbHdeeFVJuFQvskQrkyJ5Mjcm8Z6jHy7r3vfRig"
                      + "OqcDAn+rloy1ukgjE1YyYlz1J+sufW2yNzlkfAVHDxTSJ//845Rf734owAH9NiRGS5TGSyLL4g"
                      + "32RC/jFlkZnS6x/a7bQeRkavPxpqgjLxtrVq/XP96sOn+hUok4l6b277SL2h8wu1j9XaPSjNlL"
                      + "2PcSWkuEDa6W/qw9i0vmsxXSPjmEssFP4AUEsDBBQAAAAIAMxLhkhqezGEkAAAABIBAAAXABwA"
                      + "aGl2ZS1jb25mL2NvcmUtc2l0ZS54bWxVVAkAAxC7BFcQuwRXdXgLAAEEzUjeXgQBAthejY9NDs"
                      + "IgEEb3nIKwF3TngtKdJ9ADIB1bYsuQARrr6e3Pxpg0cfmSN9/L6Po19HwESh5DJU7yKDgEh40P"
                      + "bSVu18vhLGrDmHYYHr4tZPMsGsa5joQRKE8LzBjsAKazDWKUCVwhnydpS+4gZO/WM61WafNH2x"
                      + "cwT6A7ECatNl6G1ffy3xkk/96rZCqwU9Dq57MPUEsDBBQAAAAIAGNLhkhUdGVzmgAAAMoAAAAX"
                      + "ABwAaGl2ZS1jb25mL2hpdmUtc2l0ZS54bWxVVAkAA0q6BFdKugRXdXgLAAEEzUjeXgQBAtheXY"
                      + "67DsIwFEN3viLKTgJMDGkL4iEGpErQziiESxvR3kS3aQV/Tx8DEqNlH9sqedcV64Aa6zDiS7Hg"
                      + "DNC4h8Ui4nl2nK95Es+Ucfi0RUs69Ll4xpjy5DxQ+Ayil6hriEvbgWiA+r6V0G0oAYM1IyNeQH"
                      + "cg1whPFo31ulJyhCa+01U7FcjbKb1mm905zfeHy1bJyRo25W9Uyb9LX1BLAQIeAwoAAAAAAM1L"
                      + "hkgAAAAAAAAAAAAAAAAKABgAAAAAAAAAEADtQQAAAABoaXZlLWNvbmYvVVQFAAMRuwRXdXgLAA"
                      + "EEzUjeXgQBAtheUEsBAh4DFAAAAAgAxTKESJ14/pIiAgAAbAQAABUAGAAAAAAAAQAAAKSBRAAA"
                      + "AGhpdmUtY29uZi9oaXZlLWVudi5zaFVUBQAD8usBV3V4CwABBM1I3l4EAQLYXlBLAQIeAxQAAA"
                      + "AIAMxLhkhqezGEkAAAABIBAAAXABgAAAAAAAEAAACkgbUCAABoaXZlLWNvbmYvY29yZS1zaXRl"
                      + "LnhtbFVUBQADELsEV3V4CwABBM1I3l4EAQLYXlBLAQIeAxQAAAAIAGNLhkhUdGVzmgAAAMoAAA"
                      + "AXABgAAAAAAAEAAACkgZYDAABoaXZlLWNvbmYvaGl2ZS1zaXRlLnhtbFVUBQADSroEV3V4CwAB"
                      + "BM1I3l4EAQLYXlBLBQYAAAAABAAEAGUBAACBBAAAAAA=GER\\aignatow";

  @SuppressWarnings("unchecked")
  @Test
  public void bind_doNothing_returnMultitenantCredentialsMap() throws Exception {
    //given
    String host = "jojoservice";
    String port = "10000";
    String expected = "jdbc:hive2://jojoservice:10000/%{organization};"
                      + "principal=hive/jojoservice@CLOUDERA;auth=kerberos";
    ExternalConfiguration conf = new ExternalConfiguration();
    conf.setHiveServerHost(host);
    conf.setHiveServerPort(port);
    conf.setHiveProvidedZip(hadoopConf);
    HivePlanMultitenant plan = new HivePlanMultitenant(HiveBindingClientFactory.create(conf));

    //when
    ServiceInstance serviceInstance = getServiceInstance();
    Map<String, Object> actualOutputCredentials = plan.bind(serviceInstance);

    //then
    assertThat(actualOutputCredentials, hasEntry("connectionUrl", expected));
  }

}

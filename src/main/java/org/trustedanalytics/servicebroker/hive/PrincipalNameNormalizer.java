package org.trustedanalytics.servicebroker.hive;

import java.util.Objects;

public final class PrincipalNameNormalizer implements NameNormalizer {

  public static PrincipalNameNormalizer create() {
    return new PrincipalNameNormalizer();
  }

  private PrincipalNameNormalizer() {
  }

  @Override
  public String normalize(final String name) {
    Objects.requireNonNull(name);
    return name.replaceAll("/", "_");
  }
}

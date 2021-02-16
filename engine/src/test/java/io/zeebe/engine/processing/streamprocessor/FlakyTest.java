/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.streamprocessor;

import java.util.Random;
import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.EdgeCasesMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import org.junit.Assert;

public class FlakyTest {

  @Property(
      tries = 100,
      shrinking = ShrinkingMode.OFF,
      edgeCases = EdgeCasesMode.NONE,
      afterFailure = AfterFailureMode.PREVIOUS_SEED)
  void raftProperty(@ForAll("seeds") final long seed) throws Exception {
    System.out.println("Seed:" + seed);

    if (seed % 2 == 0) {
      if (new Random().nextBoolean()) {
        Assert.fail("Flaky test failure");
      }
    }
  }

  @Provide
  Arbitrary<Long> seeds() {
    return Arbitraries.longs();
  }
}

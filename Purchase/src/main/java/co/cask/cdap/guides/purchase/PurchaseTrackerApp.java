/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.guides.purchase;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;

/**
 * This implements a simple purchase tracking application via a custom Dataset --
 * see package-info for more details.
 */
public class PurchaseTrackerApp extends AbstractApplication {

  public static final String APP_NAME = "PurchaseTrackerApp";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Stores purchases in a Dataset, and makes it available for ad-hoc querying.");
    addStream(new Stream("purchaseStream"));
    addFlow(new PurchaseFlow());
    createDataset("purchases", PurchaseStore.class, PurchaseStore.properties());
  }
}

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
package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.guides.purchase.Purchase;
import co.cask.cdap.guides.purchase.PurchaseStore;
import co.cask.cdap.guides.purchase.PurchaseTrackerApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link PurchaseTrackerApp}.
 */
public class PurchaseTrackerAppTest extends TestBase {

  @Test
  public void test() throws Exception {
    // Deploy the PurchaseApp application
    ApplicationManager appManager = deployApplication(PurchaseTrackerApp.class);
    try {

      // Start PurchaseFlow
      appManager.startFlow("PurchaseFlow");

      // Send stream events to the "purchaseStream" Stream
      ArrayList<Purchase> purchaseEvents = new ArrayList<Purchase>();
      purchaseEvents.add(new Purchase("bob", 353, 3, System.currentTimeMillis()));
      purchaseEvents.add(new Purchase("joe", 18, 1, 0));
      purchaseEvents.add(new Purchase("joe", 20, 10, 0));
      purchaseEvents.add(new Purchase("cat", 287, 32, 0));
      purchaseEvents.add(new Purchase("cat", 14, 2, 0));

      StreamWriter streamWriter = appManager.getStreamWriter("purchaseStream");
      for (Purchase purchase: purchaseEvents) {
        String event = String.format("%s,%d,%d", purchase.getCustomer(), purchase.getQuantity(),
                                     purchase.getProductId());
        streamWriter.send(event);
      }

      // Wait for the Flowlet to finish processing the stream events, with a timeout of at most 15 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics(PurchaseTrackerApp.APP_NAME, "PurchaseFlow",
                                                              "StreamReaderFlowlet");
      metrics.waitForProcessed(purchaseEvents.size(), 15, TimeUnit.SECONDS);


      // Ensure that the purchase events sent to the stream match the purchases persisted to the Dataset.
      ArrayList<Purchase> dsPurchases = new ArrayList<Purchase>();
      DataSetManager<PurchaseStore> dsManager = appManager.getDataSet("PurchasesDataset");
      PurchaseStore purchaseStore = dsManager.get();
      List<Split> splits = purchaseStore.getSplits();
      for (Split split : splits) {
        RecordScanner<Purchase> reader = purchaseStore.createSplitRecordScanner(split);
        reader.initialize(split);
        while (reader.nextRecord()) {
          Purchase purchaseFromStore = reader.getCurrentRecord();
          dsPurchases.add(purchaseFromStore);
        }
      }

      Assert.assertEquals(purchaseEvents.size(), dsPurchases.size());
      for (int i = 0; i < purchaseEvents.size(); i++) {
        Purchase purchaseEvent = purchaseEvents.get(i);
        Purchase dsPurchase = dsPurchases.get(i);
        Assert.assertEquals(purchaseEvent.getCustomer(), dsPurchase.getCustomer());
        Assert.assertEquals(purchaseEvent.getQuantity(), dsPurchase.getQuantity());
        Assert.assertEquals(purchaseEvent.getProductId(), dsPurchase.getProductId());
      }

    } finally {
      appManager.stopAll();
    }
  }
}
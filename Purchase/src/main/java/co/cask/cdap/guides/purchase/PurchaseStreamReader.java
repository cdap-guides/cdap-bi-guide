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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Flowlet reads events from a Stream and parses them as sentences of the form
 * <pre><name> bought <n> <items> for $<price></pre>. The event is then converted into
 * a Purchase object and emitted. If the event does not have this form, it is dropped.
 */
public class PurchaseStreamReader extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(PurchaseStreamReader.class);
  private Metrics metrics;

  @UseDataSet("purchases")
  private PurchaseStore store;

  @ProcessInput
  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());
    // <name>,<quantity>,<productId>
    String[] tokens =  body.split(",");
    for (int i=0; i<tokens.length; i++) {
      tokens[i] = tokens[i].trim();
    }
    if (tokens.length != 3) {
      LOG.error("Invalid stream event:{}", body);
      return;
    }
    String customer = tokens[0];
    int quantity = Integer.parseInt(tokens[1]);
    int item = Integer.parseInt(tokens[2]);

    Purchase purchase = new Purchase(customer, item, quantity, System.currentTimeMillis());
    metrics.count("purchases." + purchase.getCustomer(), 1);

    LOG.info("Purchase info: Customer {}, ProductId {}, Quantity {}",
             purchase.getCustomer(), purchase.getProductId(), purchase.getQuantity());
    store.write(purchase);
  }
}

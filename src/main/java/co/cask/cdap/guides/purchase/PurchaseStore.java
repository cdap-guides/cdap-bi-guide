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

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.internal.io.UnsupportedTypeException;

import java.lang.reflect.Type;
import java.util.List;

/**
 * This stores purchases in an embedded object store. Embedding the object store into this Dataset
 * ensures that the type parameter (the Purchase class) is bundled with this Dataset's code when it is
 * deployed. That means a PurchaseStore can be used outside of this application, particularly for
 * querying the Dataset with SQL.
 *
 * This class implements RecordScannable in order to run ad-hoc queries against it.
 */
public class PurchaseStore extends AbstractDataset implements RecordScannable<Purchase> {

  // the embedded object store
  private final ObjectStore<Purchase> store;

  /**
   * These properties will be required to create the object store. We provide them here through a static
   * method such that application code that uses this Dataset does not need to be aware of the detailed
   * properties that are expected.
   *
   * @return the properties required to create an instance of this Dataset
   */
  public static DatasetProperties properties() {
    try {
      return ObjectStores.objectStoreProperties(Purchase.class, DatasetProperties.EMPTY);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("This should never be thrown - Purchase is a supported type", e);
    }
  }

  /**
   * Constructor from a specification and the embedded object store. By convention,
   * implementing this constructor allows to define this Dataset type without an explicit DatasetDefinition.
   *
   * @param spec the specification
   * @param objStore the embedded object store
   */
  public PurchaseStore(DatasetSpecification spec,
                       @EmbeddedDataset("store") ObjectStore<Purchase> objStore) {
    super(spec.getName(), objStore);
    this.store = objStore;
  }

  @Override // RecordScannable
  public Type getRecordType() {
    return Purchase.class;
  }

  @Override // RecordScannable
  public List<Split> getSplits() {
    return store.getSplits();
  }

  @Override // RecordScannable
  public RecordScanner<Purchase> createSplitRecordScanner(Split split) {
    return Scannables.valueRecordScanner(store.createSplitReader(split));
  }

  /**
   * Write a purchase purchase to the store. Uses the a computed key of the purchase as the key.
   *
   * @param purchase The purchase to store.
   */
  public void write(Purchase purchase) {
    store.write(purchase.getKey(), purchase);
  }

  /**
   * @param key key for the requested purchase
   * @return the purchase for the given key
   */
  public Purchase read(byte[] key) {
    return store.read(key);
  }
}

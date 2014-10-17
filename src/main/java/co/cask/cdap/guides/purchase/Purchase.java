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

/**
 * This class represents a purchase made by a customer. It is a very simple class and only contains
 * the name of the customer, the id of the product, product quantity, and the purchase time.
 */
public class Purchase {
  private final String customer;
  private final String product;
  private final int quantity;
  private final long purchaseTime;

  public Purchase(String customer, String product, int quantity, long purchaseTime) {
    this.customer = customer;
    this.product = product;
    this.quantity = quantity;
    this.purchaseTime = purchaseTime;
  }

  public String getCustomer() {
    return customer;
  }

  public long getPurchaseTime() {
    return purchaseTime;
  }

  public int getQuantity() {
    return quantity;
  }

  public String getProduct() {
    return product;
  }

  public byte[] getKey() {
    String hashedKey = purchaseTime + customer + product;
    return hashedKey.getBytes();
  }
}

package org.aws.dynamodb;

/**
 * Duplicates statistic POJO.
 */
public class Statistic {
  private int duplicatedItems = 0;
  private int total = 0;

  public void incrementDuplicatedItems() {
    duplicatedItems++;
  }

  public void incrementTotal() {
    total++;
  }

  public int getDuplicatedItems() {
    return duplicatedItems;
  }

  public int getTotal() {
    return total;
  }
}

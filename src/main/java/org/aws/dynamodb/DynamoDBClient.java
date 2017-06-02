package org.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import java.util.Iterator;

/**
 * Dynamo DB client wrapper.
 *
 * Uses {@link com.amazonaws.auth.DefaultAWSCredentialsProviderChain} credential provider.
 */
public class DynamoDBClient {
  private Table table;

  private DynamoDB dynamoDB;

  /**
   * Create {@link DynamoDBClient} in defined region.
   *
   * @param region AWS region name.
   */
  public DynamoDBClient(String region) {
    dynamoDB = buildDynamoDB(region);
  }

  /**
   * Connect to defined table for further operations.
   *
   * @param tableName Dynamo DB Table name.
   */
  public void connectToTable(String tableName) {
    table = dynamoDB.getTable(tableName);
  }

  /**
   * Get all values by Global Secondary Index.
   *
   * @param secondaryIndexColumnName Global secondary index column name.
   * @return {@link Iterator} of all values by secondary index.
   */
  public Iterator<Item> getSecondaryIndexValues(String secondaryIndexColumnName) {

    ScanSpec scanSpec = new ScanSpec().withAttributesToGet(secondaryIndexColumnName);

    return table.scan(scanSpec).iterator();
  }

  /**
   * Get all rows that has same secondary index value.
   *
   * @param secondaryIndexColumnName  Secondary index column name.
   * @param secondaryIndexColumnValue Secondary index column value.
   * @return {@link Iterator} rows searched by secondary index name and value.
   */
  public Iterator<Item> getRowsByIndexValue(String secondaryIndexColumnName,
                                            String secondaryIndexColumnValue) {

    Index gsi = table.getIndex(secondaryIndexColumnName);

    return gsi.query(new KeyAttribute(secondaryIndexColumnName, secondaryIndexColumnValue))
        .iterator();
  }

  protected DynamoDB buildDynamoDB(String region) {
    return new DynamoDB(AmazonDynamoDBClientBuilder.standard().withRegion(region).build());
  }
}

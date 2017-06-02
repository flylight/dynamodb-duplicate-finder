package org.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Amazon Web Services Dynamo DB duplicate finder.
 *
 * Using table name and secondary index column name find all duplicates.
 *
 * Notice : This approach works for secondary index columns with type : STRING, other types are not
 * supported but you may change this code and use any type that supported by Dynamo DB.
 */
public class DuplicateFinder {
  private static final Logger log = Logger.getLogger(DuplicateFinder.class.getCanonicalName());

  private final DynamoDBClient dynamoDBClient;

  private Statistic statistic = new Statistic();

  /**
   * Create Duplicate finder constructor.
   *
   * @param region    AWS Region name.
   * @param tableName AWS Dynamo DB table name.
   */
  public DuplicateFinder(String region, String tableName) {
    dynamoDBClient = buildDynamoDBClient(region);
    dynamoDBClient.connectToTable(tableName);
  }

  /**
   * Search all duplicated items inside Dynamo DB Table according to secondary index column.
   *
   * @param secondaryIndexColumnName Secondary index column name.
   * @return Statistic
   */
  public Statistic startSearchOfDuplicates(String secondaryIndexColumnName) {
    log.info("Started search of duplicates");

    getAllIndexColumnValues(secondaryIndexColumnName)
        .forEach(siValue -> logDuplicatesIfPresent(dynamoDBClient
            .getRowsByIndexValue(secondaryIndexColumnName, siValue)));

    log.info("Finished");
    log.info("Summary: Duplicated items - " + statistic.getDuplicatedItems()
        + ", Total - " + statistic.getTotal());
    return statistic;
  }

  protected DynamoDBClient buildDynamoDBClient(String region) {
    return new DynamoDBClient(region);
  }

  /**
   * Get all unique secondary index values.
   *
   * @param secondaryIndexColumnName Secondary index column name.
   * @return Set of unique secondary index values.
   */
  private Set<String> getAllIndexColumnValues(String secondaryIndexColumnName) {
    Set<String> results = new HashSet<>();

    Iterator<Item> items = dynamoDBClient.getSecondaryIndexValues(secondaryIndexColumnName);

    while (items.hasNext()) {
      //NOTICE : Here we expect to get STRING value type of index column
      results.add(items.next().getString(secondaryIndexColumnName));
    }

    return results;
  }

  /**
   * Log duplicated items.
   *
   * @param items {@link Iterator} of fetched items.
   */
  private void logDuplicatesIfPresent(Iterator<Item> items) {
    Item firstItem = items.hasNext() ? items.next() : null;

    if (items.hasNext()) {
      statistic.incrementDuplicatedItems();
      statistic.incrementTotal();

      log.info(firstItem.toJSON());

      while (items.hasNext()) {
        statistic.incrementTotal();
        log.info(items.next().toJSON());
      }
    }
  }

  /**
   * Main method for stand alone execution.
   *
   * @param args search parameters. Template : region tableName indexName. Example Of execution :
   *             java -jar DuplicateFinder.jar eu-central-1 tableName columnName
   */
  public static void main(String[] args) {

    if (args.length != 3) {
      log.info("Wrong input arguments!");
      log.info("Arguments : 1: regionName, 2: tableName, 3: secondaryIndexColumnName");
      log.info("Example: java -jar DuplicateFinder.jar eu-central-1 tableName columnName");

      return;
    }

    String region = args[0];
    String tableName = args[1];
    String secondaryIndex = args[2];

    DuplicateFinder duplicateFinder = new DuplicateFinder(region, tableName);
    duplicateFinder.startSearchOfDuplicates(secondaryIndex);
  }
}

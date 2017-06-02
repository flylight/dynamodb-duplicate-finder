package org.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for DuplicateFinder.
 */
public class DuplicateFinderTest {

  @Test
  public void testWithDuplicates() {
    //GIVEN
    String tableName = "testTable";
    String regionName = "testRegion";
    String secondaryIndexColumnName = "testColumn";

    DynamoDB dynamoDB = mock(DynamoDB.class);

    DynamoDBClient dynamoDBClient = new DynamoDBClient(regionName) {
      @Override
      protected DynamoDB buildDynamoDB(String region) {
        return dynamoDB;
      }
    };

    Table table = mock(Table.class);

    ItemCollection<ScanOutcome> itemCollection = mock(ItemCollection.class);

    Item item1 = mock(Item.class);
    Item item2 = mock(Item.class);

    IteratorSupport<Item, ScanOutcome> itemScanOutcomeIteratorSupport = mock(IteratorSupport.class);

    Index index = mock(Index.class);

    ItemCollection<QueryOutcome> outcomeItemCollection = mock(ItemCollection.class);

    IteratorSupport<Item, QueryOutcome> rowIteratorSupport1 = mock(IteratorSupport.class);
    IteratorSupport<Item, QueryOutcome> rowIteratorSupport2 = mock(IteratorSupport.class);

    Item item1ForRow1 = mock(Item.class);
    Item item2ForRow1 = mock(Item.class);

    Item item1ForRow2 = mock(Item.class);

    //WHEN
    when(dynamoDB.getTable(eq(tableName))).thenReturn(table);

    when(table.scan(Mockito.<ScanSpec>any())).thenReturn(itemCollection);

    when(itemCollection.iterator()).thenReturn(itemScanOutcomeIteratorSupport);

    when(itemScanOutcomeIteratorSupport.hasNext()).thenReturn(true, true, false);
    when(itemScanOutcomeIteratorSupport.next()).thenReturn(item1, item2, null);

    when(item1.getString(eq(secondaryIndexColumnName))).thenReturn("testItem1");
    when(item2.getString(eq(secondaryIndexColumnName))).thenReturn("testItem2");

    when(table.getIndex(eq(secondaryIndexColumnName))).thenReturn(index);

    when(index.query(Mockito.<KeyAttribute>any())).thenReturn(outcomeItemCollection);

    when(outcomeItemCollection.iterator()).thenReturn(rowIteratorSupport1, rowIteratorSupport2);

    when(rowIteratorSupport1.hasNext()).thenReturn(true, true, true, false);
    when(rowIteratorSupport1.next()).thenReturn(item1ForRow1, item2ForRow1, null);

    when(rowIteratorSupport2.hasNext()).thenReturn(true, false);
    when(rowIteratorSupport2.next()).thenReturn(item1ForRow2, null);

    when(item1ForRow1.toJSON()).thenReturn("{test1}");
    when(item2ForRow1.toJSON()).thenReturn("{test2}");
    //THEN
    DuplicateFinder duplicateFinder = new DuplicateFinder(regionName, tableName) {
      @Override
      protected DynamoDBClient buildDynamoDBClient(String region) {
        return dynamoDBClient;
      }
    };

    Statistic statistic = duplicateFinder.startSearchOfDuplicates(secondaryIndexColumnName);

    assertEquals(2, statistic.getTotal());
    assertEquals(1, statistic.getDuplicatedItems());
  }
}

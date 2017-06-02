# Dynamo DB Duplicate Finder

### Description

This is a simple example of how to deal with Dynamo DB Tables and looking for duplicated records using Global Secondary Index.

**Feel free to reuse this approach in your code.**

### Example of usage :

```$java
    String region = eu-central-1;
    String tableName = testTable;
    String secondaryIndex = testColumn;

    DuplicateFinder duplicateFinder = new DuplicateFinder(region, tableName);
    duplicateFinder.startSearchOfDuplicates(secondaryIndex);
```

### Build

This project can be used as **embedded** or **stand alone** solution.

To build project just use **Maven** command : `mvn clean install`. After this just go into `target` 
folder and execute in terminal:

`java -jar DuplicateFinder.jar eu-central-1 testTable testColumn`


## Enjoy!

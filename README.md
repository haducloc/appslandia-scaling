# Appslandia Scaling

## Features

 - **Facebook RocksDB** Utilizing 
   
## Installation

### Maven
```XML
<dependency>
    <groupId>com.appslandia</groupId>
    <artifactId>appslandia-scaling</artifactId>
    <version>{LATEST_VERSION}</version>
</dependency>

<dependency>
    <groupId>com.appslandia</groupId>
    <artifactId>appslandia-common</artifactId>
    <version>{LATEST_VERSION}</version>
</dependency>
```

### Gradle
```
dependencies {
  compile 'com.appslandia:appslandia-scaling:{LATEST_VERSION}'
  compile 'com.appslandia:appslandia-common:{LATEST_VERSION}'
}
```

## Sample Usage

### RocksDB
``` java
List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();

// DEFAULT_COLUMN_FAMILY is required at index 0
descriptors.add(RocksUtils.newCfDescriptor(RocksManager.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
descriptors.add(RocksUtils.newCfDescriptor("other_column_family", new ColumnFamilyOptions()));

DBOptions dbOptions = RocksUtils.newDBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
boolean readOnlyMode = false;
RocksCloseable rocksCloseable = RocksUtils.newRocksCloseable().add(descriptors).add(dbOptions);

try (RocksManager db = RocksManager.open("database_dir", dbOptions, readOnlyMode, descriptors, rocksCloseable)) {

	// db.put, db.get, db.delete, db.newRocksIterator, db.newRocksEntryIterator, etc.
	
} catch (RocksDBException ex) {
	ex.printStackTrace();
}
```

## Questions?
Please feel free to contact me if you have any questions or comments.
Email: haducloc13@gmail.com

## License
This code is distributed under the terms and conditions of the [MIT license](LICENSE).
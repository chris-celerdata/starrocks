---
displayed_sidebar: docs
---

# Metabase

Metabase supports querying and visualizing both internal data and external data in StarRocks.

## Connection methods

There are two ways to connect Metabase to StarRocks:

- **StarRocks driver (recommended)**: A community-maintained driver that provides native StarRocks support, including multi-catalog browsing and compatibility with Metabase v0.50+.
- **MySQL driver (legacy)**: Uses the built-in MySQL driver. This method works on older versions of Metabase but may fail on Metabase v0.50 and later due to MySQL protocol incompatibilities.

## Option 1: StarRocks driver (recommended)

The [StarRocks Metabase driver](https://github.com/Carbon-Arc/metabase-starrocks-driver) resolves MySQL protocol incompatibilities introduced in Metabase v0.50+ and adds first-class support for StarRocks multi-catalog features.

### Prerequisites

- **Metabase**: v0.50 or later
- **StarRocks**: v3.2 or later

### Install the driver

1. Download the latest `starrocks.metabase-driver.jar` from the [driver releases page](https://github.com/Carbon-Arc/metabase-starrocks-driver/releases).

2. Copy the JAR file into your Metabase plugins directory:

   - **Docker**: Mount or copy the JAR into `/plugins/` inside the container.

     ```bash
     docker cp starrocks.metabase-driver.jar metabase:/plugins/
     ```

   - **Local installation**: Place the JAR in the `plugins/` directory relative to where Metabase is installed.

   - **Kubernetes**: Add the JAR to your plugins volume.

3. Restart Metabase.

### Configure the connection

1. In the upper-right corner of the Metabase homepage, click the **Settings** icon and choose **Admin settings**.

2. Choose **Databases** in the top menu bar.

3. On the **Databases** page, click **Add database**.

4. Configure the following parameters and click **Save**:

   - **Database type**: Select **StarRocks**.
   - **Host**: The FE hostname or IP address.
   - **Port**: The MySQL protocol port (default `9030`).
   - **Catalog**: The catalog name (for example, `default_catalog` for internal data, or an external catalog name such as `hive_catalog`).
   - **Database**: Optionally specify a database name. Leave empty to browse all databases in the catalog.
   - **Username** and **Password**: The credentials of your StarRocks cluster user.

   ![Metabase - StarRocks driver configuration](../../_assets/Metabase/Metabase_4.png)

### Known limitations

- Foreign key relationships are not supported by the driver.
- The DECIMAL data type may not display correctly in Metabase.

## Option 2: MySQL driver (legacy)

On Metabase versions earlier than v0.50, you can use the built-in MySQL driver to connect to StarRocks.

> **Note**
>
> This method may not work on Metabase v0.50 and later. Use the StarRocks driver instead.

1. In the upper-right corner of the Metabase homepage, click the **Settings** icon and choose **Admin settings**.

   ![Metabase - Admin settings](../../_assets/Metabase/Metabase_1.png)

2. Choose **Databases** in the top menu bar.

3. On the **Databases** page, click **Add database**.

   ![Metabase - Add database](../../_assets/Metabase/Metabase_2.png)

4. On the page that appears, configure the database parameters and click **Save**.

   - **Database type**: Select **MySQL**.
   - **Host** and **Port**: Enter the host and port information appropriate for your use case.
   - **Database name**: Enter a database name in the `<catalog_name>.<database_name>` format. In StarRocks versions earlier than v3.2, you can integrate only the internal catalog of your StarRocks cluster with Metabase. From StarRocks v3.2 onwards, you can integrate both the internal catalog and external catalogs of your StarRocks cluster with Metabase.
   - **Username** and **Password**: Enter the username and password of your StarRocks cluster user.
   - **Additional JDBC connection string options**: You must add the property `tinyInt1isBit=false` in this field. Otherwise, there may be an error.

   The other parameters do not involve StarRocks. Configure them based on your business needs.

   ![Metabase - Configure database](../../_assets/Metabase/Metabase_3.png)

> **Note**
>
> Please avoid using DECIMAL data types as Metabase does not understand this StarRocks specific column data type when using the MySQL driver.

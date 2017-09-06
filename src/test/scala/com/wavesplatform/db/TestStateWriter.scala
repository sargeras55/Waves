package com.wavesplatform.db

import java.util.Properties

import com.wavesplatform.database.SQLiteWriter
import com.wavesplatform.state2.StateWriter
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.flywaydb.core.Flyway
import scorex.transaction.History

object TestStateWriter {
  def apply(): StateWriter with SnapshotStateReader with History with AutoCloseable = {
    val hc = new HikariConfig()
    val flyway = new Flyway
    val props = new Properties()
    props.put("url", s"jdbc:sqlite::memory:")
    hc.setDataSourceClassName("org.sqlite.SQLiteDataSource")
    flyway.setLocations("db/migration/sqlite")
    // the following line is SUPER IMPORTANT: http://www.sqlite.org/pragma.html#pragma_foreign_keys
    props.setProperty("enforceForeignKeys", "true")
    props.setProperty("lockingMode", "NORMAL")
    //      props.setProperty("incrementalVacuum", "-1")
    props.setProperty("journalMode", "TRUNCATE")
    props.setProperty("cacheSize", "-500000")
    //      hc.setMaximumPoolSize(1)

    hc.setDataSourceProperties(props)
    val hds = new HikariDataSource(hc)

    flyway.setDataSource(hds)
    flyway.migrate()
    hc.setAutoCommit(false)

    new SQLiteWriter(hds) with AutoCloseable {
      override def close(): Unit = hds.close()
    }
  }
}

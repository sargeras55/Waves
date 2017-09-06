package com.wavesplatform.db

import com.typesafe.config.ConfigFactory
import com.wavesplatform.features.FeatureProvider
import com.wavesplatform.history.Domain
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{BlockchainUpdaterImpl, StateWriter}
import scorex.transaction.{DebugNgHistory, History, NgHistory}
import scorex.utils.TimeImpl
import com.wavesplatform.settings.loadConfig

trait WithState {
  def withStateAndHistory(settings: WavesSettings = WavesSettings.fromConfig(loadConfig(ConfigFactory.load())))
                         (test: StateWriter with SnapshotStateReader with History => Any): Unit = {
    val state = TestStateWriter()
    try { test(state) }
    finally { state.close() }
  }

  def withDomain[A](settings: WavesSettings = WavesSettings.fromConfig(loadConfig(ConfigFactory.load())))
                   (test: Domain => A): A = {
    val stateWriter = TestStateWriter()
    val time = new TimeImpl
    val bcu = new BlockchainUpdaterImpl(stateWriter, settings, time, stateWriter)
    val history: NgHistory with DebugNgHistory with FeatureProvider = bcu.historyReader
    try { test(Domain(history, stateWriter, bcu)) }
    finally {
      stateWriter.close()
      time.close()
    }
  }
}

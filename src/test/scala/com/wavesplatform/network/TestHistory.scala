package com.wavesplatform.network

import com.wavesplatform.state2.ByteStr
import scorex.block.Block.BlockId
import scorex.block.{Block, MicroBlock}
import scorex.transaction.History.BlockchainScore
import scorex.transaction.{History, NgHistory}


class TestHistory extends NgHistory {
  def appendId(s: BlockId): Unit = {
    lastBlockIds = s +: lastBlockIds
  }

  override def lastBlockHeaderAndSize = ???

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: BlockId) = ???

  override protected val activationWindowSize: Int = 0

  override def approvedFeatures() = ???

  override def featureVotesCountWithinActivationWindow(height: Int) = ???

  private var lastBlockIds = Seq.empty[BlockId] // fresh head

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = lastBlockIds.take(howMany)

  override def microBlock(id: ByteStr): Option[MicroBlock] = ???

  override def bestLastBlockInfo(maxTimestamp: Long): Option[History.BlockMinerInfo] = ???

  override def blockBytes(height: Int): Option[Array[Byte]] = ???

  override def heightOf(blockId: ByteStr): Option[Int] = ???

  override def height: Int = ???

  override def scoreOf(id: ByteStr): Option[BlockchainScore] = ???

  override def score = ???

  override def lastBlock = ???

  override def blockBytes(blockId: BlockId) = ???

  override def blockIdsAfter(parentSignature: BlockId, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???
}

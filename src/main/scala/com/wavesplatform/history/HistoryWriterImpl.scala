package com.wavesplatform.history

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.primitives.{Ints, Shorts}
import com.wavesplatform.db._
import com.wavesplatform.features.FeatureProvider
import com.wavesplatform.settings.{FeaturesSettings, FunctionalitySettings}
import com.wavesplatform.state2._
import com.wavesplatform.utils._
import kamon.Kamon
import org.iq80.leveldb.DB
import scorex.block.{Block, BlockHeader}
import scorex.transaction.History.BlockchainScore
import scorex.transaction.ValidationError.GenericError
import scorex.transaction._
import scorex.utils.Synchronized.WriteLock
import scorex.utils.{NTP, ScorexLogging, Time}

import scala.util.Try

class HistoryWriterImpl private(db: DB, val synchronizationToken: ReentrantReadWriteLock,
                                functionalitySettings: FunctionalitySettings, featuresSettings: FeaturesSettings, time: Time)
  extends SubStorage(db, "history") with PropertiesStorage with VersionedStorage with History with FeatureProvider with ScorexLogging {

  override protected val Version: Int = 1

  import HistoryWriterImpl._

  override val activationWindowSize: Int = functionalitySettings.featureCheckBlocksPeriod
  val MinVotesWithinWindowToActivateFeature: Int = functionalitySettings.blocksForFeatureActivation

  private val BlockAtHeightPrefix = "blocks".getBytes(Charset)
  private val SignatureAtHeightPrefix = "signatures".getBytes(Charset)
  private val HeightBySignaturePrefix = "heights".getBytes(Charset)
  private val ScoreAtHeightPrefix = "scores".getBytes(Charset)
  private val VotesAtHeightPrefix = "votes".getBytes(Charset)
  private val FeatureStatePrefix = "features".getBytes(Charset)
  private val FeaturesIndexKey = makeKey("feature-index".getBytes(Charset), 0)

  private val HeightProperty = "history-height"

  private lazy val preAcceptedFeatures = functionalitySettings.preActivatedFeatures.mapValues(h => h - activationWindowSize)

  private var heightInfo: (Int, Long) = (height(), time.getTimestamp())

  override def approvedFeatures(): Map[Short, Int] = read { implicit lock =>
    preAcceptedFeatures ++ getFeaturesState
  }

  override def featureVotesCountWithinActivationWindow(height: Int): Map[Short, Int] = read { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize)
    get(makeKey(VotesAtHeightPrefix, votingWindowOpening)).map(VotesMapCodec.decode).map(_.explicitGet().value).getOrElse(Map.empty)
  }

  private def alterVotes(height: Int, votes: Set[Short], voteMod: Int): Unit = write("alterVotes") { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize)
    val votesWithinWindow = featureVotesCountWithinActivationWindow(height)
    val newVotes = votes.foldLeft(votesWithinWindow)((v, feature) => v + (feature -> (v.getOrElse(feature, 0) + voteMod)))
    put(makeKey(VotesAtHeightPrefix, votingWindowOpening), VotesMapCodec.encode(newVotes))
  }

  def appendBlock(block: Block, acceptedFeatures: Set[Short])(consensusValidation: => Either[ValidationError, BlockDiff]): Either[ValidationError, BlockDiff] =
    write("appendBlock") { implicit lock =>

      assert(block.signaturesValid().isRight)

      if ((height() == 0) || (this.lastBlock.get.uniqueId == block.reference)) consensusValidation.map { blockDiff =>
        val h = height() + 1
        val score = (if (height() == 0) BigInt(0) else this.score()) + block.blockScore()
        put(makeKey(BlockAtHeightPrefix, h), block.bytes())
        put(makeKey(ScoreAtHeightPrefix, h), score.toByteArray)
        put(makeKey(SignatureAtHeightPrefix, h), block.uniqueId.arr)
        put(makeKey(HeightBySignaturePrefix, block.uniqueId.arr), Ints.toByteArray(h))
        setHeight(h)

        val presentFeatures = allFeatures().toSet
        val newFeatures = acceptedFeatures.diff(presentFeatures)
        newFeatures.foreach(f => addFeature(f, h))
        alterVotes(h, block.featureVotes, 1)

        blockHeightStats.record(h)
        blockSizeStats.record(block.bytes().length)
        transactionsInBlockStats.record(block.transactionData.size)

        log.trace(s"Full Block $block(id=${block.uniqueId} persisted")
        blockDiff
      }
      else {
        Left(GenericError(s"Parent ${block.reference} of block ${block.uniqueId} does not match last block ${this.lastBlock.map(_.uniqueId)}"))
      }
    }

  private def allFeatures(): Seq[Short] = read { implicit lock =>
    get(FeaturesIndexKey).map(ShortSeqCodec.decode).map(_.explicitGet().value).getOrElse(Seq.empty[Short])
  }

  private def addFeature(featureId: Short, height: Int): Unit = {
    val features = (allFeatures() :+ featureId).distinct
    put(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId)), Ints.toByteArray(height))
    put(FeaturesIndexKey, ShortSeqCodec.encode(features))
  }

  private def deleteFeature(featureId: Short): Unit = {
    val features = allFeatures().filterNot(f => f == featureId).distinct
    delete(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId)))
    put(FeaturesIndexKey, ShortSeqCodec.encode(features))
  }

  private def getFeatureHeight(featureId: Short): Option[Int] =
    get(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId))).flatMap(b => Try(Ints.fromByteArray(b)).toOption)

  private def getFeaturesState(): Map[Short, Int] = {
    allFeatures().foldLeft(Map.empty[Short, Int]) { (r, f) =>
      val h = getFeatureHeight(f)
      if (h.isDefined) r.updated(f, h.get) else r
    }
  }

  def discardBlock(): Option[Block] = write("discardBlock") { implicit lock =>
    val h = height()

    alterVotes(h, blockAt(h).map(b => b.featureVotes).getOrElse(Set.empty), -1)

    val key = makeKey(BlockAtHeightPrefix, h)
    val maybeBlockBytes = get(key)
    val tryDiscardedBlock = maybeBlockBytes.map(b => Block.parseBytes(b))
    val maybeDiscardedBlock = tryDiscardedBlock.flatMap(_.toOption)


    delete(key)
    delete(makeKey(ScoreAtHeightPrefix, h))

    if (h % activationWindowSize == 0) {
      allFeatures().foreach { f =>
        val featureHeight = getFeatureHeight(f)
        if (featureHeight.isDefined && featureHeight.get == h) deleteFeature(f)
      }
    }

    val signatureKey = makeKey(SignatureAtHeightPrefix, h)
    get(signatureKey).foreach(b => delete(makeKey(HeightBySignaturePrefix, b)))
    delete(signatureKey)

    setHeight(h - 1)

    maybeDiscardedBlock
  }

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = read { implicit lock =>
    val startHeight = Math.max(1, height - howMany + 1)
    (startHeight to height).flatMap(getBlockSignature).reverse
  }

  override def height(): Int = read { implicit lock =>
    getIntProperty(HeightProperty).getOrElse(0)
  }

  private def setHeight(x: Int)(implicit lock: WriteLock): Unit = {
    putIntProperty(HeightProperty, x)
    heightInfo = (x, time.getTimestamp())
  }

  override def scoreOf(id: ByteStr): Option[BlockchainScore] = read { implicit lock =>
    val maybeHeight = heightOf(id)
    if (maybeHeight.isDefined) {
      val maybeScoreBytes = get(makeKey(ScoreAtHeightPrefix, maybeHeight.get))
      if (maybeScoreBytes.isDefined) Some(BigInt(maybeScoreBytes.get)) else None
    } else None
  }

  override def heightOf(blockSignature: ByteStr): Option[Int] = read { implicit lock =>
    get(makeKey(HeightBySignaturePrefix, blockSignature.arr)).map(Ints.fromByteArray)
  }

  override def blockBytes(height: Int): Option[Array[Byte]] = read { implicit lock =>
    get(makeKey(BlockAtHeightPrefix, height))
  }

  override def close(): Unit = db.close()

  override def lastBlockTimestamp(): Option[Long] = this.lastBlock.map(_.timestamp)

  override def lastBlockId(): Option[ByteStr] = this.lastBlock.map(_.signerData.signature)

  override def blockAt(height: Int): Option[Block] = blockBytes(height).map(Block.parseBytes(_).get)

  override def blockHeaderAndSizeAt(height: Int): Option[(BlockHeader, Int)] =
    blockBytes(height).map(bytes => (BlockHeader.parseBytes(bytes).get._1, bytes.length))

  override def debugInfo: HeightInfo = heightInfo

  private def getBlockSignature(height: Int): Option[ByteStr] = get(makeKey(SignatureAtHeightPrefix, height)).map(ByteStr.apply)
}

object HistoryWriterImpl extends ScorexLogging {
  def apply(db: DB, synchronizationToken: ReentrantReadWriteLock, functionalitySettings: FunctionalitySettings,
            featuresSettings: FeaturesSettings, time: Time = NTP): Try[HistoryWriterImpl] =
    createWithVerification[HistoryWriterImpl](new HistoryWriterImpl(db, synchronizationToken, functionalitySettings, featuresSettings, time))

  private val blockHeightStats = Kamon.metrics.histogram("block-height")
  private val blockSizeStats = Kamon.metrics.histogram("block-size-bytes")
  private val transactionsInBlockStats = Kamon.metrics.histogram("transactions-in-block")
}

package com.wavesplatform.state2.diffs

import cats.Monoid
import cats.implicits._
import com.wavesplatform.features.{BlockchainFeatures, FeatureProvider}
import com.wavesplatform.metrics.Instrumented
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2._
import com.wavesplatform.state2.patch.{CancelAllLeases, CancelLeaseOverflow}
import com.wavesplatform.state2.reader.CompositeStateReader.composite
import com.wavesplatform.state2.reader.SnapshotStateReader
import scorex.account.Address
import scorex.block.{Block, MicroBlock}
import scorex.transaction.ValidationError.ActivationError
import scorex.transaction.{Transaction, ValidationError}
import scorex.utils.ScorexLogging

object BlockDiffer extends ScorexLogging with Instrumented {

  def right(diff: Diff): Either[ValidationError, Diff] = Right(diff)

  def fromBlock(settings: FunctionalitySettings, fp: FeatureProvider, s: SnapshotStateReader, maybePrevBlock: Option[Block], block: Block): Either[ValidationError, Diff] = {
    val blockSigner = block.signerData.generator.toAddress
    val stateHeight = s.height

    // height switch is next after activation
    val ng4060switchHeight = fp.featureActivationHeight(BlockchainFeatures.NG.id).getOrElse(Int.MaxValue)

    lazy val prevBlockFeeDistr: Option[Diff] =
      if (stateHeight > ng4060switchHeight)
        maybePrevBlock
          .map(prevBlock => Diff.empty.copy(
            portfolios = Map(blockSigner -> prevBlock.prevBlockFeePart())))
      else None

    lazy val currentBlockFeeDistr =
      if (stateHeight < ng4060switchHeight)
        Some(Diff.empty.copy(portfolios = Map(blockSigner -> block.feesPortfolio())))
      else
        None

    val prevBlockTimestamp = maybePrevBlock.map(_.timestamp)
    for {
      _ <- block.signaturesValid()
      r <- apply(settings, s, prevBlockTimestamp)(block.signerData.generator, prevBlockFeeDistr, currentBlockFeeDistr, block.timestamp, block.transactionData, 1)
    } yield r
  }

  def fromMicroBlock(settings: FunctionalitySettings, fp: FeatureProvider, s: SnapshotStateReader, pervBlockTimestamp: Option[Long], micro: MicroBlock, timestamp: Long): Either[ValidationError, Diff] = {
    for {
      // microblocks are processed within block which is next after 40-only-block which goes on top of activated height
      _ <- Either.cond(fp.featureActivationHeight(BlockchainFeatures.NG.id).exists(s.height > _), (), ActivationError(s"MicroBlocks are not yet activated, current height=${s.height}"))
      _ <- micro.signaturesValid()
      r <- apply(settings, s, pervBlockTimestamp)(micro.generator, None, None, timestamp, micro.transactionData, 0)
    } yield r
  }

  private def apply(settings: FunctionalitySettings, s: SnapshotStateReader, pervBlockTimestamp: Option[Long])
                   (blockGenerator: Address, prevBlockFeeDistr: Option[Diff], currentBlockFeeDistr: Option[Diff],
                    timestamp: Long, txs: Seq[Transaction], heightDiff: Int): Either[ValidationError, Diff] = {
    val currentBlockHeight = s.height + heightDiff
    val txDiffer = TransactionDiffer(settings, pervBlockTimestamp, timestamp, currentBlockHeight) _

    val txsDiffEi = currentBlockFeeDistr match {
      case Some(feedistr) =>
        txs.foldLeft(right(Monoid.combine(prevBlockFeeDistr.orEmpty, feedistr))) { case (ei, tx) => ei.flatMap(diff =>
          txDiffer(composite(diff, s), tx)
            .map(newDiff => diff.combine(newDiff)))
        }
      case None =>
        txs.foldLeft(right(prevBlockFeeDistr.orEmpty)) { case (ei, tx) => ei.flatMap(diff =>
          txDiffer(composite(diff, s), tx)
            .map(newDiff => diff.combine(newDiff.copy(portfolios = newDiff.portfolios.combine(Map(blockGenerator -> tx.feeDiff()).mapValues(_.multiply(Block.CurrentBlockFeePart)))))))
        }
    }

    txsDiffEi.map { d =>
      val diffWithCancelledLeases = if (currentBlockHeight == settings.resetEffectiveBalancesAtHeight)
        Monoid.combine(d, CancelAllLeases(composite(d, s)))
      else d

      val diffWithLeasePatches = if (currentBlockHeight == settings.blockVersion3AfterHeight)
        Monoid.combine(diffWithCancelledLeases, CancelLeaseOverflow(composite(diffWithCancelledLeases, s)))
      else diffWithCancelledLeases

      val newSnapshots = diffWithLeasePatches.portfolios
        .map { case (acc, portfolioDiff) =>
          val oldWavesBalance = s.wavesBalance(acc)
          val newWavesBalance = if (portfolioDiff.balance != 0 || portfolioDiff.effectiveBalance != 0)
            Some(safeSum(oldWavesBalance, portfolioDiff.balance))
            else None
          val stateAssetBalances = s.assetBalance(acc)
          val assetBalances: Map[ByteStr, Long] = if (portfolioDiff.assets.isEmpty) Map.empty else {
            portfolioDiff.assets.map {
              case (assetId, diff) =>
                val newValue = safeSum(stateAssetBalances.getOrElse(assetId, 0L), diff)
                require(newValue >= 0, s"Negative balance $newValue for asset $assetId on $acc")
                assetId -> newValue
            }
          }
          acc -> Snapshot(newWavesBalance, assetBalances = assetBalances)
        }
      diffWithLeasePatches
    }
  }
}

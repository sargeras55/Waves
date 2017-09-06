package com.wavesplatform.database

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.wavesplatform.state2.reader.{LeaseDetails, SnapshotStateReader}
import com.wavesplatform.state2.{AssetDescription, AssetInfo, ByteStr, Diff, LeaseInfo, OrderFillInfo, StateWriter}
import scalikejdbc.{DB, DBSession, using, _}
import scorex.account.{Address, AddressOrAlias, Alias, PublicKeyAccount}
import scorex.block.{Block, BlockHeader, SignerData}
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.transaction.{TransactionParser, _}
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}

class SQLiteWriter(ds: DataSource) extends SnapshotStateReader with StateWriter with History {

  private def readOnly[A](f: DBSession => A): A = using(DB(ds.getConnection))(_.localTx(f))

  override def scoreOf(blockId: ByteStr) = ???

  override def lastBlockHeaderAndSize = readOnly { implicit s =>
    val featureVotes: Set[Short] = Set.empty
    sql"""with block_meta as (select * from blocks order by height desc limit 1)
         |select
         |bm.block_timestamp,
         |bm.version,
         |bm.reference,
         |bm.generator_public_key, block_id,
         |bm.base_target, generation_signature,
         |count(tx.height),
         |length(bm.block_data_bytes)
         |from block_meta bm
         |left join transactions tx on tx.height = bm.height
         |group by bm.height""".stripMargin
      .map { rs =>
        (new BlockHeader(
          rs.get[Long](1),
          rs.get[Byte](2),
          rs.get[ByteStr](3),
          SignerData(PublicKeyAccount(rs.get[Array[Byte]](4)), rs.get[ByteStr](5)),
          NxtLikeConsensusBlockData(rs.get[Long](6), rs.get[ByteStr](7)),
          rs.get[Int](8),
          featureVotes
        ), rs.get[Int](9))
      }.single().apply()
  }

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: ByteStr) = ???

  override def status = ???

  override def blockBytes(height: Int) = readOnly { implicit s =>
    sql"select block_body_bytes from blocks where height = ?"
      .bind(height)
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
  }

  override def blockBytes(blockId: ByteStr) = readOnly { implicit s =>
    sql"select block_body_bytes from blocks where block_id = ?"
      .bind(blockId.base58)
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
  }

  override protected val activationWindowSize: Int = 1000

  private def loadApprovedFeatures()(implicit s: DBSession) = {
    sql"select * from accepted_features"
      .map(rs => (rs.get[Short](2), rs.get[Int](1)))
      .list()
      .apply()
      .toMap
  }

  private var approvedFeaturesCache = readOnly(loadApprovedFeatures()(_))

  override def approvedFeatures() = approvedFeaturesCache

  override def featureVotesCountWithinActivationWindow(height: Int) = Map.empty

  override def heightOf(blockId: ByteStr) = readOnly { implicit s =>
    sql"select height from blocks where block_id = ?"
      .bind(blockId.arr)
      .map(_.get[Int](1))
      .single()
      .apply()
  }

  override def lastBlockIds(howMany: Int) = ???

  private def loadScore()(implicit s: DBSession) = {
    sql"select cumulative_score from blocks order by height desc limit 1"
      .map(rs => BigInt(rs.get[String](1)))
      .single()
      .apply()
      .getOrElse(BigInt(0))
  }

  private var scoreCache = readOnly(loadScore()(_))

  override def score = scoreCache

  override def lastBlock = readOnly { implicit s =>
    sql"select block_data_bytes from blocks order by height desc limit 1"
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
      .map(bytes => Block.parseBytes(bytes).get)
  }

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def nonZeroLeaseBalances = readOnly { implicit s =>
    sql"""with last_balance as (select address, max(height) height from lease_balances group by address)
         |select lb.* from lease_balances lb, last_balance
         |where lb.address = last_balance.address
         |and lb.height = last_balance.height
         |and (lease_in <> 0 or lease_out <> 0)""".stripMargin
      .map(rs => Address.fromString(rs.get[String](1)).right.get -> LeaseInfo(rs.get[Long](2), rs.get[Long](3)))
      .list()
      .apply()
      .toMap
  }

  override def transactionInfo(id: ByteStr) = ???

  override def containsTransaction(id: ByteStr) = readOnly { implicit s =>
    sql"select 1 from transactions where tx_id = ?"
      .bind(id)
      .map(_.get[Int](1))
      .single()
      .apply()
      .isDefined
  }

  override def addressTransactions(address: Address,
                                   types: Set[TransactionParser.TransactionType.Value],
                                   from: Int, count: Int) = readOnly { implicit s =>
    ???
  }

  private val balanceCache = CacheBuilder
    .newBuilder()
    .maximumSize(100000).build(new CacheLoader[Address, java.lang.Long] {
    override def load(key: Address) = readOnly { implicit s =>
      sql"""select wb.regular_balance from waves_balances wb
           |where wb.address = ?
           |order by wb.height desc
           |limit 1""".stripMargin
        .bind(key.address)
        .map(_.get[java.lang.Long](1))
        .single()
        .apply()
        .getOrElse(0L)
    }
  })

  override def wavesBalance(a: Address) = balanceCache.get(a)

  override def leaseInfo(a: Address) = readOnly { implicit s =>
    sql"select lease_in, lease_out from lease_balances where address = ? order by height desc limit 1"
      .bind(a.address)
      .map(rs => LeaseInfo(rs.get[Long](1), rs.get[Long](2)))
      .single()
      .apply()
      .getOrElse(LeaseInfo(0, 0))
  }

  private val assetBalanceCache = CacheBuilder
    .newBuilder()
    .maximumSize(100000)
    .build(new CacheLoader[Address, Map[ByteStr, Long]] {
      override def load(key: Address) = readOnly { implicit s =>
        sql"""with latest_heights as (
             |  select address, asset_id, max(height) height
             |  from asset_balances
             |  where address = ? group by address, asset_id)
             |select ab.asset_id, ab.balance from asset_balances ab, latest_heights lh
             |where ab.height = lh.height
             |and ab.asset_id = lh.asset_id
             |and ab.address = lh.address""".stripMargin
          .bind(key.address)
          .map(rs => rs.get[ByteStr](1) -> rs.get[Long](2))
          .list()
          .apply()
          .toMap
      }
    })

  override def assetBalance(a: Address) = assetBalanceCache.get(a)

  private val assetInfoCache = CacheBuilder.newBuilder()
    .recordStats()
    .maximumSize(10000)
    .build(new CacheLoader[ByteStr, Option[AssetInfo]] {
      override def load(key: ByteStr) = readOnly { implicit s =>
        sql"select reissuable, quantity from asset_quantity where asset_id = ? order by height desc limit 1"
          .bind(key.arr)
          .map { rs => AssetInfo(rs.get[Boolean](1), rs.get[Long](2)) }
          .single()
          .apply()
      }
    })

  private val assetDescriptionCache = CacheBuilder.newBuilder()
    .recordStats()
    .maximumSize(10000)
    .build(new CacheLoader[ByteStr, Option[AssetDescription]] {
      override def load(key: ByteStr) = readOnly { implicit s =>
        sql"""select ai.issuer, ai.name, ai.decimals, min(aq.reissuable)
             |from asset_info ai, asset_quantity aq
             |where ai.asset_id = aq.asset_id
             |and ai.asset_id = ?
             |group by ai.issuer, ai.name, ai.decimals""".stripMargin
          .bind(key)
          .map { rs => AssetDescription(
            PublicKeyAccount(rs.get[Array[Byte]](1)),
            rs.get[Array[Byte]](2),
            rs.get[Int](3),
            rs.get[Boolean](4)) }
          .single()
          .apply()
      }
    })

  override def assetDescription(id: ByteStr) = assetDescriptionCache.get(id)

  private val h = new AtomicInteger(readOnly { implicit s =>
    sql"select coalesce(max(height), 0) from blocks".map(_.get[Int](1)).single().apply().getOrElse(0)
  })

  override def height = h.get()


  override def paymentTransactionIdByHash(hash: ByteStr) = readOnly { implicit s =>
    sql"select * from payment_transactions where tx_hash = ?"
      .bind(hash.arr)
      .map(rs => ByteStr(rs.get[Array[Byte]](1)))
      .single()
      .apply()
  }

  override def aliasesOfAddress(a: Address) = readOnly { implicit s =>
    sql"select alias from aliases where address = ?"
      .bind(a.address)
      .map(rs => Alias.fromString(rs.get[String](1)).right.get)
      .list()
      .apply()
  }

  override def resolveAlias(a: Alias) = readOnly { implicit s =>
    sql"select address from aliases where alias = ?"
      .bind(a.stringRepr)
      .map(rs => Address.fromString(rs.get[String](1)).right.get)
      .single()
      .apply()
  }

  override def activeLeases = readOnly { implicit s =>
    sql"select lease_id from lease_status group by lease_id having not min(active)"
      .map(rs => ByteStr(rs.get[Array[Byte]](1)))
      .list()
      .apply()
  }

  override def leaseOverflows = readOnly { implicit s =>
    sql"""with
         |    balance_heights as (select address, max(height) height from waves_balances group by address),
         |    lease_heights as (select address, max(height) height from lease_balances group by address)
         |select lh.address, lb.lease_out from lease_heights lh
         |left join balance_heights bh using(address)
         |left join waves_balances wb on wb.address = bh.address and wb.height = bh.height
         |left join lease_balances lb on lb.address = lh.address and lb.height = lh.height
         |where ifnull(wb.regular_balance, 0) - lb.lease_out < 0""".stripMargin
      .map(rs => Address.fromString(rs.get[String](1)).right.get -> rs.get[Long](2))
      .list()
      .apply()
      .toMap
  }

  override def leasesOf(address: Address) = readOnly { implicit s =>
    sql"""with latest_lease_status as (select lease_id, min(active) active from lease_status group by lease_id)
         |select li.*, lls.active
         |from lease_info li, latest_lease_status lls, address_transaction_ids ati
         |where ati.address = ?
         |and ati.tx_id = li.lease_id
         |and lls.lease_id = li.lease_id""".stripMargin
      .bind(address.address)
      .map(rs => ByteStr(rs.get[Array[Byte]](1)) -> LeaseDetails(
        PublicKeyAccount(rs.get[Array[Byte]](2)),
        AddressOrAlias.fromString(rs.get[String](3)).right.get,
        rs.get[Int](5),
        rs.get[Long](4),
        rs.get[Boolean](6)))
      .list()
      .apply()
      .toMap
  }

  override def leaseDetails(leaseId: ByteStr) = readOnly { implicit s =>
    sql"""with this_lease_status as (
         |select lease_id, min(active) active from lease_status where lease_id = ? group by lease_id)
         |select li.*, tl.active from lease_info li, this_lease_status tl
         |where li.lease_id = tl.lease_id""".stripMargin
      .bind(leaseId.arr)
      .map(rs => LeaseDetails(
        PublicKeyAccount(rs.get[Array[Byte]](2)),
        AddressOrAlias.fromString(rs.get[String](3)).right.get,
        rs.get[Int](5),
        rs.get[Long](4),
        rs.get[Boolean](6)))
      .single()
      .apply()
  }

  override def lastUpdateHeight(acc: Address) = readOnly { implicit s =>
    sql"select height from waves_balances where address = ? order by height desc limit 1"
      .bind(acc.address)
      .map(_.get[Option[Int]](1)).single.apply().flatten
  }

  override def snapshotAtHeight(acc: Address, h: Int) = ???

  override def filledVolumeAndFee(orderId: ByteStr) = readOnly { implicit s =>
    sql"""with this_order as (select ? order_id)
         |select ifnull(fq.filled_quantity, 0), ifnull(fq.fee, 0) from this_order tho
         |left join filled_quantity fq on tho.order_id = fq.order_id
         |order by fq.height desc limit 1""".stripMargin
      .bind(orderId.arr)
      .map(rs => OrderFillInfo(rs.get[Long](1), rs.get[Long](2)))
      .single()
      .apply()
      .getOrElse(OrderFillInfo(0, 0))
  }


  def effectiveBalanceAtHeightWithConfirmations(acc: Address, atHeight: Int, confirmations: Int) =
    readOnly { implicit s =>
      sql"""with lowest_height as (
           |    select height, address from waves_balances
           |    where address = ? and height <= ?
           |    order by height desc limit 1)
           |select coalesce(min(wb.effective_balance), 0) from waves_balances wb, lowest_height lh
           |where wb.address = lh.address
           |and wb.height <= ?
           |and wb.height >= lh.height""".stripMargin
        .bind(acc.address, atHeight - confirmations, atHeight)
        .map(_.get[Long](1))
        .single()
        .apply()
        .getOrElse(0L)
    }

  override def rollbackTo(targetBlockId: ByteStr) = using(DB(ds.getConnection))(_.localTx { implicit s =>
    val targetHeight = sql"select height from blocks where block_id = ?"
      .bind(targetBlockId.arr)
      .map(_.get[Int](1))
      .single()
      .apply()

    val blocks = targetHeight.fold(Seq.empty[Block]) { h =>
      val recoveredTransactions = sql"select block_data_bytes from blocks where height > ?"
        .bind(h)
        .map(_.get[Array[Byte]](1))
        .list()
        .apply()
        .map(Block.parseBytes(_).get)

      sql"delete from blocks where height > ?"
        .bind(h)
        .update()
        .apply()

      recoveredTransactions
    }

    scoreCache = loadScore()
    approvedFeaturesCache = loadApprovedFeatures()

    blocks
  })

  override def append(diff: Diff, block: Block): Unit = {
    using(DB(ds.getConnection)) { db =>
      db.localTx { implicit s =>
        val newHeight = sql"select coalesce(max(height), 0) from blocks".map(_.get[Int](1)).single().apply().fold(1)(_ + 1)

        h.set(newHeight)
        scoreCache += block.blockScore()

        // todo: update caches

        storeBlocks(block, newHeight)
        storeTransactions(diff, newHeight)
        storeIssuedAssets(diff, newHeight)
        storeReissuedAssets(diff, newHeight)
        storeFilledQuantity(diff, newHeight)
        storeLeaseInfo(diff, newHeight)

        sql"insert into lease_status (lease_id, active, height) values (?,?,?)"
          .batch(diff.transactions.collect {
            case (_, (_, lt: LeaseTransaction, _)) => Seq(lt.id().arr, true, newHeight)
            case (_, (_, lc: LeaseCancelTransaction, _)) => Seq(lc.leaseId.arr, false, newHeight)
          }.toSeq: _*)
          .apply()

        storeLeaseBalances(diff, newHeight)
        storeWavesBalances(diff, newHeight)
        storeAssetBalances(diff, newHeight)

        sql"insert into aliases (alias, address, height) values (?,?,?)"
          .batch(diff.transactions.values.collect {
            case (_, cat: CreateAliasTransaction, _) => Seq(cat.alias.stringRepr, cat.sender.toAddress.address, newHeight)
          }.toSeq: _*)
          .apply()

        storeAddressTransactionIds(diff, newHeight)

        if (newHeight % 2000 == 0) {
          sql"analyze".update().apply()

          val heightParams = Seq(newHeight - 4000, newHeight - 2000, newHeight - 4000)

          sql"""with last_asset_changes as (
               |    select address, asset_id from asset_balances where height between ? and ? group by address, asset_id)
               |delete from asset_balances where (address, asset_id) in last_asset_changes and height < ?""".stripMargin
            .bind(heightParams: _*)
            .update()
            .apply()

          sql"""with last_changes as (select address from waves_balances where height between ? and ? group by address)
               |delete from waves_balances where waves_balances.address in last_changes and height < ?""".stripMargin
            .bind(heightParams: _*)
            .update()
            .apply()
        }
      }
    }
  }

  private def storeWavesBalances(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    val wavesBalances = diff.portfolios
      .collect { case (address, p) if p.balance != 0 => address -> (p.balance + wavesBalance(address)) }
      .toSeq

    sql"insert into waves_balances (address, regular_balance, height) values(?, ?, ?)"
      .batch((for {
        (address, balance) <- wavesBalances
      } yield Seq(address.stringRepr, balance, newHeight)): _*)
      .apply()

    for ((a, b) <- wavesBalances) {
      balanceCache.put(a, b)
    }
  }

  private def storeAssetBalances(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    val assetBalanceParams = for {
      (address, portfolio) <- diff.portfolios
      (assetId, balance) <- portfolio.assets
      if balance != 0
    } yield Seq(address.stringRepr, assetId, balance + assetBalance(address).getOrElse(assetId, 0L))

    sql"insert into asset_balances (address, asset_id, balance, height) values (?,?,?,?)"
      .batch(assetBalanceParams.toSeq: _*)
      .apply()

    diff.portfolios.foreach {
      case (a, p) =>
        val cachedPortfolio = assetBalance(a)
        val updatedPortfolio = for {
          k <- p.assets.keySet ++ cachedPortfolio.keySet
        } yield k -> (BigInt(p.assets.getOrElse(k, 0L)) + BigInt(cachedPortfolio.getOrElse(k, 0L))).bigInteger.longValueExact()

        assetBalanceCache.put(a, updatedPortfolio.toMap)
    }
  }

  private def storeLeaseBalances(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    sql"""insert into lease_balances (address, lease_in, lease_out, height)
         |with this_lease as (select ? address)
         |select tl.address, ifnull(lb.lease_in, 0) + ?, ifnull(lb.lease_out, 0) + ?, ?
         |from this_lease tl
         |left join lease_balances lb on tl.address = lb.address
         |order by lb.height desc limit 1""".stripMargin
      .batch((for {
        (address, p) <- diff.portfolios
        if p.leaseInfo.leaseIn != 0 || p.leaseInfo.leaseOut != 0
      } yield Seq(address.address, p.leaseInfo.leaseIn, p.leaseInfo.leaseOut, newHeight)).toSeq: _*)
      .apply()
  }

  private def storeLeaseInfo(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    sql"insert into lease_info (lease_id, sender, recipient, amount, height) values (?,?,?,?,?)"
      .batch(diff.transactions.collect {
        case (_, (_, lt: LeaseTransaction, _)) =>
          Seq(lt.id().arr, lt.sender.publicKey, lt.recipient.stringRepr, lt.amount, newHeight)
      }.toSeq: _*)
      .apply()
  }

  private def storeAddressTransactionIds(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    sql"insert into address_transaction_ids (address, tx_id, signature, height) values (?,?,?,?)"
      .batch((for {
        (_, (_, tx, addresses)) <- diff.transactions
        address <- addresses
      } yield tx match {
        case pt: PaymentTransaction => Seq(address.address, ByteStr(pt.hash()), pt.signature, newHeight)
        case t: SignedTransaction => Seq(address.address, t.id(), t.signature, newHeight)
        case gt: GenesisTransaction => Seq(address.address, gt.id(), gt.signature, newHeight)
      }).toSeq: _*)
      .apply()
  }

  private def storeFilledQuantity(diff: Diff, newHeight: Int)(implicit session: DBSession) =
    sql"""insert into filled_quantity(order_id, filled_quantity, fee, height)
         |with this_order as (select ? order_id)
         |select tho.order_id, ifnull(fq.filled_quantity, 0) + ?, ifnull(fq.fee, 0) + ?, ? from this_order tho
         |left join filled_quantity fq on tho.order_id = fq.order_id
         |order by fq.height desc limit 1""".stripMargin
      .batch((for {
        (orderId, fillInfo) <- diff.orderFills
      } yield Seq(orderId.arr, fillInfo.volume, fillInfo.fee, newHeight)).toSeq: _*)
      .apply()

  private def storeReissuedAssets(diff: Diff, newHeight: Int)(implicit session: DBSession): Unit = {
    sql"insert into asset_quantity (asset_id, quantity_change, reissuable, height) values (?,?,?,?)"
      .batch(diff.issuedAssets.map { case (id, ai) => Seq(id, ai.volume, ai.isReissuable, newHeight) }.toSeq: _*)
      .apply()
  }

  private def storeIssuedAssets(diff: Diff, newHeight: Int)(implicit session: DBSession): Unit = {
    val issuedAssetParams = diff.transactions.values.collect {
      case (_, i: IssueTransaction, _) =>
        Seq(i.assetId(), i.sender.publicKey, i.decimals, i.name, i.description, newHeight): Seq[Any]
    }.toSeq

    sql"insert into asset_info(asset_id, issuer, decimals, name, description, height) values (?,?,?,?,?,?)"
      .batch(issuedAssetParams: _*)
      .apply()

    for ((id, ai) <- diff.issuedAssets) {
      assetInfoCache.put(id, Some(ai))
      assetDescriptionCache.invalidate(id)
    }
  }

  private def storeBlocks(block: Block, newHeight: Int)(implicit session: DBSession): Unit =
    sql"""insert into blocks (
         |height, block_id, reference, version, block_timestamp, generator_address, generator_public_key,
         |base_target, generation_signature, block_data_bytes, cumulative_score)
         |values (?,?,?,?,?,?,?,?,?,?,?)""".stripMargin
      .bind(newHeight, block.uniqueId, block.reference, block.version, new Timestamp(block.timestamp),
        block.signerData.generator.toAddress.stringRepr, block.signerData.generator.publicKey,
        block.consensusData.baseTarget, block.consensusData.generationSignature, block.bytes(), scoreCache)
      .update()
      .apply()

  private def storeTransactions(diff: Diff, newHeight: Int)(implicit session: DBSession) = {
    val transactionParams = Seq.newBuilder[Seq[Any]]

    diff.transactions.values.foreach {
      case (_, pt: PaymentTransaction, _) =>
        transactionParams += Seq(ByteStr(pt.hash()), pt.signature, pt.transactionType.id, newHeight)
      case (_, t: SignedTransaction, _) =>
        transactionParams += Seq(t.id(), t.signature, t.transactionType.id, newHeight)
      case (_, gt: GenesisTransaction, _) =>
        transactionParams += Seq(gt.id(), gt.signature, gt.transactionType.id, newHeight)
    }

    sql"insert into transactions (tx_id, signature, tx_type, height) values (?,?,?,?)"
      .batch(transactionParams.result(): _*)
      .apply()
  }
}

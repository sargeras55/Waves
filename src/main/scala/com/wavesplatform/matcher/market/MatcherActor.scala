package com.wavesplatform.matcher.market

import akka.actor.{Actor, ActorRef, Props}
import com.wavesplatform.matcher.market.MatcherActor.OrderResponse
import com.wavesplatform.matcher.market.OrderBookActor.GetOrderBookRequest
import play.api.libs.json.{JsValue, Json}
import scorex.transaction.assets.exchange.{AssetPair, Order}
import scorex.utils.ScorexLogging

object MatcherActor {
  def name = "matcher"
  def props(): Props = Props(new MatcherActor())

  sealed trait OrderResponse {
    val json: JsValue
  }
  case class OrderAccepted(order: Order) extends OrderResponse {
    val json = order.json
  }
  case object OrderCanceled extends OrderResponse {
    val json = Json.toJson("Order Canceled")
  }
}

class MatcherActor extends Actor with ScorexLogging {
  var bids: Map[AssetPair, OrderBook] = Map()
  var asks: Map[AssetPair, OrderBook] = Map()

  private def buy(order: Order) {
    val (executedOrders, remaining) = asks(order.assetPair).execute(order)

    if (executedOrders.nonEmpty) {
      log.info("Buy Executed: {}", executedOrders)
    }

    if (remaining > 0) {
      bids(order.assetPair).add(order.copy(amount = remaining))
    }
  }

  private def sell(order: Order) {
    val (executedOrders, remaining) = bids(order.assetPair).execute(order)

    if (executedOrders.nonEmpty) {
      log.info("Sell Executed: {}", executedOrders)
    }

    if (remaining > 0) {
      asks(order.assetPair).add(order.copy(amount = remaining))
    }
  }

  def getBidOrders(assetPair: AssetPair): Seq[Order]  = {
    bids(assetPair).flattenOrders
  }

  def getAskOrders(assetPair: AssetPair): Seq[Order]  = {
    asks(assetPair).flattenOrders
  }

  def place(order: Order): OrderResponse = ???

  /*def receive: Receive = {
    case order @ Order(clientId, OrderType.BUY, _, _, _) =>
      log.info("Market - received Buy message: {}", order)
      buy(order)
      sender() ! OrderCreated(clientId)
    case order @ Order(clientId, OrderType.SELL, _, _, _) =>
      log.info("Market - received Sell message: {}", order)
      sell(order)
      sender() ! OrderCreated(clientId)
  }*/

  def createOrderBook(pair: AssetPair) =
    context.actorOf(OrderBookActor.props(pair), OrderBookActor.name(pair))

  def createAndForward(pair: AssetPair, req: Any) = {
    val orderBook = createOrderBook(pair)
    forwardReq(req)(orderBook)
  }

  def forwardReq(req: Any)(orderBook: ActorRef) = orderBook forward req

  def forwardToOrderBook: Receive = {
    case order: Order =>
      context.child(OrderBookActor.name(order.assetPair))
        .fold(createAndForward(order.assetPair, order))(forwardReq(order))
    case ob: GetOrderBookRequest =>
      context.child(OrderBookActor.name(ob.pair))
        .fold(createAndForward(ob.pair, ob))(forwardReq(ob))
  }

  override def receive: Receive = forwardToOrderBook
}

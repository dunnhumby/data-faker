package com.dunnhumby

import java.sql.Date
import java.time.{LocalDateTime, ZoneId}

import scala.util.Random

package object datafaker {
  case class Item(card_id: Long, prod_id: Int, store_id: Int, item_qty: Int, item_discount_amt: BigDecimal, spend_amt: BigDecimal, date_id: Date, net_spend_amt: BigDecimal)
  object Item {
    def random(card_id: Long): Item = {
      val item_discount_amt = BigDecimal(Random.nextInt())
      import java.util.Calendar
      val calendar = Calendar.getInstance
      calendar.set(Calendar.YEAR, 2018)
      calendar.set(Calendar.DAY_OF_YEAR, Random.nextInt(90))
      val spend_amount = BigDecimal(Random.nextInt() * 100)
      Item(
        card_id,
        Random.nextInt(1000),
        Random.nextInt(100),
        Random.nextInt(10),
        BigDecimal(Random.nextDouble()),
        BigDecimal(Random.nextInt(100)),
        new Date(calendar.getTimeInMillis),
        spend_amount - item_discount_amt
      )
    }
  }

  case class CardDim(card_id: Long, card_code: String, hshd_id: Int, hshd_code: String, prsn_id: Int, prsn_code: String, hshd_isba_market_code: String)
  object CardDim {
    def random(numRecords: Int, card_id: Long): CardDim = {
      val hashId = Random.nextInt(numRecords)
      val prsnId = Random.nextInt(numRecords)
      val hashCode = trimRight(s"0000000000$hashId")
      CardDim(
        card_id,
        trimRight(s"0000000000$card_id"),
        hashId,
        hashCode,
        prsnId,
        trimRight(s"0000000000$prsnId"),
        s"isba$hashCode"
      )
    }
  }

  def trimRight(str: String): String ={
    str.substring(str.length - 10, str.length)
  }
}

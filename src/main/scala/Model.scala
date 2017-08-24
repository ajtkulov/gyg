package model

import cats.Monoid

/**
  * Ad Model
  *
  * @param keyword     keyword
  * @param impressions impressions
  * @param ctr         ctr
  * @param cost        cost
  * @param position    position
  * @param company     position
  * @param revenue     revenue
  */
case class Model(keyword: String, impressions: Double, ctr: Double, cost: Double, position: Int, company: String, revenue: Double) {
  def toSubModel: KeywordModel = KeywordModel(keyword)

  def toSubModelPair: KeyChange = KeyChange(keyword, company)

  def toAverage: AverageAccumulator = AverageAccumulator(impressions, ctr, cost, revenue, 1)
}

/**
  * Submodel for common keywords
  *
  * @param keyword keyword
  */
case class KeywordModel(keyword: String) {
  def prettyPrint: String = keyword
}

/**
  * Submodel for reaching changes
  *
  * @param keyword keyword
  * @param company company
  */
case class KeyChange(keyword: String, company: String) {
  def prettyPrint: String = s"$keyword,$company"
}

/**
  * Difference for key model
  *
  * @param key   key
  * @param db    source model
  * @param input input model
  */
case class Diff(key: KeyChange, db: Set[Model], input: Set[Model]) {
  def prettyString = s"{${key.prettyPrint}}: db: ${db.mkString("[", ", ", "]")}, input: ${input.mkString("[", ", ", "]")}"
}

/**
  * Accumulator
  *
  * @param impression impression
  * @param ctr        ctr
  * @param cost       cost
  * @param revenue    revenue
  * @param count      count
  */
case class AverageAccumulator(impression: Double, ctr: Double, cost: Double, revenue: Double, count: Int) {
  def toAverage = Average(impression, ctr / count, cost, revenue)
}

/**
  * Average
  *
  * @param impression impression
  * @param ctr        ctr
  * @param cost       cost
  * @param revenue    revenue
  */
case class Average(impression: Double, ctr: Double, cost: Double, revenue: Double)

/**
  * Monoid for average accumulator
  */
object AverageAccumulatorMonoid {
  implicit val averageMonoid = new Monoid[AverageAccumulator] {
    override def empty: AverageAccumulator = AverageAccumulator(0, 0, 0, 0, 1)

    override def combine(fst: AverageAccumulator, snd: AverageAccumulator): AverageAccumulator =
      AverageAccumulator(fst.impression + snd.impression, fst.ctr + snd.ctr, fst.cost + snd.cost, fst.revenue + snd.revenue, fst.count + snd.count)
  }
}

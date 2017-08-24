package main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.SparkUtils
import cats.implicits._
import cats.Monoid
import model._
import AverageAccumulatorMonoid._
import Calculator._

/**
  * Calculations
  */
object Calculator {
  lazy val header = "Search keyword,Impressions,CTR,Cost,Position,Company,Revenue"

  type Company = String

  def readModel(value: String): Model = {
    val split = value.split(",")
    Model(split(0), split(1).toDouble, split(2).toDouble, split(3).toDouble, split(4).toInt, split(5), split(6).toDouble)
  }

  def readFile(fileName: String)(implicit sc: SparkContext): RDD[Model] = {
    sc.textFile(fileName).filter(_ != header).map(readModel)
  }

  def stats(values: RDD[Model]): Map[Company, Average] = {
    values.groupBy(_.company).mapValues(grouped => {
      grouped.map(_.toAverage).reduce[AverageAccumulator] { case (a: AverageAccumulator, b: AverageAccumulator) => a |+| b }.toAverage
    }).collect().toMap

  }

  def changes(db: RDD[Model], input: RDD[Model]): RDD[Diff] = {
    val inputSubModelPair = input.map(x => x.toSubModelPair -> x)
    val dbSubModelPair = db.map(x => x.toSubModelPair -> x)

    val inputGrouped: RDD[(KeyChange, Iterable[(KeyChange, Model)])] = inputSubModelPair.groupBy(_._1)
    val dbGrouped: RDD[(KeyChange, Iterable[(KeyChange, Model)])] = dbSubModelPair.groupBy(_._1)

    val join: RDD[(KeyChange, (Iterable[(KeyChange, Model)], Iterable[(KeyChange, Model)]))] = inputGrouped.join(dbGrouped)

    join.flatMap {
      case (key, (inputCollection, dbCollection)) =>
        val inputSet = inputCollection.map(_._2).toSet
        val dbSet = dbCollection.map(_._2).toSet

        if (inputSet.intersect(dbSet).isEmpty) {
          Some(Diff(key, dbSet, inputSet))
        } else {
          None
        }
    }
  }
}

object Main extends App {
  override def main(args: Array[String]): Unit = {

    val (inputStats, dbStats) = SparkUtils.withSpark { implicit sc =>
      val db = readFile("db.csv")
      val input = readFile("input.csv")

      val inputSubModel = input.map(_.toSubModel).distinct()
      val dbSubModel = db.map(_.toSubModel).distinct()
      inputSubModel.subtract(dbSubModel).map(_.prettyPrint).saveAsTextFile("newKeywords")
      dbSubModel.subtract(inputSubModel).map(_.prettyPrint).saveAsTextFile("deletedKeywords")

      changes(db, input).map(_.prettyString).saveAsTextFile("changes")
      (stats(input), stats(db))
    }

    println(s"inputStats: ${inputStats}")
    println(s"dbStats: ${dbStats}")
  }
}

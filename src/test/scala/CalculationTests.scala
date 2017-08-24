import main.Calculator
import model.{Average, Diff, KeyChange, Model}
import org.scalatest.FunSuite
import spark.SparkUtils

class CalculationTests extends FunSuite {
  test("stats") {
    SparkUtils.withSpark { sc =>
      val input = List(
        Model("a", 1, 2, 3, 4, "company1", 5),
        Model("b", 2, 3, 4, 5, "company1", 6),
        Model("c", 1, 1, 1, 2, "company2", 7)

      )
      val output = Calculator.stats(sc.makeRDD(input))
      val outputStandard = Map("company1" -> Average(3.0, 2.5, 7.0, 11.0), "company2" -> Average(1.0, 1.0, 1.0, 7.0))
      assert(output == outputStandard)
    }
  }

  test("changes") {
    SparkUtils.withSpark { sc =>
      val dbRaw = List(
        Model("a", 2, 2, 3, 4, "company1", 5),
        Model("b", 2, 3, 4, 5, "company1", 6),
        Model("c", 1, 1, 1, 2, "company2", 7)
      )

      val inputRaw = List(
        Model("a", 1, 2, 3, 4, "company1", 5),
        Model("b", 2, 3, 4, 5, "company1", 6),
        Model("c", 1, 1, 1, 2, "company2", 7)
      )

      val output = Calculator.changes(sc.makeRDD(dbRaw), sc.makeRDD(inputRaw)).collect().toList

      assert(output == List(Diff(KeyChange("a", "company1"), Set(Model("a", 2.0, 2.0, 3.0, 4, "company1", 5.0)), Set(Model("a", 1.0, 2.0, 3.0, 4, "company1", 5.0)))))
    }
  }
}

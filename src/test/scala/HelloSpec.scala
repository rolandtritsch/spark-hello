package org.tritsch.spark.hello

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec

class HelloSpec extends FlatSpec with BeforeAndAfterAll {
  import Hello._

  "buildWordSizeHistogram(String)" should "return a/the histogram of word sizes" in {
    val histo = buildWordSizeHistogram("1 12 12 123 123 123 ")
    histo.show()
    assert(histo.count() === 3)
  }
}
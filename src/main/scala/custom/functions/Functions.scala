package custom

import org.apache.spark.sql.functions.{regexp_replace, split, upper, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, SparkSession}

object Functions {

  /**
   * Converts a column containing dollars to euros, considering a rate of 1$ = 0.9€
   *
   * Example:
   * convertToEur("$13") = 11,7
   * convertToEur("25$") = 22,5
   * convertToEur("70") = 63
   *
   * @param price column containing dollar information
   * @return price in errors
   */
  def convertToEur(price: Column): Column = {

    val priceInNumber = regexp_replace(price, "\\$", "").cast(DoubleType)

    when(priceInNumber.isNotNull, priceInNumber.*(0.9))
      .otherwise(price)
  }

  def mapStringToArray(string: Column, delimiter: String): Column = {

    split(string, delimiter)
  }

  /**
   * Converts a String containing either K (kilobytes) or M (megabytes) to bytes. If no K or M is found return "NaN".
   *
   * Example:
   * convertStringToBytes("3", "M") = 3000000
   * convertStringToBytes("13", "K") = 13000
   * convertStringToBytes("9", "L") = NaN
   *
   * @param number column containing the number of either kilobytes or megabytes
   * @param unit column containing the unit, either K or M
   * @return number representing bytes or NaN
   */
  def convertStringToBytes(number: Column, unit: Column): Column = {

    val doubleCasted = number.cast(DoubleType)
    val upperUnit = upper(unit)
    when(upperUnit.equalTo("M"), doubleCasted.*(1000000))
      .when(upperUnit.equalTo("K"), doubleCasted.*(1000))
      .otherwise("NaN")
  }
}

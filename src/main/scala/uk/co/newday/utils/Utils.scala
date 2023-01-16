package uk.co.newday.utils

import org.apache.spark.sql.DataFrame

/**
 * This utility object is just for reference for validating different types of fields, the actual validation
 * will be based on business requirements
 */
object Utils {

  //Some validations for reference
  val PHONE_NUMBER = "phone_number"
  val POSTAL_CODE = "postal_code"
  val EMAIL = "email"
  val NRIC = "nric"
  val FIN = "fin"

  /**
   * This is the fucntion to validate different fields
   * @param result - the dataframe which need to be validated
   * @param column - this is the column need to be validated
   * @param validateMethod - this need to mentioned - phone_number, postal_code, etc
   * @return - validated dataframe
   */
  def validate(result: DataFrame, column: String, validateMethod: String): (DataFrame) = {

    validateMethod match {
      case PHONE_NUMBER => "Do phone number validation as per business requirement"
      case POSTAL_CODE => "Do postal code validation as per business requirement"
      case EMAIL => "Do email validation as per business requirement"
      case NRIC => "Do nric validation as per business requirement"
      case FIN => "Do fin validation as per business requirement"
      case _ => "other validations"
    }
    result
  }

}

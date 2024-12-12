

case class WeatherReport(
    temperature: Double,
    visibility: Double,
    fog: Boolean,
    rain: Boolean,
    snow: Boolean,
    hail: Boolean,
    thunder: Boolean,
    tornado: Boolean) {
  def clear = !fog && !rain && !snow && !hail && !thunder && !tornado
  def visibility_clear = visibility > 30
  def visibility_moderate: Boolean = visibility <= 30 && visibility > 10
  def visibility_low: Boolean = visibility <= 10 && visibility > 2
  def visibility_poor: Boolean = visibility <= 2
  def temp_very_cold: Boolean = temperature < 32
  def temp_cold: Boolean = temperature < 50 && temperature >=32
  def temp_chilly: Boolean = temperature < 65 && temperature >= 50
  def temp_warm: Boolean = temperature < 75 && temperature >= 65
  def temp_hot: Boolean = temperature >= 75

}
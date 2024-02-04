class Counter {
  private var privateValue = 0  // Make it a private field and modify the field name
  def value = privateValue      // Define a method with the desired field name
  def value_=(newValue: Int): Unit = {
    if (newValue > 0) privateValue = newValue  // Only allow modification if the new value is positive
  }
  def increment(step: Int): Unit = { privateValue += step }  // Corrected line
  def current(): Int = { privateValue }
}

object MyCounter {
  def main(args: Array[String]): Unit = {
    val myCounter = new Counter
    println(myCounter.value)  // Print the initial value
    myCounter.value = 3        // Set a new value for 'value'
    println(myCounter.value)  // Print the new value
    myCounter.increment(1)     // Increment by 1
    println(myCounter.current) // Print the current value
  }
}

abstract class AppObject {
  val name: String = null
  def initial(): Unit;
  def execute(): Unit;
  def console(): Unit;
}


object CompanionClassObject extends AppObject {

  override val name = "CompanionClass"
  
  override def initial() {
    println("nothing")
  }
  
  override def execute() {
    println("nothing")
  }

  override def console(): Unit = {
    println("+++++++++++class CompanionClassObject++++++++++++++")
  }

}


object Main {
  
  private def console(): Unit = {
    println("+++++++++++object CompanionClassObject++++++++++++++")
  }

  def main(args: Array[String]): Unit = {
    /* val c = new CompanionClassObject()*/
    val cons = Class.forName("CompanionClassObject" /*+ "$"*/).getDeclaredConstructors();
    cons(0).setAccessible(true);
    //initialize implementation class to kick off execution
    val c = cons(0).newInstance().asInstanceOf[AppObject]
    println(c.name)
    c.console()
  }

}

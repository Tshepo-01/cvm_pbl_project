package cvm_pbl

object Arguments {
  def isArgsValid(args: Array[String]): Boolean = {
    if (args.length < 8) false
    else true
  }

}

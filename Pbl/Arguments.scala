package cvm_pbl

object Arguments {
  def isArgsValid(args: Array[String]): Boolean = {
    if (args.length < 4l) false
    else true
  }

}

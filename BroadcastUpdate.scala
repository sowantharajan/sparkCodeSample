**
  * This wrapper lets us update brodcast variables within DStreams' foreachRDD
  * without running into serialization issues
  * @param ssc
  * @param _v
  */
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val ssc: StreamingContext,
                                          @transient private val _v: T) {

  @transient private var v = ssc.sparkContext.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
}

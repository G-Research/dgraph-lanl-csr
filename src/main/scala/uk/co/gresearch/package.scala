package uk.co

package object gresearch {

  trait WhenTransformation[T] {
    /**
     * Executes the given transformation.
     *
     * @param transformation transformation
     * @return transformation result
     */
    def call(transformation: T => T): T
  }

  case class ThenTransformation[T](t: T) extends WhenTransformation[T] {
    override def call(transformation: T => T): T = t.call(transformation)
  }

  case class OtherwiseTransformation[T](t: T) extends WhenTransformation[T] {
    override def call(transformation: T => T): T = t
  }

  implicit class ExtendedTransformation[T](t: T) {

    /**
     * Executes the given transformation on the decorated instance.
     *
     * This allows writing fluent code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .call(transformation)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * transformation(
     *   i.doThis()
     *    .doThat()
     * ).doMore()
     * }}}
     *
     * where the effective sequence of operations is not clear.
     *
     * @param transformation transformation
     * @return the transformation result
     */
    def call[R](transformation: T => R): R = transformation(t)

    /**
     * Allows to perform a transformation fluently only if the given condition is true:
     *
     * {{{
     *   a.when(true).call(_.action())
     * }}}
     *
     * This allows to write elegant code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .when(condition).call(transformation)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * val intermediate1 =
     *   i.doThis()
     *    .doThat()
     * val intermediate2 =
     *   if (condition) transformation(intermediate1) else indermediate1
     * intermediate2.doMore()
     * }}}
     *
     * @param condition condition
     * @return WhenTransformation
     */
    def when(condition: Boolean): WhenTransformation[T] =
      if (condition) ThenTransformation(t) else OtherwiseTransformation(t)

  }

}

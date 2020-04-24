package org.apache.spark.ml.optimization

import org.apache.spark.mllib.linalg.Vector

/**
  * :: DeveloperApi ::
  * Class used to perform steps (weight update) with per-coordinate learning rate.
  *
  */
abstract class PerCoordinateUpdater extends Serializable {
  def compute(
      weightsOld: Vector,
      gradient: Vector,
      alpha: Double,
      beta: Double,
      l1: Double,
      l2: Double,
      n: Vector,
      z: Vector): (Vector, Double, Vector, Vector)
}

package com.nikvanderhoof.spark.sql

package object constraints {
  private[constraints] def raiseInternalError = throw new java.lang.IllegalStateException(
    "Internal Error: Datasets should be initialized with" ++
      " an empty constraint store. If this error persists," ++
      " contact the maintainer of spark-constraints."
  )
}

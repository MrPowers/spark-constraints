package com.nikvanderhoof.spark.sql.constraints

import org.apache.spark.sql.{Column, Dataset, DataFrame}


object syntax {
  private val CM = ConstraintMetastore

  /** Wraps a {@code Dataset[T]} to provide methods for altering constraints. */
  implicit class DatasetExt[T](dataset: Dataset[T]) {
    /** Adds a constraint to dataset.
     *
     *  @param name a name to give the constraint
     *  @param f a function that creates a constraint given a dataset
     */
    def addConstraint(name: String, f: Dataset[T] => Constraint[T]): Dataset[T] = {
      val constraint = f(dataset)
      CM.add(name, constraint)
      dataset
    }

    /** Removes a constraint from a dataset.
     *
     *  If no constraint named {@code name} exists, this is a no-op
     *  @param name the constraint to remove
     */
    def dropConstraint(name: String): Dataset[T] = {
      CM.drop(dataset, name)
      dataset
    }

    /** Removes all constraints from a dataset. */
    def dropAllConstraints: Dataset[T] = {
      constraints.foreach { case (name, _) => CM.drop(dataset, name) }
      dataset
    }

    /** Lists the constraints currently applied to this dataset. */
    def constraints: Seq[(String, Constraint[T])] = CM.list(dataset)

    /** Returns all the constraints who are violated. */
    def violations: Map[String, Constraint[T]] = CM.violations(dataset)
  }

  def check[T](condition: Column)(dataset: Dataset[T]): Check[T] =
    Check(dataset, condition)

  def notNull[T](columns: Column *)(dataset: Dataset[T]): NotNull[T] =
    NotNull(dataset, columns)

  def unique[T](columns: Column *)(dataset: Dataset[T]): Unique[T] =
    Unique(dataset, columns)

  def primaryKey[T](columns: Column *)(dataset: Dataset[T]): PrimaryKey[T] =
    PrimaryKey(dataset, columns)

  case class foreignKey[T](columns: Column *) {
    case class references[U](refDataset: Dataset[U]) {
      def at(refColumns: Column *)(dataset: Dataset[T]): ForeignKey[T, U] =
        ForeignKey(dataset, columns, refDataset, refColumns)
    }
  }
}

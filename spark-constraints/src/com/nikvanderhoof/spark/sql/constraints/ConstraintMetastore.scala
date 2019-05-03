package com.nikvanderhoof.spark.sql.constraints

import org.apache.spark.sql.Dataset
import scala.collection.{mutable => M}
import java.util.UUID.randomUUID

/**
  * Tracks all existing constraints on a dataset.
  */
private object ConstraintMetastore {
  private type ConstraintMap = M.HashMap[String, Constraint[_]]
  private val cache = new M.HashMap[Dataset[_], ConstraintMap] {
    // Automatically create a new constraint map for each dataset
    override def default(key: Dataset[_]) = {
      val value = M.HashMap.empty[String, Constraint[_]]
      put(key, value)
      value
    }
  }
  private def ensureSinglePrimaryKey(constraints: ConstraintMap,
                                     constraint: Constraint[_]) =
    constraint match {
      case pk: PrimaryKey[_] => {
        val pks = constraints.collect {
          case (n, c: PrimaryKey[_]) => (n, c)
        }
        require(pks.isEmpty,
                "A primary key is already defined on that dataset.")
      }
      case _ => {}
    }
  private def ensureNoDuplicateNames(constraints: ConstraintMap, name: String) =
    require(constraints.get(name).isEmpty,
            s"A constraint named: ${name} already exists on that dataset.")

  def list[T](dataset: Dataset[T]): Seq[(String, Constraint[T])]
    = cache(dataset).toSeq.map(_.asInstanceOf[(String, Constraint[T])])

  def add[T](name: String, constraint: Constraint[T]): Unit = {
    val ds = constraint.dataset
    val dsConstraints = cache(ds)
    ensureNoDuplicateNames(dsConstraints, name)
    ensureSinglePrimaryKey(dsConstraints, constraint)
    dsConstraints += (name -> constraint)
  }

  def add[T](constraint: Constraint[T]): Unit = {
    val prefix = constraint match {
      case _: PrimaryKey[_] => "PK"
      case _: ForeignKey[_, _] => "FK"
      case _: Check[_] => "CK"
      case _: Unique[_] => "UK"
      case _: NotNull[_] => "NN"
    }
    add(s"${prefix}_${randomUUID}", constraint)
  }

  def drop[T](dataset: Dataset[T], name: String): Unit = {
    val dsConstraints = cache(dataset)
    dsConstraints -= name
  }

  def violations[T](dataset: Dataset[T]): Map[String, Constraint[T]] =
    cache(dataset).toMap.collect {
      case (name, const) if const.isViolated =>
        (name, const.asInstanceOf[Constraint[T]])
    }
}

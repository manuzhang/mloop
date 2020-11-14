package io.github.manuzhang.mlp.dag.model;

import org.apache.spark.ml.param.Params;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ParamsAnnotation {
  Class<? extends Params> paramsType();

  String name();

  String id();

  String desc();
}

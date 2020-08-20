// Databricks notebook source
// DBTITLE 1,Esquema del Data General
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val myDataSchema = StructType(
  Array(
    StructField("id", DecimalType(26, 0), true),
    StructField("anio", IntegerType, true),
    StructField("mes", IntegerType, true),
    StructField("provincia", IntegerType, true),
    StructField("canton", IntegerType, true),
    StructField("area", StringType, true),
    StructField("genero", StringType, true),
    StructField("edad", IntegerType, true),
    StructField("estado_civil", StringType, true),
    StructField("nivel_de_instruccion", StringType, true),
    StructField("etnia", StringType, true),
    StructField("ingreso_laboral", IntegerType, true),
    StructField("condicion_actividad", StringType, true),
    StructField("sectorizacion", StringType, true),
    StructField("grupo_ocupacion", StringType, true),
    StructField("rama_actividad", StringType, true),
    StructField("factor_expansion", DoubleType, true)
  )
);

// COMMAND ----------

// DBTITLE 1,Carga de cada uno de los CSV a utilizar
val data = spark
  .read
  .schema(myDataSchema)
  .option("header","true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")
val dataProv = spark
    .read
    .option("delimiter", ";")
    .option("header","true")
    .option("inferSchema", "true")
    .csv("/FileStore/tables/cvsProvincias.csv")
val dataCant = spark
    .read
    .option("delimiter", ",")
    .option("header","true")
    .option("inferSchema", "true")
    .csv("/FileStore/tables/Cantones.csv")

// COMMAND ----------

// DBTITLE 1,Creación del DataFrame final 
val innerProvince = data.join(dataProv, "provincia")
val dataProvCantones = innerProvince.join(dataCant, innerProvince("canton") === dataCant("codigoCanton"))
val dataProvCant = dataProvCantones.drop("provincia", "canton", "codigoCanton")

// COMMAND ----------

// DBTITLE 1,Distribución del ingreso laboral promedio según las edades
display(dataProvCant.groupBy("edad").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").sort("edad"))

// COMMAND ----------

// DBTITLE 1,Extracción de un DataFrame con los Outliers superiores
dataProvCant.select("ingreso_laboral").summary().show()
val dfSueldo = dataProvCant.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val avgS = dfSueldo.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stdDesvS = dfSueldo.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]
val inferiorS = avgS - 3 * stdDesvS
val superiorS = avgS + 3 * stdDesvS

val dataOutliers = dataProvCant.where(($"ingreso_laboral" > superiorS))

// COMMAND ----------

// DBTITLE 1,Distribución del ingreso laboral promedio según las edades de los datos Outliers basados en el Ingreso Laboral​  ​
display(dataOutliers.groupBy("edad").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").sort("edad"))

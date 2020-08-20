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

// DBTITLE 1,Distribución de los datos según el año
display(dataProvCant.groupBy("anio").count().sort(desc("anio")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según los Nombres de Cantón
display(dataProvCant.groupBy("nomCanton").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según el Nivel de Instrucción
display(dataProvCant.groupBy("nivel_de_instruccion").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según el Estado Civil
display(dataProvCant.groupBy("estado_civil").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según la Condición Actividad
display(dataProvCant.groupBy("condicion_actividad").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según el Grupo de Ocupación
display(dataProvCant.groupBy("grupo_ocupacion").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos según la Rama de Actividad
display(dataProvCant.groupBy("rama_actividad").count().sort(desc("count")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos en cada Área según cada año​
display(dataProvCant.groupBy("anio").pivot("area").count().sort(desc("anio")))

// COMMAND ----------

// DBTITLE 1,Distribución de los datos en cada nivel de instrucción según cada año​
display(dataProvCant.groupBy("anio").pivot("nivel_de_instruccion").count())

// COMMAND ----------

// DBTITLE 1,Distribución de los datos en cada Etnia según cada año​
display(dataProvCant.groupBy("anio").pivot("etnia").count())

// COMMAND ----------

// DBTITLE 1,Número de hombres y mujeres que tienen un nivel de instrucción de Secundaria según cada año​
display(dataProvCant.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy("anio").pivot("genero").count())

// COMMAND ----------

// DBTITLE 1,Frecuencia de datos de hombres de cada estado civil según la sectorización ​
display(dataProvCant.filter($"genero" === "1 - Hombre").groupBy("estado_civil").pivot("sectorizacion").count())

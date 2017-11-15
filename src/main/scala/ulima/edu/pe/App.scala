package ulima.edu.pe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
/**
 * @author ${user.name}
 */
 object App {

   def getFile() : RDD[String] = {
     return MySparkContext.getSparkContext().textFile("data/FullData.csv")
   }

   def getSelectablePlayersRDD() : RDD[Array[String]] = {
     return getFile().map( x => x.split(",") )
   }

   def main(args : Array[String]) {

     var arraybuffer1 = ArrayBuffer[String]()

     // 0 : defensivo
     // 1 : ofensivo

     //Defensas
     arraybuffer1.++=(getPositionStream("0", "CB").collect()) //Back central
     arraybuffer1.++=(getPositionStream("0", "LCB").collect()) //Defensa izquierdo
     arraybuffer1.++=(getPositionStream("0", "RCB").collect()) //Defensa derecho

     arraybuffer1.++=(getPositionStream("0", "LB").collect()) //Central izquierdo
     arraybuffer1.++=(getPositionStream("0", "RB").collect()) //Central derecho

     arraybuffer1.++=(getPositionStream("0", "LWB").collect()) //Lateral izquierdo
     arraybuffer1.++=(getPositionStream("0", "RWB").collect()) //Lateral derecho

     //Mediocampistas
     arraybuffer1.++=(getPositionStream("0", "CDM").collect()) //Mediocampista central defensivo
     arraybuffer1.++=(getPositionStream("0", "CMS").collect()) //Mediocampista central
     arraybuffer1.++=(getPositionStream("1", "CAM").collect()) //Mediocampista central de ataque

     arraybuffer1.++=(getPositionStream("1", "LAM").collect()) //Mediocampista izquierdo de ataque
     arraybuffer1.++=(getPositionStream("1", "RAM").collect()) //Mediocampista derecho de ataque

     arraybuffer1.++=(getPositionStream("1", "LCM").collect()) //Mediocampista central izquierdo
     arraybuffer1.++=(getPositionStream("1", "RCM").collect()) //Mediocampista central derecho

     arraybuffer1.++=(getPositionStream("0", "LDM").collect()) //Mediocampista defensivo izquierdo
     arraybuffer1.++=(getPositionStream("0", "RDM").collect()) //Mediocampista defensivo derecho

     arraybuffer1.++=(getPositionStream("1", "LM").collect()) //Mediocampista izquierdo
     arraybuffer1.++=(getPositionStream("1", "RM").collect()) //Mediocampista derecho

     //Delanteros
     arraybuffer1.++=(getPositionStream("1", "CF").collect()) //Centrodelantero

     arraybuffer1.++=(getPositionStream("1", "LF").collect()) //Delantero izquierdo - alero
     arraybuffer1.++=(getPositionStream("1", "RF").collect()) //Delantero derecho - alero

     arraybuffer1.++=(getPositionStream("1", "LS").collect()) //Delantero izquierdo
     arraybuffer1.++=(getPositionStream("1", "RS").collect()) //Delantero derecho

     arraybuffer1.++=(getPositionStream("1", "LWS").collect()) //Wing izquierdo
     arraybuffer1.++=(getPositionStream("1", "RWS").collect()) //Wing derecho

     arraybuffer1.++=(getPositionStream("1", "ST").collect()) //Delantero

     arraybuffer1 = Random.shuffle(arraybuffer1)

     MySparkContext.getSparkContext().parallelize(arraybuffer1)
     .saveAsTextFile("data/resultadoSinRating1/")

     var arraybuffer2 = ArrayBuffer[String]()

     // 0 : Back central
     // 1 : Defensa izquierdo
     // 2 : Defensa derecho

     // 3 : Central izquierdo
     // 4 : Central derecho

     // 5 : Lateral izquierdo
     // 6 : Lateral derecho

     // 7 : Mediocampista central defensivo
     // 8 : Mediocampista central
     // 9 : Mediocampista central de ataque

     // 10 : Mediocampista izquierdo de ataque
     // 11 : Mediocampista derecho de ataque

     // 12 : Mediocampista central izquierdo
     // 13 : Mediocampista central derecho

     // 14 : Mediocampista defensivo izquierdo
     // 15 : Mediocampista defensivo derecho

     // 16 : Mediocampista izquierdo
     // 17 : Mediocampista derecho

     // 18 : Centrodelantero

     // 19 : Delantero izquierdo - alero
     // 20 : Delantero derecho - alero

     // 21 : Delantero izquierdo
     // 22 : Delantero derecho

     // 23 : Wing izquierdo
     // 24 : Wing derecho

     // 25 : Delantero

     //Defensas
     arraybuffer2.++=(getPositionStream("0", "CB").collect()) //Back central
     arraybuffer2.++=(getPositionStream("1", "LCB").collect()) //Defensa izquierdo
     arraybuffer2.++=(getPositionStream("2", "RCB").collect()) //Defensa derecho

     arraybuffer2.++=(getPositionStream("3", "LB").collect()) //Central izquierdo
     arraybuffer2.++=(getPositionStream("4", "RB").collect()) //Central derecho

     arraybuffer2.++=(getPositionStream("5", "LWB").collect()) //Lateral izquierdo
     arraybuffer2.++=(getPositionStream("6", "RWB").collect()) //Lateral derecho

     //Mediocampistas
     arraybuffer2.++=(getPositionStream("7", "CDM").collect()) //Mediocampista central defensivo
     arraybuffer2.++=(getPositionStream("8", "CMS").collect()) //Mediocampista central
     arraybuffer2.++=(getPositionStream("9", "CAM").collect()) //Mediocampista central de ataque

     arraybuffer2.++=(getPositionStream("10", "LAM").collect()) //Mediocampista izquierdo de ataque
     arraybuffer2.++=(getPositionStream("11", "RAM").collect()) //Mediocampista derecho de ataque

     arraybuffer2.++=(getPositionStream("12", "LCM").collect()) //Mediocampista central izquierdo
     arraybuffer2.++=(getPositionStream("13", "RCM").collect()) //Mediocampista central derecho

     arraybuffer2.++=(getPositionStream("14", "LDM").collect()) //Mediocampista defensivo izquierdo
     arraybuffer2.++=(getPositionStream("15", "RDM").collect()) //Mediocampista defensivo derecho

     arraybuffer2.++=(getPositionStream("16", "LM").collect()) //Mediocampista izquierdo
     arraybuffer2.++=(getPositionStream("17", "RM").collect()) //Mediocampista derecho

     //Delanteros
     arraybuffer2.++=(getPositionStream("18", "CF").collect()) //Centrodelantero

     arraybuffer2.++=(getPositionStream("19", "LF").collect()) //Delantero izquierdo - alero
     arraybuffer2.++=(getPositionStream("20", "RF").collect()) //Delantero derecho - alero

     arraybuffer2.++=(getPositionStream("21", "LS").collect()) //Delantero izquierdo
     arraybuffer2.++=(getPositionStream("22", "RS").collect()) //Delantero derecho

     arraybuffer2.++=(getPositionStream("23", "LWS").collect()) //Wing izquierdo
     arraybuffer2.++=(getPositionStream("24", "RWS").collect()) //Wing derecho

     arraybuffer2.++=(getPositionStream("25", "ST").collect()) //Delantero

     arraybuffer2 = Random.shuffle(arraybuffer2)

     MySparkContext.getSparkContext().parallelize(arraybuffer2)
     .saveAsTextFile("data/resultadoSinRating2/")
   }

   def getPositionStream(label : String, position : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains(position))
     .map( fields => fields(0) + ","
                       + label + "," + fields(17) + "," + fields(18) + ","
                       + fields(19) + "," + fields(20) + "," + fields(21) + ","
                       + fields(22) + "," + fields(23) + "," + fields(24) + ","
                       + fields(25) + "," + fields(26) + "," + fields(27) + ","
                       + fields(28) + "," + fields(29) + "," + fields(30) + ","
                       + fields(31) + "," + fields(32) + "," + fields(33) + ","
                       + fields(34) + "," + fields(35) + "," + fields(36) + ","
                       + fields(37) + "," + fields(38) + "," + fields(39) + ","
                       + fields(40) + "," + fields(41) + "," + fields(42) + ","
                       + fields(43) + "," + fields(44) + "," + fields(45) + ","
                       + fields(46) + "," + fields(47) //+ "," + fields(9)
                     )
   }

 }

 object MySparkContext {

   var sc : SparkContext = null

   def getSparkContext() : SparkContext = {
     if(this.sc == null) {
       var conf = new SparkConf().setAppName("bigdata-taller3-normalizer").setMaster("local")
       this.sc = new SparkContext(conf)
     }
     return this.sc
   }

 }

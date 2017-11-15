package ulima.edu.pe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
 * @author ${user.name}
 */
 object App {

   def getFile() : RDD[String] = {
     return MySparkContext.getSparkContext().textFile("data/FullData.csv")
   }

   def getFileHeader() : RDD[String] = {
     return MySparkContext.getSparkContext().textFile("data/headers.csv")
   }

   def getSelectablePlayersRDD() : RDD[Array[String]] = {
     return getFile().map( x => x.split(",") )
   }

   def getHeadersRDD() : RDD[Array[String]] = {
     return getFileHeader().map( x => x.split(",") )
   }

   def main(args : Array[String]) {

     var arraybuffer1 = ArrayBuffer[String]()
     arraybuffer1.++=(getHeadersStream().collect())

     // 0 : defensivo
     // 1 : ofensivo

     //Defensas
     arraybuffer1.++=(getCBStream("0").collect()) //Back central
     arraybuffer1.++=(getLCBStream("0").collect()) //Defensa izquierdo
     arraybuffer1.++=(getRCBStream("0").collect()) //Defensa derecho

     arraybuffer1.++=(getLBStream("0").collect()) //Central izquierdo
     arraybuffer1.++=(getRBStream("0").collect()) //Centras derecho

     arraybuffer1.++=(getLWBStream("0").collect()) //Lateral izquierdo
     arraybuffer1.++=(getRWBStream("0").collect()) //Lateral derecho

     //Mediocampistas
     arraybuffer1.++=(getCDMStream("0").collect()) //Mediocampista central defensivo
     arraybuffer1.++=(getCMStream("0").collect()) //Mediocampista central
     arraybuffer1.++=(getCAMStream("0").collect()) //Mediocampista central de ataque

     arraybuffer1.++=(getLAMStream("1").collect()) //Mediocampista izquierdo de ataque
     arraybuffer1.++=(getRAMStream("1").collect()) //Mediocampista derecho de ataque

     arraybuffer1.++=(getLCMStream("1").collect()) //Mediocampista central izquierdo
     arraybuffer1.++=(getRCMStream("1").collect()) //Mediocampista central derecho

     arraybuffer1.++=(getLDMStream("0").collect()) //Mediocampista defensivo izquierdo
     arraybuffer1.++=(getRDMStream("0").collect()) //Mediocampista defensivo derecho

     arraybuffer1.++=(getLMStream("1").collect())//Mediocampista izquierdo
     arraybuffer1.++=(getRMStream("1").collect()) //Mediocampista derecho

     //Delanteros
     arraybuffer1.++=(getCFStream("1").collect()) //Centrodelantero

     arraybuffer1.++=(getLFStream("1").collect()) //Delantero izquierdo - alero
     arraybuffer1.++=(getRFStream("1").collect()) //Delantero derecho - alero

     arraybuffer1.++=(getLSStream("1").collect()) //Delantero izquierdo
     arraybuffer1.++=(getRSStream("1").collect()) //Delantero derecho

     arraybuffer1.++=(getLWStream("1").collect()) //Wing izquierdo
     arraybuffer1.++=(getRWStream("1").collect()) //Wing derecho

     arraybuffer1.++=(getSTStream("1").collect()) //Delantero

     MySparkContext.getSparkContext().parallelize(arraybuffer1)
     .saveAsTextFile("data/resultado1/")

     var arraybuffer2 = ArrayBuffer[String]()
     arraybuffer2.++=(getHeadersStream().collect())

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
     arraybuffer2.++=(getCBStream("0").collect()) //Back central
     arraybuffer2.++=(getLCBStream("1").collect()) //Defensa izquierdo
     arraybuffer2.++=(getRCBStream("2").collect()) //Defensa derecho

     arraybuffer2.++=(getLBStream("3").collect()) //Central izquierdo
     arraybuffer2.++=(getRBStream("4").collect()) //Central derecho

     arraybuffer2.++=(getLWBStream("5").collect()) //Lateral izquierdo
     arraybuffer2.++=(getRWBStream("6").collect()) //Lateral derecho

     //Mediocampistas
     arraybuffer2.++=(getCDMStream("7").collect()) //Mediocampista central defensivo
     arraybuffer2.++=(getCMStream("8").collect()) //Mediocampista central
     arraybuffer2.++=(getCAMStream("9").collect()) //Mediocampista central de ataque

     arraybuffer2.++=(getLAMStream("10").collect()) //Mediocampista izquierdo de ataque
     arraybuffer1.++=(getRAMStream("11").collect()) //Mediocampista derecho de ataque

     arraybuffer2.++=(getLCMStream("12").collect()) //Mediocampista central izquierdo
     arraybuffer2.++=(getRCMStream("13").collect()) //Mediocampista central derecho

     arraybuffer2.++=(getLDMStream("14").collect()) //Mediocampista defensivo izquierdo
     arraybuffer2.++=(getRDMStream("15").collect()) //Mediocampista defensivo derecho

     arraybuffer2.++=(getLMStream("16").collect())//Mediocampista izquierdo
     arraybuffer2.++=(getRMStream("17").collect()) //Mediocampista derecho

     //Delanteros
     arraybuffer2.++=(getCFStream("18").collect()) //Centrodelantero

     arraybuffer2.++=(getLFStream("19").collect()) //Delantero izquierdo - alero
     arraybuffer2.++=(getRFStream("20").collect()) //Delantero derecho - alero

     arraybuffer2.++=(getLSStream("21").collect()) //Delantero izquierdo
     arraybuffer2.++=(getRSStream("22").collect()) //Delantero derecho

     arraybuffer2.++=(getLWStream("23").collect()) //Wing izquierdo
     arraybuffer2.++=(getRWStream("24").collect()) //Wing derecho

     arraybuffer2.++=(getSTStream("25").collect()) //Delantero

     MySparkContext.getSparkContext().parallelize(arraybuffer1)
     .saveAsTextFile("data/resultado2/")
   }

   def getHeadersStream() : RDD[String] = {
     getHeadersRDD()
     .map( fields => fields(0) + ","
                       + fields(15) + "," + fields(17) + "," + fields(18) + ","
                       + fields(19) + "," + fields(20) + "," + fields(21) + ","
                       + fields(22) + "," + fields(23) + "," + fields(24) + ","
                       + fields(25) + "," + fields(26) + "," + fields(27) + ","
                       + fields(28) + "," + fields(29) + "," + fields(30) + ","
                       + fields(31) + "," + fields(32) + "," + fields(33) + ","
                       + fields(34) + "," + fields(35) + "," + fields(36) + ","
                       + fields(37) + "," + fields(38) + "," + fields(39) + ","
                       + fields(40) + "," + fields(41) + "," + fields(42) + ","
                       + fields(43) + "," + fields(44) + "," + fields(45) + ","
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )

   }

   def getGKStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("GK"))
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
                       + fields(46) + "," + fields(47) + "," + fields(48) + ","
                       + fields(49) + "," + fields(50) + "," + fields(51) + ","
                       + fields(52) + "," + fields(9))
   }

   def getCBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLCBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LCB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRCBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RCB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLWBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LWB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRWBStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RWB"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getCDMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CDM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getCMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getCAMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CAM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLAMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LAM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRAMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RAM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLCMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LCM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRCMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RCM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLDMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LDM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRDMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RDM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRMStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RM"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getCFStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CF"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLFStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LF"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRFStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RF"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLSStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LS"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRSStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RS"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getLWStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LW"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getRWStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RW"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
                     )
   }

   def getSTStream(label : String) : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("ST"))
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
                       + fields(46) + "," + fields(47) //+ "," + fields(48) + ","
                       //+ fields(49) + "," + fields(50) + "," + fields(51) + ","
                       //+ fields(52)
                       + "," + fields(9)
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

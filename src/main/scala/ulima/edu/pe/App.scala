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

/*   //Arqueros
     //getGKs()

     //Defensas
     getCBs() //Back central
     getLCBs() //Defensa izquierdo
     getRCBs() //Defensa derecho

     getLBs() //Central izquierdo
     getRBs() //Centras derecho

     getLWBs() //Lateral izquierdo
     getRWBs() //Lateral derecho

     //Mediocampistas
     getCDMs() //Mediocampista central defensivo
     getCMs() //Mediocampista central
     getCAMs() //Mediocampista central de ataque

     getLAMs() //Mediocampista izquierdo de ataque
     getRAMs() //Mediocampista derecho de ataque

     getLCMs() //Mediocampista central izquierdo
     getRCMs() //Mediocampista central derecho

     getLDMs() //Mediocampista defensivo izquierdo
     getRDMs() //Mediocampista defensivo derecho

     getLMs() //Mediocampista izquierdo
     getRMs() //Mediocampista derecho

     //Delanteros
     getCFs() //Centrodelantero

     getLFs() //Delantero izquierdo - alero
     getRFs() //Delantero derecho - alero

     getLSs() //Delantero izquierdo
     getRSs() //Delantero derecho

     getLWs() //Wing izquierdo
     getRWs() //Wing derecho

     getSTs() //Delantero */

     val headers = Array()
     var arraybuffer = ArrayBuffer[String]()
     arraybuffer.++=(getHeadersStream().collect())

     //Arqueros
     //getGKs()

     //Defensas
     arraybuffer.++=(getCBStream().collect()) //Back central
     arraybuffer.++=(getLCBStream().collect()) //Defensa izquierdo
     arraybuffer.++=(getRCBStream().collect()) //Defensa derecho

     arraybuffer.++=(getLBStream().collect()) //Central izquierdo
     arraybuffer.++=(getRBStream().collect()) //Centras derecho

     arraybuffer.++=(getLWBStream().collect()) //Lateral izquierdo
     arraybuffer.++=(getRWBStream().collect()) //Lateral derecho

     //Mediocampistas
     arraybuffer.++=(getCDMStream().collect()) //Mediocampista central defensivo
     arraybuffer.++=(getCMStream().collect()) //Mediocampista central
     arraybuffer.++=(getCAMStream().collect()) //Mediocampista central de ataque

     arraybuffer.++=(getLAMStream().collect()) //Mediocampista izquierdo de ataque
     arraybuffer.++=(getRAMStream().collect()) //Mediocampista derecho de ataque

     arraybuffer.++=(getLCMStream().collect()) //Mediocampista central izquierdo
     arraybuffer.++=(getRCMStream().collect()) //Mediocampista central derecho

     arraybuffer.++=(getLDMStream().collect()) //Mediocampista defensivo izquierdo
     arraybuffer.++=(getRDMStream().collect()) //Mediocampista defensivo derecho

     arraybuffer.++=(getLMStream().collect())//Mediocampista izquierdo
     arraybuffer.++=(getRMStream().collect()) //Mediocampista derecho

     //Delanteros
     arraybuffer.++=(getCFStream().collect()) //Centrodelantero

     arraybuffer.++=(getLFStream().collect()) //Delantero izquierdo - alero
     arraybuffer.++=(getRFStream().collect()) //Delantero derecho - alero

     arraybuffer.++=(getLSStream().collect()) //Delantero izquierdo
     arraybuffer.++=(getRSStream().collect()) //Delantero derecho

     arraybuffer.++=(getLWStream().collect()) //Wing izquierdo
     arraybuffer.++=(getRWStream().collect()) //Wing derecho

     arraybuffer.++=(getSTStream().collect()) //Delantero

     MySparkContext.getSparkContext().parallelize(arraybuffer)
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

   def getGKs() {
     getGKStream().saveAsTextFile("data/resultadoGK/") //folder donde se guarda data/resultado##
   }

   def getGKStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("GK"))
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
                       + fields(46) + "," + fields(47) + "," + fields(48) + ","
                       + fields(49) + "," + fields(50) + "," + fields(51) + ","
                       + fields(52) + "," + fields(9))
   }

   def getCBs() {
     getCBStream().saveAsTextFile("data/resultadoCB/") //folder donde se guarda data/resultado##
   }

   def getCBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CB"))
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

   def getLCBs() {
     getLCBStream.saveAsTextFile("data/resultadoLCB/") //folder donde se guarda data/resultado##
   }

   def getLCBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LCB"))
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

   def getRCBs() {
     getRCBStream.saveAsTextFile("data/resultadoRCB/") //folder donde se guarda data/resultado##
   }

   def getRCBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RCB"))
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

   def getLBs() {
     getLBStream.saveAsTextFile("data/resultadoLB/") //folder donde se guarda data/resultado##
   }

   def getLBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LB"))
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

   def getRBs() {
     getRBStream.saveAsTextFile("data/resultadoRB/") //folder donde se guarda data/resultado##
   }

   def getRBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RB"))
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

   def getLWBs() {
     getLWBStream.saveAsTextFile("data/resultadoLWB/") //folder donde se guarda data/resultado##
   }

   def getLWBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LWB"))
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

   def getRWBs() {
     getRWBStream.saveAsTextFile("data/resultadoRWB/") //folder donde se guarda data/resultado##
   }

   def getRWBStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RWB"))
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

   def getCDMs() {
     getCDMStream().saveAsTextFile("data/resultadoCDM/") //folder donde se guarda data/resultado##
   }

   def getCDMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CDM"))
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

   def getCMs() {
     getCMStream.saveAsTextFile("data/resultadoCM/") //folder donde se guarda data/resultado##
   }

   def getCMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CM"))
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

   def getCAMs() {
     getCAMStream().saveAsTextFile("data/resultadoCAM/") //folder donde se guarda data/resultado##
   }

   def getCAMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CAM"))
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

   def getLAMs() {
     getLAMStream.saveAsTextFile("data/resultadoLAM/") //folder donde se guarda data/resultado##
   }

   def getLAMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LAM"))
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

   def getRAMs() {
     getRAMStream.saveAsTextFile("data/resultadoRAM/") //folder donde se guarda data/resultado##
   }

   def getRAMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RAM"))
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

   def getLCMs() {
     getLCMStream.saveAsTextFile("data/resultadoLCM/") //folder donde se guarda data/resultado##
   }

   def getLCMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LCM"))
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

   def getRCMs() {
     getRCMStream.saveAsTextFile("data/resultadoRCM/") //folder donde se guarda data/resultado##
   }

   def getRCMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RCM"))
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

   def getLDMs() {
     getLDMStream.saveAsTextFile("data/resultadoLDM/") //folder donde se guarda data/resultado##
   }

   def getLDMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LDM"))
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

   def getRDMs() {
     getRDMStream.saveAsTextFile("data/resultadoRDM/") //folder donde se guarda data/resultado##
   }

   def getRDMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RDM"))
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

   def getLMs() {
     getLMStream.saveAsTextFile("data/resultadoLM/") //folder donde se guarda data/resultado##
   }

   def getLMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LM"))
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

   def getRMs() {
     getRMStream.saveAsTextFile("data/resultadoRM/") //folder donde se guarda data/resultado##
   }

   def getRMStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RM"))
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

   def getCFs() {
     getCFStream().saveAsTextFile("data/resultadoCF/") //folder donde se guarda data/resultado##
   }

   def getCFStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("CF"))
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

   def getLFs() {
     getLFStream.saveAsTextFile("data/resultadoLF/") //folder donde se guarda data/resultado##
   }

   def getLFStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LF"))
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

   def getRFs() {
     getRFStream.saveAsTextFile("data/resultadoRF/") //folder donde se guarda data/resultado##
   }

   def getRFStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RF"))
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

   def getLSs() {
     getLSStream.saveAsTextFile("data/resultadoLS/") //folder donde se guarda data/resultado##
   }

   def getLSStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LS"))
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

   def getRSs() {
     getRSStream.saveAsTextFile("data/resultadoRS/") //folder donde se guarda data/resultado##
   }

   def getRSStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RS"))
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

   def getLWs() {
     getLWStream.saveAsTextFile("data/resultadoLW/") //folder donde se guarda data/resultado##
   }

   def getLWStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("LW"))
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

   def getRWs() {
     getRWStream.saveAsTextFile("data/resultadoRW/") //folder donde se guarda data/resultado##
   }

   def getRWStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("RW"))
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

   def getSTs() {
     getSTStream().saveAsTextFile("data/resultadoST/") //folder donde se guarda data/resultado##
   }

   def getSTStream() : RDD[String] = {
     getSelectablePlayersRDD().filter( fields => fields(15).toUpperCase.contains("ST"))
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

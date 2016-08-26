// // // // // // // // // // // // // // // // // 
//           PROJET SPARK                    // //
//       KWEDOM Annick Yolande               // //
// // // // // // // // // // // // // // // // //

package control

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

  //define the schema using a case class
  case class Client (idclient:String, nom:String, ville:String, province:String, codePostal:String)
  
object gestionClient {
  
  def main(args: Array[String]){
    
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

  //configuration de l'application Spark
  val conf= new SparkConf()
  conf.setAppName("SparkSQL").setMaster("local")
  
  //creation du Context Spark
  val sc = new SparkContext(conf)
  
  //SQLContext entry point pour donner les donnees structures
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  //conversion RDD vers DataFrame.
  import sqlContext.implicits._
  
  val clientText = sc.textFile("file:///home/cloudera/client.csv")
   clientText.collect()
   clientText.first()
 
  
  //creation RDD d'objets Client
  
  val client = clientText.map(_.split(", ")).map(p => Client(p(0), p(1), p(2), p(3), p(4)))
  
  //convertir client RDD en un DataFrame
  val dfClients = client.toDF()
 
  //Enregistrer le Dataframe comme table-clients-
  dfClients.registerTempTable("clients")
  
  // Afficher le dataframe dans un format tabulaire
  print(dfClients.show())
  
  //Retourne le schema de ce DataFrame
  dfClients.printSchema() 
  
  // Afficher uniquement la colonne nom
  sqlContext.sql("SELECT nom FROM clients").show

  // Afficher uniquement les colonnes nom et ville
  sqlContext.sql("SELECT nom, ville FROM clients").show

  //Detail du client ayant comme ID 30?
  sqlContext.sql("SELECT * FROM clients WHERE  idclient=30").show
  
  //grouper les clients par code postal
  dfClients.groupBy("codePostal").count.show
 
}
  
}

// //                      FIN                               // //
package org.venraas.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix, IndexedRow, IndexedRowMatrix}
import collection.JavaConversions._
import scopt.OptionParser

object CosineSim {

    def main(args: Array[String]) {

    	run()
    }

    def run() {
        val MAX_TRANS_SIZE = 50
        println("MAX_TRANS_SIZE: %d".format(MAX_TRANS_SIZE))

        val conf = new SparkConf().setAppName("Cosine similarity")

        val sc = new SparkContext(conf)

        val txt = sc.textFile("hdfs://itrihd34:8020/tmp/user_p3category_preferences/*")

	val inData = txt.map(l => { 
		var ary = l.trim.split(',');
		(ary(1), (ary(0), ary(2)));
	})


	//-- mapping table of category_code 2 index 
        var cateCodes = inData.map(t => t._1).distinct().collect()
        var cat2idx:Array[Tuple2[String, String]] = new Array[Tuple2[String, String]](cateCodes.length)
	for (i <-0 to (cateCodes.length - 1)) {		
		cat2idx(i) = new Tuple2(cateCodes(i), i.toString )
	} 
//	cat2idx.foreach { println; }
	
	//-- re-mapping feature index and forms as ($uid, ($feaIdx, $faaVal))
        //
	var cat2idx_rdd:RDD[Tuple2[String, String]] = sc.parallelize(cat2idx)
	var inData_reIdx = inData.join(cat2idx_rdd).map( t => (t._2._1._1, (t._2._2.toInt, t._2._1._2.toDouble)) )
//	inData_reIdx.take(1000).foreach{ println; }

	//-- user category_code feature vector, i.e. ($uid, [($feaIdx, $faaVal)] )
	var u2vct = inData_reIdx.groupByKey().map(t => { (t._1, Vectors.sparse(cateCodes.length, t._2.toSeq)) })
//	u2vct.take(1000).foreach { println; }

	val uids = txt.map(l => { l.trim.split(',')(0) }).distinct().collect()
	var u2idx:Array[Tuple2[String, String]] = new Array[Tuple2[String, String]] (uids.length)
	for (i <-0 to (uids.length -1)) {
		u2idx(i) = new Tuple2(uids(i), i.toString)
	}
//	u2idx.take(100).foreach { println; }

	val u2idx_rdd:RDD[Tuple2[String, String] ] = sc.parallelize(u2idx)
	val rows = u2idx_rdd.join(u2vct).map( t => new IndexedRow(t._2._1.toLong, t._2._2) )
//	rows.take(1000).foreach{ println; }

	val mat:IndexedRowMatrix = new IndexedRowMatrix(rows)
//	println(mat.numCols())
//	println(mat.numRows())

	//-- calc Cosine Similarities
	val exact = mat.toCoordinateMatrix().transpose().toRowMatrix().columnSimilarities()
	exact.toRowMatrix().rows.take(1000).foreach{ println; }



///    trans
///    .map(
///        t => t.mkString("\t")
///    )
///    .saveAsTextFile(
///        "hdfs://itrihd34:8020/tmp/tmp_gohappy_ar_trans"
///    )

//        model.generateAssociationRules(MIN_CONFIDENCE)
//        .filter( 
//            rule => rule.antecedent.size == 1 && rule.consequent.size == 1
//        )
//        .map(
//            rule => {
//                rule.antecedent.mkString(",") + "\t" + rule.consequent.mkString(",") + "\t" + rule.confidence
//            }
//        )
//        .saveAsTextFile(
//            params.output
//        )

        println("done")
        sc.stop()
    }
}


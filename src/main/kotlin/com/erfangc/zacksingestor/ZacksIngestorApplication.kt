package com.erfangc.zacksingestor

import ZacksFundamentalA
import ZacksFundamentalAWrapper
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.google.common.hash.Hashing
import com.vhl.blackmo.grass.dsl.grass
import org.litote.kmongo.KMongo
import org.litote.kmongo.getCollection
import org.litote.kmongo.save
import java.io.File
import java.nio.charset.StandardCharsets

@ExperimentalStdlibApi
fun main() {
    val connectionString = System.getenv("MONGO_URI")
    val mongo = KMongo.createClient(connectionString)
    val database = mongo.getDatabase("starburst")
    val col1 = database.getCollection<ZacksFundamentalAWrapper>()
    val col2 = database.getCollection<ZacksSalesEstimatesWrapper>()

    grass<ZacksFundamentalA> {

    }
        .harvest(csvReader().readAllWithHeader(File("ZACKS_FC.csv")))
        .forEach {
            val _id = Hashing
                .sha256()
                .hashString(
                    "${it.m_ticker}${it.per_end_date}${it.per_type}",
                    StandardCharsets.UTF_8
                )
                .toString()
            col1.save(ZacksFundamentalAWrapper(_id = _id, content = it))
        }

    grass<ZacksSalesEstimates>()
        .harvest(csvReader().readAllWithHeader(File("ZACKS-SE.csv")))
        .forEach {
            val _id = Hashing
                .sha256()
                .hashString(
                    "${it.m_ticker}${it.per_end_date}${it.per_type}",
                    StandardCharsets.UTF_8
                )
                .toString()
            col2.save(ZacksSalesEstimatesWrapper(_id = _id, content = it))
        }

}

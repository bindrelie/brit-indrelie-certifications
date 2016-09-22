package elasticsearch

import grails.converters.JSON

//import grails.transaction.Transactional
import grails.plugins.rest.client.RestBuilder
import org.springframework.http.HttpStatus

//@Transactional
class ElasticsearchService {

    def rest = new RestBuilder()

    def populateElasticsearch() {
        def recordId = 0
        def recordString = ""
        new File("uscitiespop.csv").eachCsvLine { tokens ->
            if (recordId != 0) { //ignore column name line

                if (recordId % 1000 == 0) { //don't send batches over that are too big
                    rest.post("http://localhost:9200/population/external/_bulk?pretty") {
                        contentType "application/json"
                        json recordString
                    }

                    recordString = ""
                }

                def populationRecord = [:]
                populationRecord['sumlev'] = tokens[0]
                populationRecord['name'] = tokens[8]
                populationRecord['stname'] = tokens[9]
                try {
                    populationRecord['census2010pop'] = tokens[10] as int
                } catch (Exception e) { //number not formatted correctly
                    populationRecord['census2010pop'] = null
                }
                try {
                    populationRecord['popestimate2014'] = tokens[16] as int
                } catch (Exception e) { //number not formatted correctly
                    populationRecord['popestimate2014'] = null
                }
                def popChangePercentage
                try {
                    popChangePercentage = Math.round(((populationRecord['popestimate2014'] as int) / (populationRecord['census2010pop'] as int) - 1) * 100)
                } catch (Exception e) {
                    popChangePercentage = "Percentage Unavailable"
                }
                populationRecord['popchangeest'] = popChangePercentage

                recordString += (['index':['_id':recordId]] as JSON)
                recordString += "\n"
                recordString += (populationRecord as JSON)
                recordString += "\n"
            }
            recordId += 1
        }

        def resp = rest.post("http://localhost:9200/population/external/_bulk?pretty") {
            contentType "application/json"
            json recordString
        }

        println('Finished populating population data')

        return resp.statusCode == HttpStatus.OK
    }

    def search(String name, String stname, String census2010pop, String popestimate2014, String popchangeest, Boolean or) {
        def queryTermList = []
        def rangeList = []

        queryTermList.add(["wildcard": ["name": "*${name ? name.toLowerCase() : ""}*"]])
        queryTermList.add(["wildcard": ["stname": "*${stname ? stname.toLowerCase() : ""}*"]])

        ["census2010pop": census2010pop, "popestimate2014": popestimate2014, "popchangeest": popchangeest].each { key, val ->
            if (val) {
                if (val.startsWith("<")) {
                    rangeList.add(["range": ["${key}": ["lt": val[1..-1] as int]]])
                } else if (val.startsWith(">")) {
                    rangeList.add(["range": ["${key}": ["gt": val[1..-1] as int]]])
                } else {
                    queryTermList.add(["match": ["${key}": val as int]])
                }
            }
        }

        def queryObject
        def boolType = or ? "should" : "must"

        //build query object
        if (rangeList.size() > 0) {
            queryObject = [
                "query" : [
                    "bool" : [
                        "${boolType}" : queryTermList,
                        "filter" : [
                            "bool" : [
                                "${boolType}" : rangeList
                            ]
                        ]
                    ]
                ]
            ]
        } else {
            queryObject = [
                "query" : [
                    "bool" : [
                        "${boolType}" : queryTermList
                    ]
                ]
            ]
        }

        def resp = rest.post("http://localhost:9200/population/_search?pretty") {
            contentType: "application/json"
            json queryObject as JSON
        }

       return resp.json
    }

}

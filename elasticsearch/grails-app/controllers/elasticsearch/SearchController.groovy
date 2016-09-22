package elasticsearch

import grails.async.Promise
import grails.async.Promises
import grails.converters.JSON


class SearchController {
    def elasticsearchService

    def index() {
        def or = params.or ? params.or.toBoolean() : false

        def results = elasticsearchService.search(params.name, params.stname, params.census2010pop, params.popestimate2014, params.popchangeest, or)

        if (results.status == 404) {
            render "Elasticsearch data not present, hit /search/populate to populate the data."
        } else {
            render results
        }
    }

    def populate() {
        def asyncProcess = new Thread({
            elasticsearchService.populateElasticsearch()
        } as Runnable)
        asyncProcess.start()
        render "populating elasticsearch data, allow a minute or two for the data to fully populate"
    }
}

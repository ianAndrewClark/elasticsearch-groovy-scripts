#!/usr/bin/env groovy

@GrabResolver(name='sonatype', root='https://oss.sonatype.org/content/groups/public')
@Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.2')
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

import org.elasticsearch.common.xcontent.json.JsonXContent
import groovy.io.FileType
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.action.bulk.BulkResponse
import java.util.zip.GZIPInputStream
import org.elasticsearch.common.xcontent.smile.SmileXContent

def settingsOverride = ['index.number_of_replicas' : '0',
                        'index.refresh_interval' : '-1'
                        ]

def cli = new CliBuilder()
cli.help( longOpt: 'help', required: false, 'show usage information' )
cli.input( longOpt: 'inputfolder', argName: 'input', required: true, args: 1, 'specify the input folder' )
cli.host( longOpt: 'eshost', argName: 'host', required: false, args: 1, 'the es node to connect to' )
cli.index( longOpt: 'esindex', argName: 'index_name', required: true, args: 1, 'the es index to use' )
//TODO - is this really required - can we tell the client to connect with out specifying the cluster?
cli.cluster( longOpt: 'escluster', argName: 'cluster_name', required: true, args: 1, 'the name of the cluster to join to' )

def opt = cli.parse(args)
if (!opt) { return }
if (opt.help) {
	cli.usage();
	return
}

println "proceeding with restore of index \"${opt.index}\" to ${opt.input} by connecting to this es node:- \"${opt.host}\""

//connect to the client
Settings clientSettings = ImmutableSettings.settingsBuilder().put("cluster.name", opt.cluster).build();
Client client = new TransportClient(clientSettings).addTransportAddress(new InetSocketTransportAddress(opt.host, 9300))

//TODO:- remove me
if (client.admin().indices().prepareExists(opt.index).execute().get().exists) {
    print "removing existing index"
    client.admin().indices().prepareDelete(opt.index).execute().get()
    println " -- DONE"
}

//create index
def indexSettingsFile = new File(opt.input, "${opt.index}-settings.json")
indexSettingsFile.withInputStream { indexSettingsStream ->
    print "creating index with settings"
    def settingsParser = JsonXContent.jsonXContent.createParser(indexSettingsStream)
    def settingsMap = settingsParser.mapAndClose()
    //override + additional settings
    settingsMap.putAll(settingsOverride)
    indexSettingsB = ImmutableSettings.settingsBuilder().put(settingsMap)
    client.admin().indices().prepareCreate(opt.index).setSettings(indexSettingsB).execute().get()
    println " -- DONE"
}

//find the mappings and process
(new File(opt.input)).eachFileMatch FileType.FILES, ~/${opt.index}-.*-mapping.json/, {
    def fileNamePattern = ~/${opt.index}-(.*)-mapping.json/
    def mappingName = fileNamePattern.matcher(it.name)[0][1]
    print "creating mapping, ${mappingName}"
    it.withInputStream { mappingInput ->
        def mappingParser = JsonXContent.jsonXContent.createParser(mappingInput)
        client.admin().indices().preparePutMapping(opt.index).setType(mappingName).setSource(mappingParser.mapAndClose()).execute().get()
        println " -- DONE"
    }
}

//run through the data and index
def dataFile = new File(opt.input, "${opt.index}-data.gz.smile")
dataFile.withInputStream { dataStream ->
    def dataParser = SmileXContent.smileXContent.createParser(new GZIPInputStream(dataStream))
    def count = 0
    def currentBulkRequest = client.prepareBulk()
    println "indexing data"
    if(dataParser.nextToken() == XContentParser.Token.START_ARRAY) { //could be valid
        while(dataParser.currentToken() != XContentParser.Token.END_ARRAY) {
            if(dataParser.nextToken() == XContentParser.Token.START_OBJECT) {
                def esDoc = dataParser.map()
                currentBulkRequest = index(client, currentBulkRequest, opt.index, esDoc['type'], esDoc['id'], esDoc['source'])
                //println "record ${count++} -- DONE"
            }
        }
    }
    //do any remaining
    if(currentBulkRequest.numberOfActions() > 0) {
        doBulk(client, currentBulkRequest)
    }
    println " -- DONE"
}

/*
 This method will add this document to the current bulk request and if the size is big enough it will do the bulk request
 */
def index(client, currentBulkRequest, indexName, indexType, id, doc) {
    def batchSize = 1000

    def indexRequest = client.prepareIndex(indexName,indexType,id)
    indexRequest.source = doc
    currentBulkRequest.add(indexRequest)

    if(currentBulkRequest.numberOfActions() >= batchSize) {
        doBulk(client, currentBulkRequest)
        return client.prepareBulk()
    } else {
        return currentBulkRequest
    }
}

def doBulk(client, currentBulkRequest) {
    print "doing bulk request"
    BulkResponse bulkResponse = currentBulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
        bulkResponse.items().each { item ->
            println "Bulk Index failure:- "+item.failureMessage()
        }
        println " -- FAILURES!"
    } else {
        println " -- DONE"
    }

    return bulkResponse
}
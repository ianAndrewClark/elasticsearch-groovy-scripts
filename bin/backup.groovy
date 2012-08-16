#!/usr/bin/env groovy

@GrabResolver(name='sonatype', root='https://oss.sonatype.org/content/groups/public')
@Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.2')
import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.SearchHit
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.MatchAllQueryBuilder
import java.util.zip.GZIPOutputStream

def settingsToIgnore = ["index.version.created"]

def cli = new CliBuilder()
cli.help( longOpt: 'help', required: false, 'show usage information' )
cli.output( longOpt: 'outputfolder', argName: 'output', required: true, args: 1, 'specify the output folder' )
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

println "proceeding with backup of index \"${opt.index}\" to ${opt.output} by connecting to this es node:- \"${opt.host}\""

//connect to the client
Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", opt.cluster)
        .build();
Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(opt.host, 9300))

def indexMetaData = client.admin().cluster().prepareState().execute().actionGet().getState().metaData.index(opt.index)

//create settings json file
def indexSettings = indexMetaData.settings
File settingsFile = new File(opt.output, "${opt.index}-settings.json")
if(!settingsFile.createNewFile()) {
    print "deleting old settings file....."
    settingsFile.delete()
    settingsFile.createNewFile()
    println " -- DONE"
}

print "writing settings"
def settingsOut = settingsFile.newOutputStream()
XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, settingsOut).prettyPrint()
builder.startObject()
indexSettings.getAsMap().each {k, v ->
    if(!settingsToIgnore.contains(k)) builder.field(k, v) //ignore settings....
}
builder.endObject()
builder.close()
settingsOut.close()
println " -- DONE"

//create each mapping file
def indexMappings = indexMetaData.mappings()
indexMappings.each { mappingName, mappingMetaData ->
    def mappingFile = new File(opt.output, "${opt.index}-${mappingName}-mapping.json")
    if(!mappingFile.createNewFile()) {
        print "deleting old mapping file....."
        mappingFile.delete()
        mappingFile.createNewFile()
        println " -- DONE"
    }
    def mappingOut = mappingFile.newOutputStream()
    print "writing mapping for ${mappingName} type"
    XContentBuilder mappingBuilder = XContentFactory.contentBuilder(XContentType.JSON, mappingOut).prettyPrint()
    mappingBuilder.startObject()
    mappingBuilder.startObject(mappingName)
    mappingMetaData.sourceAsMap().each {  k, v ->
        mappingBuilder.field(k, v)
    }
    mappingBuilder.endObject()
    mappingBuilder.endObject()
    mappingBuilder.close()
    mappingOut.close()
    println " -- DONE"
}

//create data file
def dataFile = new File(opt.output, "${opt.index}-data.gz.smile")
if(!dataFile.createNewFile()) {
    print "deleting old data file....."
    dataFile.delete()
    dataFile.createNewFile()
    println " -- DONE"
}

println "exporting documents"
def dataOut = new GZIPOutputStream(dataFile.newOutputStream())
XContentBuilder outputBuilder = XContentFactory.contentBuilder(XContentType.SMILE, dataOut)/*.prettyPrint()*/
outputBuilder.startArray()

QueryBuilder qb = new MatchAllQueryBuilder()

SearchResponse scrollResp = client.prepareSearch(opt.index)
        .setSearchType(SearchType.SCAN)
        .setScroll(new TimeValue(60000))
        .setQuery(qb)
        .setSize(50).execute().actionGet(); //X hits per shard will be returned for each scroll
//Scroll until no hits are returned
def count = 1;
while (true) {
    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
    for (SearchHit hit : scrollResp.getHits()) {
        //Handle the hit...
        outputBuilder.startObject()
        outputBuilder.field("id", hit.id)
        outputBuilder.field("type", hit.type)
        outputBuilder.field("version", hit.version)
        outputBuilder.startObject("source")
        hit.source.each {  k, v ->
            outputBuilder.field(k, v)
        }
        outputBuilder.endObject()
        outputBuilder.endObject()
        count++
    }
    println "done ${count}"
    //Break condition: No hits are returned
    if (scrollResp.hits().hits().length == 0) {
        break;
    }
}
outputBuilder.endArray()
outputBuilder.close()
dataOut.close()
println " -- DONE"

import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.queries.internal.SimpleSetReceiver
import edu.cmu.graphchidb.queries.Queries
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.storage.{VarDataColumn, Column}



/**
 * @author Aapo Kyrola
 */
class GdBenchGraphChiDbDriver extends TestDriver {

  // Type 0: friendship, type 1: likes
  val FRIENDSHIP = 0.toByte
  val LIKE = 1.toByte

  var DB : GraphChiDatabase = null

  var varDataIndexColumn: Column[Long] = null
  var varDataColumn: VarDataColumn = null

  var type0Counters : Column[Int]  = null
  var type1Counters : Column[Int]  = null

  var firstWebpageId = 0L // TODO


  val baseFilename = "/Users/akyrola/graphs/DB/gdbench/gdbench"

  val numShards = 8
  def createDB(p1: String) = {
    GraphChiDatabaseAdmin.createDatabase(baseFilename, numShards=numShards, maxId = 256000000L)

  }

  def openDB(p1: String) = {
    DB = new GraphChiDatabase(baseFilename, disableDegree = true, enableVertexShardBits = false, numShards=numShards)
    /* Create columns */
    varDataIndexColumn = DB.createLongColumn("vardataidx", DB.vertexIndexing)
    varDataColumn = DB.createVarDataColumn("vardata", DB.vertexIndexing)

    type0Counters = DB.createIntegerColumn("type0cnt", DB.vertexIndexing)
    type1Counters = DB.createIntegerColumn("type1cnt", DB.vertexIndexing)

    DB.initialize()
    true
  }

  def closeDB() = {
     DB.close()
     true
  }

  def openTransaction() = true

  def closeTransaction() = true

  def getNumberOfNodes =  DB.numVertices

  def getNumberOfEdges = DB.numEdges

  def getDBsize = 0L

  var friendCount = 0
  var likeCount = 0
  val t = System.currentTimeMillis()

  val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")


  def insertPerson(id: Long, name: String, age: String, location: String) = {
      val payload = "" + name + "/" + age + "/" + location
      val payloadId = varDataColumn.insert(payload.getBytes)
      varDataIndexColumn.set(DB.originalToInternalId(id), payloadId)
      firstWebpageId = math.max(firstWebpageId, id + 1)
      true
  }

  def insertWebPage(wpId: Long, url: String, date: String) = {
    val vertexId = firstWebpageId + wpId
    val payload =  "" + url + "/" + date
    val payloadId = varDataColumn.insert(payload.getBytes)
    varDataIndexColumn.set(DB.originalToInternalId(vertexId), payloadId)
    true
  }

  def insertFriend(personId: Long, person2Id: Long) = {
    // Two way
    DB.addEdgeOrigId(FRIENDSHIP, personId, person2Id)
    DB.addEdgeOrigId(FRIENDSHIP, person2Id, personId)

    type0Counters.update(DB.originalToInternalId(personId), c=>c.getOrElse(0) + 1)
    type0Counters.update(DB.originalToInternalId(person2Id), c=>c.getOrElse(0) + 1)


    if (friendCount % 10000 == 0) ingestMeter.mark(10000)
    if (friendCount % 1000000 == 0) println("Friends %d".format(friendCount))

    if (friendCount % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + "s ;" + ingestMeter.getOneMinuteRate + " / sec"
      + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
    friendCount += 1

    if (friendCount % 333333 == 0) {
       // Test query for debug
       DB.queryIn(DB.originalToInternalId(personId), FRIENDSHIP)
       DB.queryIn(DB.originalToInternalId(person2Id), FRIENDSHIP)
    }

    true
  }

  def insertLike(personId: Long, webPageId: Long) = {
    DB.addEdgeOrigId(LIKE, personId, firstWebpageId + webPageId)
    type1Counters.update(DB.originalToInternalId(personId), c=>c.getOrElse(0) + 1)
    if (likeCount % 1000000 == 0) println("Likes %d".format(likeCount))

    if (likeCount % 10000 == 0) ingestMeter.mark(10000)

    if (likeCount % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + "s ;" + ingestMeter.getOneMinuteRate + " / sec"
      + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
    likeCount += 1

    true
  }

  // People having name N
  def Q1(p1: String) = 0L

  // People that like a given web page W
  def Q2(webPageId: Long) = {
     val webPageInternalId = DB.originalToInternalId(firstWebpageId + webPageId)
     DB.queryIn(webPageInternalId, LIKE).size
  }

  // The web pages that person P likes
  def Q3(p1: Long) = {
     val personInternalId = DB.originalToInternalId(p1)
     DB.queryOut(personInternalId, LIKE).size
  }

  // The name of the person with given PID
  def Q4(p1: Long) = {
    val personInternalId = DB.originalToInternalId(p1)
    val varDataId = varDataIndexColumn.get(personInternalId).get
    var data = new String(varDataColumn.get(varDataId))
    data.substring(0, data.indexOf("/"))
  }

  // The friends of friends of given person P
  def Q5(p1: Long) = {
    val personInternalId = DB.originalToInternalId(p1)
    Queries.friendsOfFriendsSet(personInternalId, FRIENDSHIP)(DB).size
  }

  // The web pages liked by the friends of given person P
  def Q6(p1: Long) = {
    val personInternalId = DB.originalToInternalId(p1)
    val friendReceiver = new SimpleSetReceiver
    DB.queryOut(personInternalId, FRIENDSHIP, friendReceiver)
    val webPageReceiver = new SimpleSetReceiver
    DB.queryOutMultiple(friendReceiver.set.toSet, LIKE, webPageReceiver)
    webPageReceiver.set.size
  }

  // Get people that likes a web page which a person P likes
  def Q7(p1: Long) = {
    val personInternalId = DB.originalToInternalId(p1)
    val webPageRecv = new SimpleSetReceiver
    DB.queryOut(personInternalId, LIKE, webPageRecv)
    val webPagesLiked = webPageRecv.set.toSeq
    val peopleLikingReceiver = new SimpleSetReceiver
    webPagesLiked.par.foreach(pageId => DB.queryIn(pageId, LIKE, peopleLikingReceiver))
    peopleLikingReceiver.set.size
  }

  // Is there connection between P1 and P2
  def Q8(p1: Long, p2: Long) = false

  // Shortest path between people P1 and P2
  def Q9(p1: Long, p2: Long) = 0L

  /// The common friends between people P1 and P2
  def Q10(p1: Long, p2: Long) = {
    val friends1Recv =  new SimpleSetReceiver
    DB.queryOut(DB.originalToInternalId(p1), FRIENDSHIP, friends1Recv)
    val friends2Recv =  new SimpleSetReceiver
    DB.queryOut(DB.originalToInternalId(p1), FRIENDSHIP, friends2Recv)
    friends1Recv.set.intersect(friends2Recv.set).size
  }

  // The common web pages that P1 and P2 like
  def Q11(p1: Long, p2: Long) = {
    val pages1Recv =  new SimpleSetReceiver
    val pages2Recv =  new SimpleSetReceiver

    val pages1 = DB.queryOut(DB.originalToInternalId(p1), LIKE, pages1Recv)
    val pages2 = DB.queryOut(DB.originalToInternalId(p1), LIKE, pages2Recv)
    pages1Recv.set.intersect(pages2Recv.set).size
  }

  // The number of friends of a person (can use aggregate)
  def Q12(p1: Long) = type0Counters.get(DB.originalToInternalId(p1)).getOrElse(0).toLong
}

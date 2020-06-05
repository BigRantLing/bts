package Query

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.{Iterables, Lists}
import org.apache.druid.query.Druids.SegmentMetadataQueryBuilder
import org.apache.druid.query.aggregation.{DoubleMaxAggregatorFactory, DoubleMinAggregatorFactory}
import org.apache.druid.query.topn.TopNQueryBuilder
import org.junit.{Assert, Before, Test}


class QueryParserTest() {

  var mapper: ObjectMapper = null

  @Before
  def init() = {
    mapper = TestHelper.makeDefaultMappper
  }


  @Test
  def testTopNParser= {

    val query =  new TopNQueryBuilder()
      .dataSource(TestHelper.DATA_SOURCE)
      .granularity(TestHelper.ALL_GRAN)
      .dimension(TestHelper.TEST_DIMENSION)
      .metric(TestHelper.INDEX_METRIC)
      .threshold(4)
      .intervals(TestHelper.FULL_ON_INTERVAL_SPEC)
      .aggregators(
        Lists.newArrayList(
          Iterables.concat(
            TestHelper.COMMON_DOUBLE_AGGREGATORS,
            Lists.newArrayList(
              new DoubleMaxAggregatorFactory("metric", "index"),
              new DoubleMinAggregatorFactory("minIndex", "index")
            ),
            Lists.newArrayList(),
            Lists.newArrayList(),
            Lists.newArrayList()
        )
       )
      )
      .postAggregators(TestHelper.ADD_ROWS_INDEX_CONSTANT)
      .build()

    val queryJson = mapper.writeValueAsString(query)
    val serializedQuery = mapper.readValue(queryJson, TestHelper.QUERY_CLASS)

    Assert.assertEquals(query, serializedQuery)

  }

  @Test
  def testSegementMetadataQuery = {
    val segementMetadataQuery = new SegmentMetadataQueryBuilder()
      .dataSource(TestHelper.DATA_SOURCE)
      .intervals(TestHelper.FULL_ON_INTERVAL_SPEC)
      .build()

    val queryJson = mapper.writeValueAsString(segementMetadataQuery)
    val serializedQuery = mapper.readValue(queryJson, TestHelper.QUERY_CLASS)

    Assert.assertEquals(segementMetadataQuery, serializedQuery)
  }



}

package Query

import java.util
import java.util.Collections

import com.fasterxml.jackson.databind.{InjectableValues, ObjectMapper}
import com.google.common.collect.{ImmutableList, Lists}
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.java.util.common.Intervals
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory
import org.apache.druid.query.aggregation.post.{ArithmeticPostAggregator, ConstantPostAggregator, FieldAccessPostAggregator}
import org.apache.druid.query.aggregation.{CountAggregatorFactory, DoubleSumAggregatorFactory, LongSumAggregatorFactory}
import org.apache.druid.query.expression.{IPv4AddressMatchExprMacro, IPv4AddressParseExprMacro, IPv4AddressStringifyExprMacro, LikeExprMacro, RegexpExtractExprMacro, TimestampCeilExprMacro, TimestampExtractExprMacro, TimestampFloorExprMacro, TimestampFormatExprMacro, TimestampParseExprMacro, TimestampShiftExprMacro, TrimExprMacro}
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder
import org.apache.druid.timeline.SegmentId
import org.joda.time.Interval

object TestHelper {

  val QUERY_CLASS = Class.forName("org.apache.druid.query.Query")
  val DATA_SOURCE = "testing"
  val FULL_ON_INTERVAL = Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
  val SEGMENT_ID = SegmentId.of(DATA_SOURCE, FULL_ON_INTERVAL, "dummy_version", 0)
  val ALL_GRAN = Granularities.ALL
  val TEST_DIMENSION = "test"
  val INDEX_METRIC = "metric"
  val TIME_DIMENSION = "__time"
  val FULL_ON_INTERVAL_SPEC = new MultipleIntervalSegmentSpec(Collections.singletonList(FULL_ON_INTERVAL))
  val ROWS_COUNT = new CountAggregatorFactory("rows")
  val INDEX_LONG_SUM = new LongSumAggregatorFactory("index", INDEX_METRIC)
  val TIME_LONG_SUM = new LongSumAggregatorFactory("sumtime", TIME_DIMENSION)
  val INDEX_DOUBLE_SUM = new DoubleSumAggregatorFactory("index", INDEX_METRIC)
  val ADD_ROWS_INDEX_CONSTANT_METRIC = "addRowsIndexConstant"
  val CONSTANT = new ConstantPostAggregator("const", 1L)
  val ROWS_POST_AGG = new FieldAccessPostAggregator("rows", "rows")
  val INDEX_POST_AGG = new FieldAccessPostAggregator("index", "index")
  val ADD_ROWS_INDEX_CONSTANT = new ArithmeticPostAggregator(ADD_ROWS_INDEX_CONSTANT_METRIC, "+", Lists.newArrayList(CONSTANT, ROWS_POST_AGG, INDEX_POST_AGG))
  val QUALITY_UNIQUES = new HyperUniquesAggregatorFactory(
    "uniques",
    "quality_uniques"
  )
  val COMMON_DOUBLE_AGGREGATORS = util.Arrays.asList(
    ROWS_COUNT,
    INDEX_DOUBLE_SUM,
    QUALITY_UNIQUES
  )

  def makeDefaultMappper = {
    val mapper = new DefaultObjectMapper()

    mapper.setInjectableValues(
      new InjectableValues.Std()
        .addValue(classOf[ObjectMapper], mapper)
        .addValue(classOf[PruneSpecsHolder], PruneSpecsHolder.DEFAULT)
        .addValue(classOf[ExprMacroTable], new ExprMacroTable(
          ImmutableList.of(
            new IPv4AddressMatchExprMacro(),
            new IPv4AddressParseExprMacro(),
            new IPv4AddressStringifyExprMacro(),
            new LikeExprMacro(),
            new RegexpExtractExprMacro(),
            new TimestampCeilExprMacro(),
            new TimestampExtractExprMacro(),
            new TimestampFloorExprMacro(),
            new TimestampFormatExprMacro(),
            new TimestampParseExprMacro(),
            new TimestampShiftExprMacro(),
            new TrimExprMacro.BothTrimExprMacro(),
            new TrimExprMacro.LeftTrimExprMacro(),
            new TrimExprMacro.RightTrimExprMacro()
          )))
        .addValue(classOf[ObjectMapper], mapper)
        .addValue(classOf[PruneSpecsHolder], PruneSpecsHolder.DEFAULT)
    )
    mapper
  }
}

package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A special variant of a MapAggregator, which improved handling of timestamp-based aggregation:
 *
 * It automatically fills timestamps with no data with "zero"s (which for example results in 0's in the case os `sum()` or `count()`, or `NaN` when using `average()`).
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 */
public class MapAggregatorByTimestamps<X> extends MapAggregator<OSHDBTimestamp, X> {

  /**
   * basic constructor
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the timestamp value into which to aggregate the respective result
   */
  MapAggregatorByTimestamps(MapReducer<X> mapReducer, SerializableFunction<X, OSHDBTimestamp> indexer) {
    super(mapReducer, indexer);
  }

  /**
   * Map-reduce routine with built-in aggregation by timestamp
   *
   * This can be used to perform an arbitrary map-reduce routine whose results should be aggregated separately according to timestamps.
   *
   * Timestamps with no results are filled with zero values (as provided by the `identitySupplier` function).
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  @Override
  public <S> SortedMap<OSHDBTimestamp, S> reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    SortedMap<OSHDBTimestamp, S> result = super.reduce(identitySupplier, accumulator, combiner);
    // fill nodata entries with "0"
    final List<OSHDBTimestamp> timestamps = this._mapReducer._getTimestamps().stream().map(OSHDBTimestamp::new).collect(Collectors.toList());
    // pop last element from timestamps list if we're dealing with OSMContributions (where the timestamps list defines n-1 time intervals)
    if (this._mapReducer._forClass.equals(OSMContribution.class))
      timestamps.remove(timestamps.size()-1);
    timestamps.forEach(ts -> result.putIfAbsent(ts, identitySupplier.get()));
    return result;
  }
}
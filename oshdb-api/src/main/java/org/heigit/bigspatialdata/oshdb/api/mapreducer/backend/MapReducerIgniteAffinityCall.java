package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDbHelper;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

/**
 * {@inheritDoc}
 *
 * <p>
 * The "AffinityCall" implementation is a very simple, but less efficient implementation of the
 * oshdb mapreducer: It's just sending separate affinityCalls() to the cluster for each data cell
 * and reduces all results locally on the client.
 * </p>
 *
 * <p>
 * It's good for testing purposes and maybe a viable option for special circumstances where one
 * knows beforehand that only few cells have to be iterated over (e.g. queries in a small area of
 * interest), where the (~constant) overhead associated with the other methods might be larger than
 * the (~linear) inefficiency with this implementation.
 * </p>
 */
public class MapReducerIgniteAffinityCall<X> extends MapReducer<X>
    implements CancelableProcessStatus {

  /**
   * Stores the start time of reduce/stream operation as returned by
   * {@link System#currentTimeMillis()}. Used to determine query timeouts.
   */
  private long executionStartTimeMillis;

  public MapReducerIgniteAffinityCall(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerIgniteAffinityCall(MapReducerIgniteAffinityCall obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerIgniteAffinityCall<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  public boolean isActive() {
    if (timeout != null && System.currentTimeMillis() - executionStartTimeMillis > timeout) {
      throw new OSHDBTimeoutException();
    }
    return true;
  }

  @Nonnull
  private static SerializableFunction<CellIdRange, LongStream> cellIdRangeToCellIds() {
    return cellIdRange -> {
      int level = cellIdRange.getStart().getZoomLevel();
      long from = CellId.getLevelId(level, cellIdRange.getStart().getId());
      long to = CellId.getLevelId(level, cellIdRange.getEnd().getId());
      return LongStream.rangeClosed(from, to);
    };
  }

  /**
   * Converts remote OSHDB and native ignite future timeouts.
   *
   * @param async the ignite future to execute and inspect
   * @param timeout max individual request runtime in milliseconds
   * @param <T> result data type
   * @return the result
   * @throws OSHDBTimeoutException if the request took to long or a OSHDBTimeoutException was
   *         thrown remotely
   */
  private static <T> T asyncGetHandleTimeouts(IgniteFuture<T> async, Long timeout)
      throws OSHDBTimeoutException {
    try {
      if (timeout == null) {
        return async.get();
      } else {
        return async.get(timeout);
      }
    } catch (IgniteFutureTimeoutException e) {
      throw new OSHDBTimeoutException();
    } catch (IgniteException e) {
      if (e.getCause().getCause() instanceof OSHDBTimeoutException) {
        throw (OSHDBTimeoutException) e.getCause().getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Implements a generic reduce operation.
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );
    
    Stream<S> updateStream = Stream.empty();
    if (this.update != null) {
      // get bitmap of changed entities
      Map<OSMType, LongBitmapDataProvider> bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      // create a second celliterator for updates, copy settings from first
      // because streams are lazy we have to have two celliterators and cannot change the first one
      CellIterator updateIterator = new CellIterator(
          this.tstamps.get(),
          this.bboxFilter, this.getPolyFilter(),
          this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
      );
      // exclude updated entities in original data and include in updates
      cellIterator.excludeIDs(bitMapIndex);
      updateIterator.includeIDsOnly(bitMapIndex);
      // create a stream of updaten data
      updateStream = Streams.stream(this.getUpdates())
          .parallel()
          .filter(ignored -> this.isActive())
          .map(oshCell -> cellProcessor.apply(oshCell, updateIterator));
    }

    final Iterable<CellIdRange> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = (OSHDBIgnite) this.oshdb;
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();
    IgniteRunnable onClose = oshdb.onClose().orElse(() -> { });

    Stream<S> result = this.typeFilter.stream().map((SerializableFunction<OSMType, S>) osmType -> {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds())
          .parallel()
          .filter(ignored -> this.isActive())
          .mapToObj(cellLongId -> asyncGetHandleTimeouts(
              compute.affinityCallAsync(cacheName, cellLongId, () -> {
                @SuppressWarnings("SerializableStoresNonSerializable")
                GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
                S ret;
                if (oshEntityCell == null) {
                  ret = identitySupplier.get();

                } else {
                  ret = cellProcessor.apply(oshEntityCell, cellIterator);
                }
                onClose.run();
                return ret;
              }),
              this.timeout
          ))
          .reduce(identitySupplier.get(), combiner);
    });

    return Streams.concat(result, updateStream).reduce(identitySupplier.get(), combiner);
  }

  /**
   * Implements a generic stream operation.
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private Stream<X> stream(
      CellProcessor<Stream<X>> cellProcessor
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    Stream<X> updateStream = Stream.empty();
    if (this.update != null) {
      // get bitmap of changed entities
      Map<OSMType, LongBitmapDataProvider> bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      // create a second celliterator for updates, copy settings from first
      // because streams are lazy we have to have two celliterators and cannot change the first one
      CellIterator updateIterator = new CellIterator(
          this.tstamps.get(),
          this.bboxFilter, this.getPolyFilter(),
          this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
      );
      // exclude updated entities in original data and include in updates
      cellIterator.excludeIDs(bitMapIndex);
      updateIterator.includeIDsOnly(bitMapIndex);
      // create a stream of updaten data
      updateStream = Streams.stream(this.getUpdates())
          .parallel()
          .filter(ignored -> this.isActive())
          .flatMap(oshCell -> cellProcessor.apply(oshCell, updateIterator));
    }

    final Iterable<CellIdRange> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = (OSHDBIgnite) this.oshdb;
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();
    IgniteRunnable onClose = oshdb.onClose().orElse(() -> { });

    Stream<X> result = Stream.empty();
    for (OSMType osmType : typeFilter) {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      GetMatchingKeysPreflight preflight;
      int maxNumCells = 0;
      for (CellIdRange cellIdRange : cellIdRanges) {
        maxNumCells += cellIdRange.getEnd().getId() - cellIdRange.getStart().getId();
      }
      // if number of "maximum to be requested cells" is larger than the total avaiable cells
      // -> use scanquery based preflight, otherwise use localpeek implementation.
      // this works as long as the assumption that calling "localPeek" is about the same effort
      // than checking wether a cell is in the requested area.
      // todo: benchmark if this assumption is really the case
      if (maxNumCells > cache.size()) {
        preflight = new GetMatchingKeysPreflightScanQuery(
            cacheName, cellIdRangeToCellIds(), cellIdRanges, cellProcessor, cellIterator
        );
      } else {
        preflight = new GetMatchingKeysPreflightLocalPeek(
            cacheName, cellIdRangeToCellIds(), cellIdRanges, cellProcessor, cellIterator
        );
      }
      List<Long> cellsWithData = asyncGetHandleTimeouts(
          compute.broadcastAsync(preflight),
          this.timeout
      ).stream()
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
      Collections.shuffle(cellsWithData);
      Stream<X> resultForType = cellsWithData.parallelStream()
          .filter(ignored -> this.isActive())
          .map(cellLongId -> asyncGetHandleTimeouts(
              compute.affinityCallAsync(cacheName, cellLongId, () -> {
                GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
                Collection<X> ret;
                if (oshEntityCell == null) {
                  ret = Collections.<X>emptyList();
                } else {
                  ret = cellProcessor.apply(oshEntityCell, cellIterator)
                      .collect(Collectors.toList());
                }
                onClose.run();
                return ret;
              }),
              this.timeout
          ))
          .flatMap(Collection::stream);
      result = Stream.concat(result, resultForType);
    }
    return Streams.concat(result, updateStream);
  }

  // === map-reduce operations ===

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMContributionCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMContributionGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMEntitySnapshotCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMEntitySnapshotGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  // === stream operations ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return stream(Kernels.getOSMContributionCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMContributionGroupingCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper, this));
  }

  abstract static class GetMatchingKeysPreflight implements IgniteCallable<Collection<Long>> {

    @IgniteInstanceResource
    Ignite ignite;

    final String cacheName;
    final Function<CellIdRange, LongStream> cellIdRangeToCellIds;
    final Iterable<CellIdRange> cellIdRanges;
    final CellProcessor<? extends Stream<?>> cellProcessor;
    final CellIterator cellIterator;

    private GetMatchingKeysPreflight() {
      throw new IllegalStateException("utility class");
    }

    GetMatchingKeysPreflight(
        String cacheName,
        Function<CellIdRange, LongStream> cellIdRangeToCellIds,
        Iterable<CellIdRange> cellIdRanges,
        CellProcessor<? extends Stream<?>> cellProcessor,
        CellIterator cellIterator
    ) {
      this.cacheName = cacheName;
      this.cellIdRangeToCellIds = cellIdRangeToCellIds;
      this.cellIdRanges = cellIdRanges;
      this.cellProcessor = cellProcessor;
      this.cellIterator = cellIterator;
    }
  }

  static class GetMatchingKeysPreflightLocalPeek extends GetMatchingKeysPreflight {
    GetMatchingKeysPreflightLocalPeek(
        String cacheName,
        Function<CellIdRange, LongStream> cellIdRangeToCellIds,
        Iterable<CellIdRange> cellIdRanges,
        CellProcessor<? extends Stream<?>> cellProcessor,
        CellIterator cellIterator
    ) {
      super(cacheName, cellIdRangeToCellIds, cellIdRanges, cellProcessor, cellIterator);
    }

    @Override
    public Collection<Long> call() {
      IgniteCache<Long, GridOSHEntity> localCache = ignite.cache(cacheName);
      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds)
          .parallel()
          .filter(cellLongId -> {
            // test if cell exists and contains any relevant data
            GridOSHEntity cell = localCache.localPeek(cellLongId);
            return cell != null && cellProcessor.apply(cell, cellIterator).findAny().isPresent();
          })
          .boxed()
          .collect(Collectors.toList());
    }
  }

  static class GetMatchingKeysPreflightScanQuery extends GetMatchingKeysPreflight {
    private final Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel;

    GetMatchingKeysPreflightScanQuery(
        String cacheName,
        Function<CellIdRange, LongStream> cellIdRangeToCellIds,
        Iterable<CellIdRange> cellIdRanges,
        CellProcessor<? extends Stream<?>> cellProcessor,
        CellIterator cellIterator
    ) {
      super(cacheName, cellIdRangeToCellIds, cellIdRanges, cellProcessor, cellIterator);

      this.cellIdRangesByLevel = new HashMap<>();
      for (CellIdRange cellIdRange : cellIdRanges) {
        int level = cellIdRange.getStart().getZoomLevel();
        if (!this.cellIdRangesByLevel.containsKey(level)) {
          this.cellIdRangesByLevel.put(level, new TreeMap<>());
        }
        this.cellIdRangesByLevel.get(level).put(cellIdRange.getStart().getId(), cellIdRange);
      }
    }

    @Override
    public Collection<Long> call() {
      IgniteCache<Long, BinaryObject> localCache = ignite.cache(cacheName).withKeepBinary();
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = Ints.asList(
          ignite.affinity(cacheName).primaryPartitions(ignite.cluster().localNode())
      );
      Collections.shuffle(myPartitions);

      return myPartitions.parallelStream()
          .map(part -> {
            try (QueryCursor<Optional<Long>> cursor = localCache.query(
                new ScanQuery<Long, Object>((key, cell) ->
                    MapReducerIgniteScanQuery.cellKeyInRange(key, cellIdRangesByLevel)
                ).setPartition(part), cacheEntry -> {
                  Object data = cacheEntry.getValue();
                  GridOSHEntity oshEntityCell;
                  if (data instanceof BinaryObject) {
                    oshEntityCell = ((BinaryObject) data).deserialize();
                  } else {
                    oshEntityCell = (GridOSHEntity) data;
                  }
                  return cellProcessor.apply(oshEntityCell, this.cellIterator).findAny().map(
                      ignored -> cacheEntry.getKey()
                  );
                }
            )) {
              List<Long> acc = new LinkedList<>();
              for (Optional<Long> entry : cursor) {
                entry.ifPresent(acc::add);
              }
              return acc;
            }
          })
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }
  }
}

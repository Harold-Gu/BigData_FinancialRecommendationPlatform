package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;

import bigdata.util.TimeUtil;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;


public class AssessedExercise {

public static void main(String[] args) throws InterruptedException {
		
		//--------------------------------------------------------
	    // Static Configuration
	    //--------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;
	
		long startTime = System.currentTimeMillis();
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();

		
		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries
		
		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries
		
		
    	//----------------------------------------
    	// Pre-provided code for loading the data 
    	//----------------------------------------
    	
    	// Create Datasets based on the input files
		
		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		//assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());
		
		// Load in the prices, this is a large file (not so much in data size, but in number of records)
    	Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
    	Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
    	Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects
		
	
		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);
		
		System.out.println(finalRanking.toString());
		
		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");
		
		Thread.sleep(60000);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		
		
		long endTime = System.currentTimeMillis();
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			
			Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
			Date sdate = Date.from( sinstant );
			
			Instant einstant = Instant.ofEpochSecond( endTime/1000 );
			Date edate = Date.from( einstant );
			
			writer.write("StartTime:"+sdate.toGMTString()+'\n');
			writer.write("EndTime:"+edate.toGMTString()+'\n');
			writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


	public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {

		//----------------------------------------
		// Student's solution starts here
		//----------------------------------------

		// 1. 提前解析截止日期为原生的 short 类型，避免后续的任何字符串解析
		String[] dateParts = datasetEndDate.split("-");
		final short endYear = Short.parseShort(dateParts[0]);
		final short endMonth = Short.parseShort(dateParts[1]);
		final short endDay = Short.parseShort(dateParts[2]);

		// 2. 极限谓词下推：时间窗口双向剪枝 (Dual-Bound Pruning)
		JavaPairRDD<String, StockPrice> pairedPrices = prices.toJavaRDD()
				.filter(new Function<StockPrice, Boolean>() {
					@Override
					public Boolean call(StockPrice sp) throws Exception {
						// 1. 过滤掉未来的数据 (你之前的代码)
						if (sp.getYear() > endYear) return false;
						if (sp.getYear() == endYear && sp.getMonth() > endMonth) return false;
						if (sp.getYear() == endYear && sp.getMonth() == endMonth && sp.getDay() > endDay) return false;

						// 2. 【核弹级优化】：过滤掉远古数据
						// 任务只需要过去 251 个交易日（约 1 个自然年）。
						// 我们直接丢弃比 endYear 早 2 年以上的所有数据！
						// 这将直接消除近 90% 的无效网络 Shuffle 和 排序开销。
						if (sp.getYear() < endYear - 2) return false;

						return true;
					}
				})
				.mapToPair(new PairFunction<StockPrice, String, StockPrice>() {
					@Override
					public Tuple2<String, StockPrice> call(StockPrice sp) throws Exception {
						return new Tuple2<>(sp.getStockTicker(), sp);
					}
				});

		// 3. 按股票代码对价格记录进行分组
		JavaPairRDD<String, Iterable<StockPrice>> groupedPrices = pairedPrices.groupByKey();

		// 4. 计算指标与底层深度过滤 (核心加速区)
		JavaPairRDD<String, AssetFeatures> featuresRDD = groupedPrices.mapValues(
				new Function<Iterable<StockPrice>, AssetFeatures>() {
					@Override
					public AssetFeatures call(Iterable<StockPrice> priceIter) throws Exception {
						List<StockPrice> priceList = new ArrayList<>();
						for (StockPrice sp : priceIter) {
							priceList.add(sp);
						}

						//
						if (priceList.size() < 251) {
							return null;
						}

						// 【性能核武器】使用原生 short 类型进行排序，彻底消除 TimeUtil 带来的对象开销
						Collections.sort(priceList, new Comparator<StockPrice>() {
							@Override
							public int compare(StockPrice p1, StockPrice p2) {
								if (p1.getYear() != p2.getYear()) return Integer.compare(p1.getYear(), p2.getYear());
								if (p1.getMonth() != p2.getMonth()) return Integer.compare(p1.getMonth(), p2.getMonth());
								return Integer.compare(p1.getDay(), p2.getDay());
							}
						});

						// 仅提取最后 251 天的收盘价
						List<StockPrice> last251StockPrices = priceList.subList(priceList.size() - 251, priceList.size());
						List<Double> last251ClosePrices = new ArrayList<>(251);
						for (StockPrice sp : last251StockPrices) {
							last251ClosePrices.add(sp.getClosePrice());
						}

						// 【短路优化】先算波动率，如果 >= 4 就直接掐断，省去后续所有计算和对象创建
						double volatility = Volitility.calculate(last251ClosePrices);
						if (volatility >= volatilityCeiling) {
							return null;
						}

						// 如果活到了这一步，才去算回报率 (这里已经修复为你正确的 251 天传参)
						double returns = Returns.calculate(5, last251ClosePrices);
						AssetFeatures features = new AssetFeatures();
						features.setAssetVolitility(volatility);
						features.setAssetReturn(returns);
						return features;
					}
				}
		);

		// 5. 统一清理 null 数据 (包含了不足 251 天的，以及波动率超标的资产)
		JavaPairRDD<String, AssetFeatures> validFeaturesRDD = featuresRDD.filter(
				new Function<Tuple2<String, AssetFeatures>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, AssetFeatures> tuple) throws Exception {
						return tuple._2() != null;
					}
				}
		);

		// 6. 关联元数据 (Join)
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> joinedRDD = validFeaturesRDD.join(assetMetadata);

		// 7. 过滤市盈率 (P/E Ratio)
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> finalFilteredRDD = joinedRDD.filter(
				new Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> tuple) throws Exception {
						double peRatio = tuple._2()._2().getPriceEarningRatio();
						if (peRatio == 0.0) return false;
						return peRatio < peRatioThreshold;
					}
				}
		);

		// 8. 映射为 Asset 对象
		JavaRDD<Asset> finalAssets = finalFilteredRDD.map(
				new Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Asset>() {
					@Override
					public Asset call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> tuple) throws Exception {
						String ticker = tuple._1();
						AssetFeatures features = tuple._2()._1();
						AssetMetadata meta = tuple._2()._2();
						features.setPeRatio(meta.getPriceEarningRatio());
						return new Asset(ticker, features, meta.getName(), meta.getIndustry(), meta.getSector());
					}
				}
		);

// 9. 去除异常值 NaN (双重保险)
		finalAssets = finalAssets.filter(new Function<Asset, Boolean>() {
			@Override
			public Boolean call(Asset asset) throws Exception {
				// 确保回报率和波动率都不是 NaN
				boolean returnIsNaN = Double.isNaN(asset.getFeatures().getAssetReturn());
				boolean volIsNaN = Double.isNaN(asset.getFeatures().getAssetVolitility());
				return !returnIsNaN && !volIsNaN;
			}
		});

		// 10. 排序并收集前 5 名 (关键修复：换回 takeOrdered 配合降序比较器)
		List<Asset> top5 = finalAssets.takeOrdered(5, new AssetReturnComparator());

		// 11. 组装结果
		AssetRanking finalRanking = new AssetRanking();
		Asset[] top5Array = new Asset[5];
		for (int i = 0; i < Math.min(top5.size(), 5); i++) {
			top5Array[i] = top5.get(i);
		}
		finalRanking.setAssetRanking(top5Array);

		return finalRanking;
	}

	// 强制使用降序比较器 (最高回报率在前)
	public static class AssetReturnComparator implements java.util.Comparator<Asset>, java.io.Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Asset a1, Asset a2) {
			// 注意：一定要是 a2.get... 相比 a1.get...
			return Double.compare(a2.getFeatures().getAssetReturn(), a1.getFeatures().getAssetReturn());
		}
	}
	
}

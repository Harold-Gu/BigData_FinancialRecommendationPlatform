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

		// 解析目标截止日期，使用 TimeUtil
		final Instant endDate = TimeUtil.fromDate(datasetEndDate);

		// 1. 将 Dataset<StockPrice> 转换为以股票代码 (Ticker) 为键的 JavaPairRDD
		JavaPairRDD<String, StockPrice> pairedPrices = prices.toJavaRDD().mapToPair(
				new PairFunction<StockPrice, String, StockPrice>() {
					@Override
					public Tuple2<String, StockPrice> call(StockPrice sp) throws Exception {
						return new Tuple2<>(sp.getStockTicker(), sp);
					}
				}
		);

		// 2. 按股票代码对价格记录进行分组
		JavaPairRDD<String, Iterable<StockPrice>> groupedPrices = pairedPrices.groupByKey();

		// 3. 计算技术指标 (波动率和回报率)
		JavaPairRDD<String, AssetFeatures> featuresRDD = groupedPrices.mapValues(
				new Function<Iterable<StockPrice>, AssetFeatures>() {
					@Override
					public AssetFeatures call(Iterable<StockPrice> priceIter) throws Exception {
						List<StockPrice> priceList = new ArrayList<>();

						// 过滤掉晚于 datasetEndDate 的数据点 [cite: 13]
						for (StockPrice sp : priceIter) {
							Instant priceDate = TimeUtil.fromDate(sp.getYear(), sp.getMonth(), sp.getDay());
							if (!priceDate.isAfter(endDate)) {
								priceList.add(sp);
							}
						}

						// 按时间升序排序
						Collections.sort(priceList, new Comparator<StockPrice>() {
							@Override
							public int compare(StockPrice p1, StockPrice p2) {
								Instant t1 = TimeUtil.fromDate(p1.getYear(), p1.getMonth(), p1.getDay());
								Instant t2 = TimeUtil.fromDate(p2.getYear(), p2.getMonth(), p2.getDay());
								return t1.compareTo(t2);
							}
						});

						// 波动率需要前一年的251天交易数据 [cite: 90]
						if (priceList.size() < 251) {
							return null; // 数据不足，稍后过滤掉
						}

						List<StockPrice> last251StockPrices = priceList.subList(priceList.size() - 251, priceList.size());
						List<Double> last251ClosePrices = new ArrayList<>();
						for (StockPrice sp : last251StockPrices) {
							last251ClosePrices.add(sp.getClosePrice());
						}


						// 计算指标
						// 计算指标
						double volatility = Volitility.calculate(last251ClosePrices);

						// 核心修复：直接将 last251ClosePrices 传给 Returns，让它自己根据天数 5 去寻找历史价格
						double returns = Returns.calculate(5, last251ClosePrices);

						// 封装到 AssetFeatures
						AssetFeatures features = new AssetFeatures();
						features.setAssetVolitility(volatility);
						features.setAssetReturn(returns);
						return features;
					}
				}
		);

		// 过滤掉因为数据量不足 (<251天) 而导致计算返回 null 的资产
		featuresRDD = featuresRDD.filter(
				new Function<Tuple2<String, AssetFeatures>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, AssetFeatures> tuple) throws Exception {
						return tuple._2() != null;
					}
				}
		);

		// 4. 初步筛选：移除波动率 >= 4 的资产 [cite: 19]
		JavaPairRDD<String, AssetFeatures> validVolatilityRDD = featuresRDD.filter(
				new Function<Tuple2<String, AssetFeatures>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, AssetFeatures> tuple) throws Exception {
						return tuple._2().getAssetVolitility() < volatilityCeiling;
					}
				}
		);

		// 5. 将技术特征与资产元数据关联 (Join)
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> joinedRDD = validVolatilityRDD.join(assetMetadata);

		// 6. 二次筛选：移除市盈率 (P/E Ratio) >= 25 的资产
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> finalFilteredRDD = joinedRDD.filter(
				new Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> tuple) throws Exception {
						AssetMetadata meta = tuple._2()._2();

						double peRatio = meta.getPriceEarningRatio();

						// 任务书要求：如果需要某个字段而该资产缺失该字段，则应将其过滤掉
						// 在 Java 中基础 double 缺失时的默认值是 0.0
						if (peRatio == 0.0) {
							return false;
						}
						return peRatio < peRatioThreshold;
					}
				}
		);

		// 7. 将筛选后的数据映射为最终的 Asset 对象
		JavaRDD<Asset> finalAssets = finalFilteredRDD.map(
				new Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Asset>() {
					@Override
					public Asset call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> tuple) throws Exception {
						String ticker = tuple._1();
						AssetFeatures features = tuple._2()._1();
						AssetMetadata meta = tuple._2()._2();

						// 为了保持数据完整，将 PE Ratio 写入 features
						features.setPeRatio(meta.getPriceEarningRatio());

						// 使用 Asset.java 提供的全参构造函数
						return new Asset(ticker, features, meta.getName(), meta.getIndustry(), meta.getSector());
					}
				}
		);

		// 8. 排序并收集前 5 名资产 (基于回报率降序排列)
		// 使用支持序列化的自定义比较器
		List<Asset> top5 = finalAssets.takeOrdered(5, new AssetReturnComparator());

		// 9. 将结果封装进 AssetRanking 并返回
		AssetRanking finalRanking = new AssetRanking(); // 构造函数默认初始化了长度为 5 的数组

		// 将 List 转换为固定长度为 5 的数组
		Asset[] top5Array = new Asset[5];
		for (int i = 0; i < Math.min(top5.size(), 5); i++) {
			top5Array[i] = top5.get(i);
		}

		// 注入到最终的 Ranking 对象中
		finalRanking.setAssetRanking(top5Array);
		return finalRanking;
	}

	public static class AssetReturnComparator implements java.util.Comparator<Asset>, java.io.Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Asset a1, Asset a2) {
			// a2 相比 a1 进行比较，实现降序排列 (最高回报率在前)
			return Double.compare(a2.getFeatures().getAssetReturn(), a1.getFeatures().getAssetReturn());
		}
	}
	
}

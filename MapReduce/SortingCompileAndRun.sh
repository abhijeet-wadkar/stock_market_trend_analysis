javac -cp `hadoop classpath` StockMarketAnalysisSorting.java
hadoop fs -rm -r /user/devendr1/output
jar cf sma.jar StockMarketAnalysis*.class CompositeKeyComparator.class NaturalKeyGroupingComparator.class NaturalKeyPartitioner.class StockKey.class
yarn jar sma.jar StockMarketAnalysisSorting IntermidiateSampleOutputSM.txt /user/devendr1/output

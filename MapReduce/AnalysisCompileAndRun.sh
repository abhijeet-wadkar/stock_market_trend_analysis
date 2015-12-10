javac -cp `hadoop classpath` StockMarketAnalysis.java
hadoop fs -rm -r /user/devendr1/output
jar cf sma.jar StockMarketAnalysis*.class
yarn jar sma.jar StockMarketAnalysis stockmarketdata/ /user/devendr1/output

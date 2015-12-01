<?xml version="1.0" encoding="ISO-8859-1" ?>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    
<%@ page import="java.io.BufferedReader" %>
<%@ page import="java.io.FileNotFoundException" %>
<%@ page import="java.io.FileReader" %>
<%@ page import="java.io.IOException" %>
<%@ page import="java.lang.NumberFormatException" %>
<%@ page import="java.text.*" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1" />
<title>TrueGen</title>
<head>
    <script type="text/javascript"
          src="https://www.google.com/jsapi?autoload={
            'modules':[{
              'name':'visualization',
              'version':'1',
              'packages':['corechart']
            }]
          }"></script>

    <script type="text/javascript">
    
      var closingPrices = [];
      var dates = [];
      var volumes = [];
      
	  <%
	  	String companyInputName = "GOOG";
		
		if((String)request.getAttribute("company1") != null){
			companyInputName = (String)request.getAttribute("company1");
		}
		if((String)request.getAttribute("company2") != null){
			companyInputName = (String)request.getAttribute("company2");
		}
		if((String)request.getAttribute("company3") != null){
			companyInputName = (String)request.getAttribute("company3");
		}
		if((String)request.getAttribute("company4") != null){
			companyInputName = (String)request.getAttribute("company4");
		}
		if((String)request.getAttribute("company5") != null){
			companyInputName = (String)request.getAttribute("company5");
		}
		if((String)request.getAttribute("userSelected") != null){
			companyInputName = (String)request.getAttribute("userSelected");
		}
		String prob_25 = null;
    	String prob_50 = null;
    	String prob_75 = null;
    	String prob_90 = null;
		
		String csvFile = "C:\\Users\\Bernie\\workspace\\WebApp_TruGen\\nasdaq\\" + companyInputName;
		csvFile = csvFile+".csv";
		
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		
		int[] volumes = new int[100];
		double[] closingPrices = new double[100];
		String[] dates = new String[100];
		
		double opening =0;
		double closing =0;
		double high =0;
		double low =0;
		
		int counter = 0;
		try {

			br = new BufferedReader(new FileReader(csvFile));
			
			while ((line = br.readLine()) != null && counter < 100) {
				if(counter > 0){
				        // use comma as separator
					String[] rawData = line.split(cvsSplitBy);
					if(counter == 1){
						try {
							opening = Double.parseDouble(rawData[1]);
							closing = Double.parseDouble(rawData[4]);
							high = Double.parseDouble(rawData[2]);
							low = Double.parseDouble(rawData[3]);
						}catch (NumberFormatException ex){
							continue;
						}
						
					}
					
					dates[counter] = rawData[0];
					%> 
					dates.push("<%out.print(dates[counter]);%>");
					<%
					try {
						
						closingPrices[counter] = Double.parseDouble(rawData[4]);
						volumes[counter] = Integer.parseInt(rawData[5]);
						%>
						closingPrices.push(<%out.print(closingPrices[counter]);%>);
						volumes.push(<%out.print(volumes[counter]);%>);
						<%
						}
						catch (NumberFormatException ex) {
						  System.out.println("------------------Number Format error");
						}
					
					System.out.println(counter+"  Date = "+ dates[counter]+"  closingPrice = "+closingPrices[counter]+"   volume ="+volumes[counter]);
				}
				counter++;

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		br = null;
		line = "";
		System.out.println("------------------------------------------------");
		String job_output = "C:\\Users\\Bernie\\workspace\\WebApp_TruGen\\recent_complete_output.txt";
		double score = 0.0;
		String percentIncrease= "";
		try {

			br = new BufferedReader(new FileReader(job_output));
			
			while ((line = br.readLine()) != null) {
				String[] rawData = line.split("[\\s]+");
				//System.out.println("array[0] = "+rawData[0] + "array[1]"+rawData[1]);
				if(rawData[0].startsWith(companyInputName)){
					System.out.println("Searching for = " +rawData[1]);
					line = br.readLine();
					String[] data = line.split("[\\s]+");
					percentIncrease = data[1];
					try{
						score = Double.parseDouble(rawData[1]);
					}catch (NumberFormatException ex){
						ex.printStackTrace();
					}
					break;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	  %>
	  google.load("visualization", "1", {packages:["corechart"]});
	  google.setOnLoadCallback(drawChart);
	  
      function drawChart() {
    	 // alert("In the drawChart Method "+closingPrices.length);
    	/*
        var data = google.visualization.arrayToDataTable([
          ['Year', 'Price', 'Volume'],
          ['2004',  1000,      400],
          ['2005',  1170,      460],
          ['2006',  660,       1120],
          ['2007',  1030,      540]
        ]);
		*/
		dates.reverse();
		closingPrices.reverse();
		volumes.reverse();
		
		var data = new google.visualization.DataTable();
		var volumeData = new google.visualization.DataTable();
		data.addColumn('date', 'date');
		data.addColumn('number', 'Closing Price'); 
		
		
		volumeData.addColumn('date', 'date');
		volumeData.addColumn('number', 'Trade Volume'); 
		
		
		for(var i = 0; i < dates.length; i++){
		    data.addRow( [ new Date( dates[i] ), closingPrices[i] ] );
		}	
		for(var i = 0; i < dates.length; i++){
		    volumeData.addRow( [ new Date(dates[i]) , volumes[i] ] );
		}
		
		var volumeOptions = {
		    title: "Trade Volume",
		    curveType: 'function',
		    colors:["red"],
		    legend: { position: 'bottom' }
		};
		
        var options = {
          title: "<%out.print(companyInputName);%> Price ($)",
          curveType: 'function',
          legend: { position: 'bottom' }
        };
        
       var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));
       chart.draw(data, options);
        
        var volumeChart = new google.visualization.LineChart(document.getElementById('volume_curve_chart'));
        volumeChart.draw(volumeData, volumeOptions);
        
        
        var data = google.visualization.arrayToDataTable([
    		                                                ['%Change', 'Probability'],
    		<%
    		                                      		// The name of the file to open.
    		                                     
    		                                            String fileName = "C:\\Users\\Bernie\\workspace\\WebApp_TruGen\\stock_output\\" + companyInputName + "-r-00000";

    		                                              // This will reference one line at a time
    		                                            line = null;
    		                                      		FileReader fileReader = null;

    		                                              try {
    		                                                  // FileReader reads text files in the default encoding.
    		                                                  fileReader = 
    		                                                      new FileReader(fileName);
    		                                                  
    		                                      			//Always wrap FileReader in BufferedReader.
    		                                                  BufferedReader bufferedReader = 
    		                                                      new BufferedReader(fileReader);

    		                                                  while((line = bufferedReader.readLine()) != null) {
    		                                                      String values[] = line.split("\\s+");
    		                                      				out.println("[" + values[1] + "," + values[0] + "],");				
    		                                      				if(prob_25 == null && Double.parseDouble(values[0]) > 0.25)
    		                                      				{
    		                                      					prob_25 = new String(values[1]);
    		                                      					//out.println("<<<<"+values[1]);
    		                                      				}
    		                                      				else if(prob_50 == null && Double.parseDouble(values[0]) > 0.50)
    		                                      				{
    		                                      					prob_50 = new String(values[1]);
    		                                      				}
    		                                      				else if(prob_75 == null && Double.parseDouble(values[0]) > 0.75)
    		                                      				{
    		                                      					prob_75 = new String(values[1]);
    		                                      				}
    		                                      				else if(prob_90 == null && Double.parseDouble(values[0]) > 0.90)
    		                                      				{
    		                                      					prob_90 = new String(values[1]);
    		                                      				}
    		                                      			}
    		                                      					
    		                                      			// Always close files.
    		                                      			bufferedReader.close();         
    		                                      		  }			
    		                                      		  catch(FileNotFoundException ex) {
    		                                                  out.println(ex);                
    		                                              }
    		                                      		  catch(IOException ex) {
    		                                                  System.out.println(
    		                                                      "Error reading file '" 
    		                                                      + fileName + "'");                  
    		                                                  // Or we could just do this: 
    		                                                  // ex.printStackTrace();
    		                                              }
    		                                      		//System.out.println("Finished file read and chart");
    		                                      		%>
    		                                              ]);
    													//alert("finished reading in file");
    		                                              var options = {
    		                                                title: '<%out.print(companyInputName);%>',
    		                                                hAxis: {title: '%Change'},
    		                                                vAxis: {title: 'Probability'},
    		                                                legend: 'none'
    		                                              };
  													
    		                                              var chart = new google.visualization.ScatterChart(document.getElementById('predict_div'));

    		                                              chart.draw(data, options);
        
        drawPrediction();
        
      }

     
    </script>
  </head>
</head>
<body>
	<table cellspacing="20">
		<tr>
			<td >
    			<table>
	    				<tr><td style="font-size: 25px; color: #ffffff;">Name: <%out.print(companyInputName);%> </td></tr>
	    				<tr><td style="color: #ffffff;">Open: <%out.print(opening);%></td></tr>
	    				<tr><td style="color: #ffffff;">High: <%out.print(high);%></td></tr>
	    				<tr><td style="color: #ffffff;">Low: <%out.print(low);%></td></tr>
	    				<tr><td style="color: #ffffff;">Close: <%out.print(closing);%></td></tr>
	    				<tr><td style="color: #ffffff;"><br></br> </td></tr>
	    				<tr><td style="font-size: 25px; color: #ffffff;">Analysis</td></tr>
	    				<tr><td style="color: #ffffff;">Score: <%out.print(score);%></td></tr>
	    				<tr><td style="color: #ffffff;">% Times Increased: <%out.print(percentIncrease);%> </td></tr>
	    				<tr><td><br></br></td></tr>
	    				<tr><td style="font-size: 25px; color: #FFFF99;">*Predicted</td></tr>
	    				<tr><td style="color: #ffffff;"><%out.println("Probability: 0.10, Change(in %): " + new DecimalFormat("#.##").format(Double.parseDouble(prob_90)));%></td></tr>
	    				<tr><td style="color: #ffffff;"><%out.println("Probability: 0.25, Change(in %): " + new DecimalFormat("#.##").format(Double.parseDouble(prob_75)));%></td></tr>
	    				<tr><td style="color: #ffffff;"><%out.println("Probability: 0.50, Change(in %): " + new DecimalFormat("#.##").format(Double.parseDouble(prob_50)));%></td></tr>	    				
	    				<tr><td style="color: #ffffff;"><%out.println("Probability: 0.75, Change(in %): " + new DecimalFormat("#.##").format(Double.parseDouble(prob_25)));%></td></tr>
	    				
    			</table>
    		</td>
    		<td>
    			<div id="curve_chart" style="width:700px; height: 200px"></div>
    			<div id="volume_curve_chart" style="width: 700px; height: 200px"></div>
    			<div id="predict_div" style="width: 700px; height: 200px"></div>
    		</td>
    	</tr>
    </table>
  </body>
  
  <script>
  	
  	
  	function drawPrediction(){

  	}
  
  </script>
  
</html>

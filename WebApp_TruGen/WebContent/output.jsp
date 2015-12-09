<%//This page displays the buttons for the pre-set companies %>
<?xml version="1.0" encoding="ISO-8859-1" ?>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">

<link href="generalDesign.css" rel="stylesheet" type="text/css">
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1" />
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
<head>
	
	<title>Insert title here</title>
</head>
	<body>
	
		<table align="center">
			<tr>
				<td style="padding:0 150px 0 100px;">
				<form action="servlettest" target="graphResults" method="get" name="graphFrame">
					<table>
						
						<tr class="companyRow">
							<td style="color: #ffffff;">1.<input type = "submit" value ="GOOG" name="company1" id="companyButton" class="active"></input></td>
						</tr>
						<tr class="companyRow">
							<td style="color: #ffffff;">2.<input type = "submit" value ="FB" name="company2" id="companyButton"></input></td>
						</tr>
						<tr class="companyRow">
							<td style="color: #ffffff;">3.<input type = "submit" value ="NFLX" name="company3" id="companyButton"></input></td>
						</tr>
						<tr class="companyRow">
							<td style="color: #ffffff;">4.<input type = "submit" value ="INTC" name="company4" id="companyButton"></input></td>
						</tr>
						<tr class="companyRow">
							<td style="color: #ffffff;">5.<input type = "submit" value ="CSCO" name="company5" id="companyButton"></input></td>
						</tr>
						<tr class="companyRow">
							<td><input type = "submit" value ="inputButton" name="userSelected" id="companyButton" style="display:none;"></input></td>
						</tr>
					</table>
				</form>
				</td>
				
				<td class="outputTable">
					<iframe name="graphResults" src="Results.jsp" frameborder="0" width="1050" height="680" align="right"></iframe>
				</td>
				
				
			</tr>
		</table>
		
				<%
				        //display user searched company
					String companyInputName = (String)request.getAttribute("companyInput");
					if(companyInputName != null && !companyInputName.equals("error")){
						%>
						<script>
								var companyName = "<%out.print(companyInputName);%>";
								$("[name='userSelected']").show();
								$("[name='userSelected']").prop('value', companyName);
								$('input[type=submit]').removeClass('active');
								$("[name='userSelected']").addClass('active');
								$("[name='userSelected']").click();
						</script>
						<%
					}
					
					if(companyInputName == "error"){
						%>
						<script>
								alert("Error: Company data not available");
						</script>
						<%
					}
					
				%>
		<script>
			$('input[type=submit]').click(function() {
				   $('input[type=submit]').removeClass('active');
				   $(this).addClass('active');
				});
			
			
		</script>
		
	</body>
</html>

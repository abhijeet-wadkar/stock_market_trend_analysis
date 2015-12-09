<%//This page is for displaying the header and the search bar, so the user can look-up comapny stock symobls%>
<?xml version="1.0" encoding="ISO-8859-1" ?>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<title>TruGen</title>
<head>

	<link href="generalDesign.css" rel="stylesheet" type="text/css">
	<div class="header" id="headDiv" align="right">
	
    	<table id="headerTable" align="right">
        	<tr >
            	<td>
                	<font color="#FFFFFF" size="+1" >Company Stock Analysis</font>
                </td>
           
                <td></td>
           
                <td>
	                <font color="#981B46" size="+4">Tru</font>
	                <font color="#FFF2F2" size="+4">Gen</font>	
                </td>
            </tr>
        </table>
        
    </div>
   
</head>
<body bgcolor="#4d4d4d" leftmargin="0" topmargin="0" marginwidth="0" marginheight="0">

	<br><br><br>
	<div align="center" id="searchDiv" class="searchDiv">
	    <form action="servlettest" target="outputFrame" method="get" name="searchForm">
	    
	        <input type="text" name="companyInput" min="0" max="100" id="textarea" placeholder="Search Company Symbol" required>
	        <input type = "submit" value = "Search" id="loginbutton" class="button" >
	        
	    </form>
	</div>
	<br><br>
	
	<iframe name="outputFrame" src="output.jsp" frameborder="0" width="1900" height="700" align="center"></iframe></td>

</body>
</html>

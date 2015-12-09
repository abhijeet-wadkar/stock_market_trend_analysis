/**
file: servletTest.java

This file takes in requests from POST/GET from the html side
and returns which comapny to display

Team: TruGen
 */
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ServletTest
 */
@WebServlet("/servlettest")
public class ServletTest extends HttpServlet {
	
	static String userCompanySearch;
	
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ServletTest() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#service(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//PrintWriter out = response.getWriter();
		userCompanySearch = (String)request.getParameter("companyInput");
		//this is if the user enters text search for a company
		if(userCompanySearch != null && userCompanySearch.length() > 0){
			if(validateInput(userCompanySearch)){
				request.setAttribute("companyInput", request.getParameter("companyInput"));
				getServletContext().getRequestDispatcher("/output.jsp").forward(request, response);
			}else{
				request.setAttribute("companyInput", "error");
				getServletContext().getRequestDispatcher("/output.jsp").forward(request, response);
			}
		}	
		//the following if/else(s) are see which pre-set compnay the user selected
		else if(request.getParameter("company1") != null && request.getParameter("company1").length() > 0){
			request.setAttribute("company1", request.getParameter("company1"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}
		else if(request.getParameter("company2") != null && request.getParameter("company2").length() > 0){
			request.setAttribute("company2", request.getParameter("company2"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}
		else if(request.getParameter("company3") != null && request.getParameter("company3").length() > 0){
			request.setAttribute("company3", request.getParameter("company3"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}
		else if(request.getParameter("company4") != null && request.getParameter("company4").length() > 0){
			request.setAttribute("company4", request.getParameter("company4"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}
		else if(request.getParameter("company5") != null && request.getParameter("company5").length() > 0){
			request.setAttribute("company5", request.getParameter("company5"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}
		else if(request.getParameter("userSelected") != null && request.getParameter("userSelected").length() > 0){
			request.setAttribute("userSelected", request.getParameter("userSelected"));
			getServletContext().getRequestDispatcher("/Results.jsp").forward(request, response);
		}else{
			getServletContext().getRequestDispatcher("/index.jsp").forward(request, response);
			return;
		}
	
	}
    /**
       validateinput()
       Returns true if the company exists. NOTE: were we able to run this on "maya" we would have searched the Hbase table
       This implementation justs searches for a file within a personal directory
     */
	public boolean validateInput(String input){
		String fullPath = "C:\\Users\\Bernie\\workspace\\WebApp_TruGen\\nasdaq\\" + input +".csv";
		File testFile = new File(fullPath);
		if(testFile.exists()){
			return true;
		}else{
			return false;
		}
	}
	
    

}

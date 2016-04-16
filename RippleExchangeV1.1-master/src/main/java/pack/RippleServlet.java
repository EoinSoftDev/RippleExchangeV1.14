package pack;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * Created by eoin on 15/04/16.
 */
@WebServlet("/RippleServlet")
public class RippleServlet extends HttpServlet {

    public RippleServlet() {
        super();
    }
   /* protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {


    }*/

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // Set response content type
        response.setContentType("text/html");

        // Actual logic goes here.
        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head>");
        out.println("<title>Ripple Servlet</title>");
        out.println("<meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\"></meta>\n" +
                "   <meta content=\"utf-8\" http-equiv=\"encoding\"></meta>");
        out.println("</head>");
        out.println("<body>");

        out.println("<h3>Welcome Fool</h3>");
        out.println("<p>Time : </p>");
        out.println("</body>");
        out.println("</html>");
    }
}

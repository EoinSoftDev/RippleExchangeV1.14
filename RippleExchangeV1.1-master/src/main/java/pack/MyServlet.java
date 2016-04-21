package pack;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


/**
 * Created by eoin on 20/04/16.
 */
@WebServlet("/myservlet")
public class MyServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String start = request.getParameter("sdate");
        String end = request.getParameter("edate");
        if (start != null && end != null) {
            RippleMain runThis = new RippleMain();
            RippleMain.setDates(start, end);
            String[] args = null;
            RippleMain.main(args);
        }

        request.getRequestDispatcher("/Rippled/WEB-INF/Rippled.jsp").forward(request, response);
    }

}

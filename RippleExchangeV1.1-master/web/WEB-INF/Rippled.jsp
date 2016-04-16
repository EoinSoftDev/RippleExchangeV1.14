<%--
http://localhost:8080/Rippled/WEB-INF/Rippled.jsp
  Created by IntelliJ IDEA.
  User: eoin
  Date: 15/04/16
  Time: 20:25
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <script>d3.json('/home/eoin/Documents/Intellij Projects/sample.json', function (data) {
    })
    /*
     d3.json('data/ufo-sightings.json', function(data) {
     data = MG.convert.date(data, 'year');
     })*/
    d3.json('/home/eoin/Documents/Intellij Projects/sample.json', function (data) {
        MG.data_graphic({
            title: "Payments",
            description: "projection",
            data: data,
            width: 650,
            height: 150,
            target: '#ufo-sightings',
            x_accessor: 'year',
            y_accessor: 'sightings',
            markers: [{'year': 1964, 'label': '"The Creeping Terror" released'}]
        })
    })
    </script>
    <title>Rippled</title>

</head>
<body>
<%= "Hello World!" %>
<h3>Welcome Fool</h3>
</body>
</html>

<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ attribute name="title" %>

<fmt:setBundle basename="resourcebundles.Main"/>
<fmt:message key="title" var="defaultTitle"/>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
    <head>
        <title><c:out value="${title}" default="${defaultTitle}"/></title>
        <%--<link rel="shortcut icon" href="<c:url value="/static/images/favicon.ico"/>">--%>
        <%--<link rel="shortcut icon" type="image/png" href="<c:url value="/static/images/favicon.png"/>">--%>

        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">

        <link rel="stylesheet" type="text/css" href="<c:url value="/wro/default.css"/>" />
        <script type="text/javascript" src="<c:url value="/wro/default.js" />"></script>

    </head>
    <body class="yui-skin-sam">
        <div id="container">
            <div id="header">
                <div id="user_greeting">
                    <strong>Welcome</strong>
                </div>
            </div>
            <div id="content_container">
                <div id="nav_container">
                    <div id="nav">
                        <ul>
                            <li><a href="<c:url value="/" />">Home</a></li>
                            <li><a href="<c:url value="/search" />">Search</a></li>
                        </ul>
                    </div>
                </div>
                <div id="content">
                    <jsp:doBody />
                </div>
            </div>
        </div>
        <div style="clear:both"></div>
        <div id="footer_wrap">
            <div id="footer_container">
                <div id="footer">
                    <div id="copy"><fmt:message key="copyright" /></div>
                </div>
            </div>
        </div>
    </div>
        <script type="text/javascript">
        (function() {

        })();
    </script>
    </body>
</html>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ attribute name="title" %>

<fmt:setBundle basename="resourcebundles.Main"/>
<fmt:message key="title" var="defaultTitle"/>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<% request.getSession().invalidate(); %>

    <head>
        <title><c:out value="Gates Big Data Project" default="${defaultTitle}"/></title>
        <%--<link rel="shortcut icon" href="<c:url value="/static/images/favicon.ico"/>">--%>
        <%--<link rel="shortcut icon" type="image/png" href="<c:url value="/static/images/favicon.png"/>">--%>

        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=9" >

        <!--<link rel="stylesheet" type="text/css" href="<c:url value="/wro/default.css"/>" />-->
        <!--<script type="text/javascript" src="<c:url value="/wro/default.js" />"></script>-->
        <link rel="stylesheet" type="text/css" href="${pageContext.request.contextPath}/wro/default.css" />
        <script type="text/javascript" src="${pageContext.request.contextPath}/wro/default.js"></script>


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
                    <div id="nav" class="yuimenu">
                        <div class="bd">
                            <ul class="first-of-type">
                                <li class="yuimenuitem">
                                    <a class="yuimenuitemlabel" style="padding:0" href="<c:url value="/" />">Home</a>
                                </li>
                                <li class="yuimenuitem">Search</li>
                            </ul>
                        </div>
                    </div>
                </div>
                <div class="clearboth"></div>
                <div id="facet_container">
                    <a class = "button down-big-overlay" id = "show_overlay">Narrow your search</a>
                </div>

                <div id="overlay" style="visibility:hidden">
                    <div id="tree_view" class="bd"></div>
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

        <script type="text/javascript">
        (function() {
            var Menu = YAHOO.widget.Menu, Connect = YAHOO.util.Connect, Json = YAHOO.lang.JSON;

            var itemData = [];
            var navMenu = new Menu("nav", {
                position: "static",
                hidedelay:  750,
                lazyload: false });

            Connect.asyncRequest('GET', '<c:url value="/solr/corenames" />' , {
                success : function(o) {
                    var response = Json.parse(o.responseText);
                    var cores = response['cores'];

                    for(var i = 0; i < cores.length; i++) {
                        itemData.push( {text: cores[i]['title'], url: "<c:url value="/search/" />" + cores[i]['name'] });
                    }

                    navMenu.subscribe("beforeRender", function () {
                        if (this.getRoot() == this) {
                            this.getItem(1).cfg.setProperty("submenu", { id: "search", itemdata: itemData });
                        }
                    });
                    navMenu.render();
                },
                failure : function (o) {
                    alert("Could not retrieve core information.");
                }
            });
        })();
    </script>
    </body>
</html>
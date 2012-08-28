<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <h1>Document view</h1>

    <div id="documentJson"></div>

    <script type="text/javascript">
    (function() {

        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON;

        var remoteFile = YAHOO.deconcept.util.getRequestParameter("hdfs");

        Connect.asyncRequest('GET', '<c:url value="/data/read?remotefile=" />' + remoteFile, {
            success : function(o) {
                var i, result = Json.parse(o.responseText);

                Dom.get("documentJson").innerHTML = "<code class=\"prettyprint\">" +
                        LWA.util.formatJson(JSON.stringify(result)) + "</code>";
            },
            failure : function(o) {
                alert("Could not retrieve collection names.");
            }
        });

    })();
    </script>
</layout:main>
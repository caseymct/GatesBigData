<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/js/yui/2.9/treeview/assets/skins/sam/menu.css"/>" />
    <style>
        .ygtvitem {
            color: #708090;
        }
    </style>
    <h1>Document view</h1>

    <div id="tree_view"></div>
    <div id="doc_view"></div>

    <script type="text/javascript">
    (function() {

        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,
            TreeView = YAHOO.widget.TreeView,
            Json = YAHOO.lang.JSON;

        var treeView = new TreeView("tree_view");

        var remoteSeg = YAHOO.deconcept.util.getRequestParameter("segment");
        var remoteFile = YAHOO.deconcept.util.getRequestParameter("file");
        var urlParams = "?segment=" + remoteSeg + "&file=" + remoteFile;

        Connect.asyncRequest('GET', '<c:url value="/data/nutch" />' + urlParams, {
            success : function(o) {
                var i, result = o.responseText;
                debugger;
                if (LWA.util.isValidJSON(result)) {
                    LWA.ui.buildTreeViewFromJson(Json.parse(result), treeView);
                } else {
                    Dom.get("doc_view").innerHTML = o.responseText;
                }
            },
            failure : function(o) {
                alert("Could not retrieve collection names.");
            }
        });

    })();
    </script>
</layout:main>
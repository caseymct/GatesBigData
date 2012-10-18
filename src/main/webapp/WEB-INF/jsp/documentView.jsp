<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/js/yui/2.9/treeview/assets/skins/sam/menu.css"/>" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />
    <style type="text/css">
        .ygtvitem {
            color: #708090;
        }
    </style>

    <h1>Document view</h1>
    <div id = "view_buttongroup" class = "yui-buttongroup search-button-style">
        <input id="preview" type="radio" name="view" value="preview" >
        <input id="fullview" type="radio" name="view" value="fullview">
    </div>
    <div class="clearboth"></div>

    <!--<div id="tree_view"></div>-->
    <div id="doc_view"></div>

    <script type="text/javascript" src="<c:url value="/static/js/search.js" />"></script>

    <script type="text/javascript">
    (function() {

        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,
            TreeView = YAHOO.widget.TreeView,
            ButtonGroup = YAHOO.widget.ButtonGroup,
            Json = YAHOO.lang.JSON;

        SEARCH.ui.changeShowOverlayButtonVisibility(false);

        var remoteSeg = YAHOO.deconcept.util.getRequestParameter("segment");
        var remoteFile = YAHOO.deconcept.util.getRequestParameter("file");
        var coreName = YAHOO.deconcept.util.getRequestParameter("core");
        var viewType = YAHOO.deconcept.util.getRequestParameter("view");
        var urlParams = "?core=" + coreName + "&segment=" + remoteSeg + "&file=" + remoteFile + "&view=" + viewType;

        var viewButtonGroup = new ButtonGroup("view_buttongroup");
        viewButtonGroup.check(+(viewType == "fullview"));
        viewButtonGroup.on("checkedButtonChange", function (o) {
            urlParams = urlParams.replace(/view=.*view/, "view=" + o.newValue.get("value"));
            window.open('<c:url value="/core/document/view" />' + urlParams, "_self");
        });

        LWA.ui.initWait();
        LWA.ui.showWait();

        Connect.asyncRequest('GET', '<c:url value="/document/nutch/get" />' + urlParams, {
            success : function(o) {
                var result = o.responseText;
                LWA.ui.hideWait();
                console.log(result);
                if (LWA.util.isValidJSON(result)) {
                    LWA.ui.buildTreeViewFromJson(Json.parse(result), new TreeView("doc_view"));
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
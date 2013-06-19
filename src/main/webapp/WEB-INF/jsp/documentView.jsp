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

    <script type="text/javascript">
    (function() {

        var Dom     = YAHOO.util.Dom,       Event       = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,   TreeView    = YAHOO.widget.TreeView,
            Json    = YAHOO.lang.JSON,      ButtonGroup = YAHOO.widget.ButtonGroup;

        var docViewElId        = "doc_view",            contentEl              = Dom.get(UI.CONTENT_EL_NAME),
            viewButtonGrpDivId = "view_buttongroup",    viewButtonGrpCSSClass  = "yui-buttongroup search-button-style",
            previewButtonId    = "preview",             previewButtonIdValue   = "preview",
            fullviewButtonId   = "fullview",            fullviewButtonIdValue  = "fullview",
            auditviewButtonId  = "auditview",           auditviewButtonIdValue = "auditview",
            viewButtonGrpName  = "view",                saveButtonElId         = "save",
            saveButtonCSSClass = "button save";

        var baseUrl         = '<c:url value="/" />',
            saveDocUrl      = baseUrl + 'document/save',
            getDocUrl       = baseUrl + 'document/content/get',
            coreDocViewUrl  = baseUrl + '/core/document/view';

        var id              = YAHOO.deconcept.util.getRequestParameter("id"),
            coreName        = YAHOO.deconcept.util.getRequestParameter("core"),
            viewType        = YAHOO.deconcept.util.getRequestParameter("view"),
            urlParams       = "?core=" + coreName + "&id=" + id + "&view=" + viewType,
            saveDoc         = saveDocUrl + urlParams;

        buildHtml();

        UI.initWait(baseUrl);
        UI.showWait();

        Connect.asyncRequest('GET', getDocUrl + urlParams, {
            success : function(o) {
                var result = o.responseText;
                UI.hideWait();

                if (UI.util.isValidJSON(result)) {
                    UI.buildTreeViewFromJson(Json.parse(result), new TreeView(docViewElId));
                } else {
                    Dom.get(docViewElId).innerHTML = o.responseText;
                }
            },
            failure : function(o) {
                alert("Could not retrieve collection names.");
            }
        });

        function buildHtml() {
            UI.addDomElementChild('h2', contentEl, { innerHTML : 'Document view' });

            var bg = UI.addDomElementChild('div', contentEl, { id : viewButtonGrpDivId }, { "class" : viewButtonGrpCSSClass });
            UI.addDomElementChild('input', bg, { id : previewButtonId,  "type" : "radio",  name : viewButtonGrpName, value : previewButtonIdValue });
            UI.addDomElementChild('input', bg, { id : fullviewButtonId, "type" : "radio",  name : viewButtonGrpName, value : fullviewButtonIdValue });
            UI.addDomElementChild('input', bg, { id : auditviewButtonId, "type" : "radio", name : viewButtonGrpName, value : auditviewButtonIdValue });

            UI.addDomElementChild('a', contentEl, { id : saveButtonElId },
                    { "class" : saveButtonCSSClass, float : "left", "margin-top" : "9px", "margin-left" : "10px" });

            UI.addClearBothDiv(contentEl);
            UI.addDomElementChild('div', contentEl, { id : docViewElId }, { "padding-top" : "10px" });

            var viewButtonGroup   = new ButtonGroup(viewButtonGrpDivId);
            switch (viewType) {
                case fullviewButtonIdValue  : viewButtonGroup.check(1); break;
                case auditviewButtonIdValue : viewButtonGroup.check(2); break;
                default                     : viewButtonGroup.check(0); break;
            }

            viewButtonGroup.on("checkedButtonChange", function (o) {
                urlParams = urlParams.replace(/view=.*view/, "view=" + o.newValue.get("value"));
                window.open(coreDocViewUrl + urlParams, "_self");
            });

            Event.addListener(saveButtonElId, "click", function(e) {
                window.open(saveDoc);
            });
        }
    })();
    </script>
</layout:main>
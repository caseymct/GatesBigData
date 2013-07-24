<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/js/yui/2.9/treeview/assets/skins/sam/menu.css"/>" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/index.css"/>" />

    <h2>Solr Cores</h2>

    <div id="solr_cores"></div>

    <script type="text/javascript">
    (function() {
        var Dom     = YAHOO.util.Dom,        Event        = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,    Json         = YAHOO.lang.JSON,
            Overlay = YAHOO.widget.Overlay,  SimpleDialog = YAHOO.widget.SimpleDialog;

        var REINDEX = 0, EMPTY = 1, THUMB = 2;
        
        var solrCoresDiv              = Dom.get("solr_cores"),          dlgCSSClass               = "yui-pe-content",
            overlayClearBothDomElName = "overlay_clearboth",            contentContainerDivElName = "content_container",
            buttonDownOverlayCSSClass = "button down-big-overlay",      buttonUpOverlayCSSClass   = "button up-big-overlay",
            reindexPrefix             = "reindex_",                     emptyPrefix               = "empty_",
            thumbPrefix               = "thumb_",                       dlgDivSubstr              = "dlg_div_",
            reindexDlgDivPrefix       = reindexPrefix + dlgDivSubstr,   emptyDlgDivPrefix         = emptyPrefix + dlgDivSubstr,
            thumbDlgDivPrefix         = thumbPrefix + dlgDivSubstr,     contentContainer          = Dom.get(contentContainerDivElName),
            structuredDataKey         = 'structuredData';

        var collectionData = {},
            collectionNames = [];

        function getOverlayName(coreName)           { return "overlay_" + coreName; }
        function getOverlayShowButtonName(coreName) { return "show_overlay_" + coreName; }
        function getButtonId(coreName, buttonFn) {
            if (buttonFn == REINDEX)  return reindexPrefix + coreName;
            if (buttonFn == EMPTY)    return emptyPrefix + coreName;
            if (buttonFn == THUMB)    return thumbPrefix + coreName;
            return coreName;
        }
        function getDlgDivId(coreName, buttonFn) { 
            if (buttonFn == REINDEX)  return reindexDlgDivPrefix + coreName; 
            if (buttonFn == EMPTY)    return emptyDlgDivPrefix + coreName;
            if (buttonFn == THUMB)    return thumbDlgDivPrefix + coreName;
            return coreName;
        }
        function getCoreNameFromDlgDivId(id, buttonFn) {
            if (buttonFn == REINDEX)  return id.substring(reindexDlgDivPrefix.length);
            if (buttonFn == EMPTY)    return id.substring(emptyDlgDivPrefix.length);
            if (buttonFn == THUMB)    return id.substring(thumbDlgDivPrefix.length);
            return id;
        }

        function getNThreadsInputElNameFromDivId(id) { return id + "_nthreads"; }
        function getNFilesInputElNameFromDivId(id)   { return id + "_nfiles"; }

        function ignoreKey(key) {
            return key.match(/instanceDir|dataDir|index.directory|config|schema/) != null;
        }

        function recursiveGetKeys(coreInfo, parentStr, allDataStrings) {
            var i, keys = Object.keys(coreInfo);

            for(i = 0; i < keys.length; i++) {
                var key = keys[i];
                var fullKeyName = (parentStr == "") ? key : parentStr + "." + key;
                if (coreInfo[key] instanceof Object) {
                    allDataStrings = recursiveGetKeys(coreInfo[key], fullKeyName, allDataStrings);
                } else if (!ignoreKey(fullKeyName)) {
                    allDataStrings.push(fullKeyName + ": " + coreInfo[key]);
                }
            }
            return allDataStrings;
        }

        function empty() {
            var coreName = getCoreNameFromDlgDivId(this.id, EMPTY);
            Connect.asyncRequest('GET', '<c:url value="/collection/empty" />' + "?collection=" + coreName, {
                success: function(o) {
                    alert("Emptying solr core.");
                },
                failure: function(o) {
                    alert("Could not connect to empty solr core.");
                }
            });
            this.hide();
        }

        function buildOverlayHTML(parentNode, name) {
            var i, d, structured = collectionData[name][structuredDataKey];

            d = UI.addDomElementChild('div', parentNode, null, { id: name + "_info_div", float: "left", width: "100%"});
            UI.addDomElementChild('p', d, { id: name + "_ops_div", innerHTML: "Operations: "},
                    { "font-weight" : "bold", float: "left", width: "100%", 'margin-top' : '10px' });

            UI.addDomElementChild('input', d, { id: getButtonId(name, EMPTY), type: "button", value: "Empty Index"},
                    { padding: "5px", "margin-left": "10px" });

            if (structured == 'false') {
                UI.addDomElementChild('input', d, { id: getButtonId(name, THUMB), type: "button", value: "Create Thumbnails"},
                    { padding: "5px", "margin-left": "10px" });
            }

            var s = collectionData[name]['shards'];
            d = UI.addDomElementChild('div', parentNode, null, { float: "left", width: "100%", 'margin-top' : '30px' });
            UI.addDomElementChild('p', d, { innerHTML: "Collection information: " }, { float: "left", width: "100%", 'font-weight' : 'bold' });
            UI.addDomElementChild('p', d, { innerHTML: UI.util.jsonSyntaxHighlight(s)}, { float: "left", width: "340px" });

            UI.insertDomElementAfter('div', parentNode, { id: getDlgDivId(name, EMPTY)});
            var emptyDlg = UI.createSimpleDlg(getDlgDivId(name, EMPTY), "Confirm", "Empty core " + name + "?", empty);
            Event.addListener(getButtonId(name, EMPTY), "click", function(e) { emptyDlg.show(); });

            if (structured == 'false') {
                UI.insertDomElementAfter('div', parentNode, { id: getDlgDivId(name, THUMB)});
                var thumbDlg = UI.createSimpleDlg(getDlgDivId(name, THUMB), "Confirm", "Create thumbnails for core " + name + "?", createThumbnail);
                Event.addListener(getButtonId(name, THUMB), "click", function(e) { thumbDlg.show(); });
            }
        }

        // edit core page? http://localhost:8080/LucidWorksApp/core/empty?core=collection1
        Connect.asyncRequest('GET', '<c:url value="/solr/info/all" />' , {
            success : function(o) {
                collectionData = Json.parse(o.responseText);
                collectionNames = Object.keys(collectionData);

                for(var i = 0; i < collectionNames.length; i++) {
                    var name = collectionNames[i];
                    var d = UI.addDomElementChild('div_' + name, solrCoresDiv, {}, { float: "left", width: "100%", "padding-bottom" : "30px" });
                    UI.addDomElementChild('a', d, {id: getOverlayShowButtonName(name), innerHTML: "Show Core " + name},
                            {class : buttonDownOverlayCSSClass} );
                    var overlay = UI.addDomElementChild('div', d, { id: getOverlayName(name)},
                            { visibility: "hidden", background:"white", border: "1px solid gray", "border-radius": "2px", padding: "5px"});
                    UI.addDomElementChild('div', d, null, {class: "clearboth"});

                    buildOverlayHTML(overlay, name);
                }
                UI.addDomElementChild('div', solrCoresDiv, { id: overlayClearBothDomElName }, { class: "clearboth"});
            },
            failure : function (o) {
                alert("Could not retrieve core information.");
            }
        });

        Event.onContentReady(overlayClearBothDomElName, function() {
            for(var i = 0; i < collectionNames.length; i++) {
                var name = collectionNames[i],
                    overlayDivElName = getOverlayName(name),
                    showOverlayButtonElName = getOverlayShowButtonName(name);

                var overlay = new Overlay(overlayDivElName, {
                    context: [showOverlayButtonElName, "tl","bl", ["beforeShow", "windowResize"]],
                    visible: false
                });
                overlay.render(contentContainer);

                Event.addListener(showOverlayButtonElName, "click", function(e) {
                    var overlay = this[0];
                    var showOverlayButtonElName = this[1];
                    if (Dom.hasClass(showOverlayButtonElName, buttonDownOverlayCSSClass)) {
                        UI.showOverlayWithButton(overlay, showOverlayButtonElName, buttonDownOverlayCSSClass, buttonUpOverlayCSSClass);
                    } else {
                        UI.hideOverlayWithButton(overlay, showOverlayButtonElName, buttonDownOverlayCSSClass, buttonUpOverlayCSSClass);
                    }
                }, [overlay, showOverlayButtonElName], true);
            }

        });

    })();
    </script>
</layout:main>
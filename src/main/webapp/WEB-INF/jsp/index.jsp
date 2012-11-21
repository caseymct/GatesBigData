<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/js/yui/2.9/treeview/assets/skins/sam/menu.css"/>" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/index.css"/>" />

    <h2>Solr Cores</h2>

    <div id="solr_cores"></div>

    <!--
    <form id="create_core_form">
        <div id="create_new_core_div">
            <h4 id="create_new_core_header">Create new core</h4>
            <div class="index_row">
                <label for="new_core_name" id = "new_core_name_label" class="index_label"><span class="red">*</span>Name: </label>
                <input id="new_core_name" type="text" class="index_input"/>
            </div>
            <div class="index_row">
                <label for="instance_dir" id = "instance_dir_label" class="index_label">Instance directory: </label>
                <input id="instance_dir" type="text" class="index_input" value="solr/{name}"/>
            </div>
            <div class="index_row">
                <label for="data_dir" id = "data_dir_label" class="index_label">Data directory: </label>
                <input id="data_dir" type="text" class="index_input" value="solr/{name}/data"/>
            </div>
            <div class="index_row">
                <label for="config_file_name" id = "config_file_name_label" class="index_label">Config file: </label>
                <input id="config_file_name" type="text" value="solrconfig.xml" class="index_input"/>
            </div>
            <div class="index_row">
                <label for="schema_file_name" id = "schema_file_name_label" class="index_label">Schema file: </label>
                <input id="schema_file_name" type="text" value="schema.xml" class="index_input"/>
            </div>

            <div class="buttons index_buttons">
                <a href="#" class="button small" id="create">Create</a>
            </div>
            <div class = "index_row"></div>
        </div>
    </form> -->

    <script type="text/javascript">
    (function() {
        var     Dom = YAHOO.util.Dom,              Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,           Json = YAHOO.lang.JSON,
           Overlay = YAHOO.widget.Overlay,  SimpleDialog = YAHOO.widget.SimpleDialog;

        var REINDEX = 0, EMPTY = 1, THUMB = 2;
        
        var solrCoresDiv              = Dom.get("solr_cores"),
            coreData                  = "",
            dlgCSSClass               = "yui-pe-content",
            overlayClearBothDomElName = "overlay_clearboth",
            contentContainerDivElName = "content_container",
            buttonDownOverlayCSSClass = "button down-big-overlay",
            buttonUpOverlayCSSClass   = "button up-big-overlay",
            reindexPrefix             = "reindex_",
            emptyPrefix               = "empty_",
            thumbPrefix               = "thumb_",
            dlgDivSubstr              = "dlg_div_",
            reindexDlgDivPrefix       = reindexPrefix + dlgDivSubstr,
            emptyDlgDivPrefix         = emptyPrefix + dlgDivSubstr,
            thumbDlgDivPrefix         = thumbPrefix + dlgDivSubstr,
            contentContainer          = Dom.get(contentContainerDivElName);

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


        function reindex()  {
            var coreName = getCoreNameFromDlgDivId(this.id, REINDEX);
            Connect.asyncRequest('GET', '<c:url value="/core/reindex" />' + "?core=" + coreName, {
                success: function(o) {
                    alert("Re-indexing solr core.....");
                },
                failure: function(o) {
                    alert("Could not connect to re-index solr core.");
                }
            });
            this.hide();
        }

        function empty() {
            var coreName = getCoreNameFromDlgDivId(this.id, EMPTY);
            Connect.asyncRequest('GET', '<c:url value="/core/empty" />' + "?core=" + coreName, {
                success: function(o) {
                    alert("Emptying solr core.");
                },
                failure: function(o) {
                    alert("Could not connect to empty solr core.");
                }
            });
            this.hide();
        }

        function createThumbnail() {
            var coreName = getCoreNameFromDlgDivId(this.id, THUMB);
            Connect.asyncRequest('GET', '<c:url value="/data/thumbnails" />' + "?core=" + coreName, {
                success: function(o) {
                    alert("Creating thumbnails....");
                },
                failure: function(o) {
                    alert("Could not create thumbnails.");
                }
            });
            this.hide();
        }

        function buildReindexDlgHtml(siblingElName, name) {
            var sibling = Dom.get(siblingElName);
            var divid = getDlgDivId(name, REINDEX);
            var d = UI.insertDomElementAfter('div', sibling, {id : divid }, {class : dlgCSSClass });
            UI.addDomElementChild('div', d, null, {class : "hd"});
            var bd = UI.addDomElementChild('div', d, null, {class : "bd"});
            UI.addDomElementChild('label', bd, { for: getNThreadsInputElNameFromDivId(divid), innerHTML: "Threads:"});
            UI.addDomElementChild('input', bd, { id : getNThreadsInputElNameFromDivId(divid), type: "text", value: "10"}, { width: "100px"});
            UI.addDomElementChild('label', bd, { for: getNFilesInputElNameFromDivId(divid), innerHTML: "Files processed/thread:"});
            UI.addDomElementChild('input', bd, { id : getNFilesInputElNameFromDivId(divid), type: "text", value: "100"}, { width: "100px"});

        }

        function buildOverlayHTML(parentNode, coreInfo) {
            var i, d, allDataStrings = recursiveGetKeys(coreInfo, "", []),
                name = coreInfo.name;

            for(i = 0; i < allDataStrings.length; i+=2) {
                d = UI.addDomElementChild('div', parentNode, null, { float: "left", width: "100%" });
                UI.addDomElementChild('p', d, { innerHTML: allDataStrings[i++]}, { float: "left", width: "340px" });
                if (i < allDataStrings.length) UI.addDomElementChild('p', d, { innerHTML: allDataStrings[i]}, { float: "left" });
            }

            d = UI.addDomElementChild('div', parentNode, null, { id: name + "_info_div", float: "left", width: "100%", "padding-top": "30px" });
            UI.addDomElementChild('p', d, { id: name + "_ops_div", innerHTML: "Operations: "}, { "font-weight" : "bold", float: "left", width: "100%" });

            UI.addDomElementChild('input', d, { id: getButtonId(name, REINDEX), type: "button", value: "Reindex Solr Core from HDFS"}, { padding: "5px" });
            UI.addDomElementChild('input', d, { id: getButtonId(name, EMPTY), type: "button", value: "Empty Index"}, { padding: "5px", "margin-left": "10px" });
            UI.addDomElementChild('input', d, { id: getButtonId(name, THUMB), type: "button", value: "Create Thumbnails"}, { padding: "5px", "margin-left": "10px" });

            buildReindexDlgHtml(parentNode, name);
            //UI.insertDomElementAfter('div', parentNode, { id: getDlgDivId(name, REINDEX)});
            var reindexDlg = UI.createSimpleDlg(getDlgDivId(name, REINDEX), "Confirm", "Reindex core " + name + "?", reindex);
            Event.addListener(getButtonId(name, REINDEX), "click", function(e) { reindexDlg.show(); });

            UI.insertDomElementAfter('div', parentNode, { id: getDlgDivId(name, EMPTY)});
            var emptyDlg = UI.createSimpleDlg(getDlgDivId(name, EMPTY), "Confirm", "Empty core " + name + "?", empty);
            Event.addListener(getButtonId(name, EMPTY), "click", function(e) { emptyDlg.show(); });

            UI.insertDomElementAfter('div', parentNode, { id: getDlgDivId(name, THUMB)});
            var thumbDlg = UI.createSimpleDlg(getDlgDivId(name, THUMB), "Confirm", "Create thumbnails for core " + name + "?", createThumbnail);
            Event.addListener(getButtonId(name, THUMB), "click", function(e) { thumbDlg.show(); });
        }

        // edit core page? http://localhost:8080/LucidWorksApp/core/empty?core=collection1
        Connect.asyncRequest('GET', '<c:url value="/solr/info/all" />' , {
            success : function(o) {
                coreData = Json.parse(o.responseText);

                for(var i = 0; i < coreData.length; i++) {
                    var name = coreData[i].name;
                    var d = UI.addDomElementChild('div_' + name, solrCoresDiv, {}, { float: "left", width: "100%", "padding-bottom" : "30px" });
                    UI.addDomElementChild('a', d, {id: getOverlayShowButtonName(name), innerHTML: "Show Core " + name},
                            {class : buttonDownOverlayCSSClass} );
                    var overlay = UI.addDomElementChild('div', d, { id: getOverlayName(name)},
                            { visibility: "hidden", background:"white", border: "1px solid gray", "border-radius": "2px", padding: "5px"});
                    UI.addDomElementChild('div', d, null, {class: "clearboth"});

                    buildOverlayHTML(overlay, coreData[i]);
                }
                UI.addDomElementChild('div', solrCoresDiv, { id: overlayClearBothDomElName}, {class: "clearboth"});
            },
            failure : function (o) {
                alert("Could not retrieve core information.");
            }
        });

        Event.onContentReady(overlayClearBothDomElName, function() {
            for(var i = 0; i < coreData.length; i++) {
                var name = coreData[i].name,
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

        Event.addListener("create", "click", function(e) {
            Event.stopEvent(e);

            var newcoreinfo =  { "corename" : Dom.get("new_core_name").value };

            Connect.initHeader('Content-Type', 'application/json');
            Connect.setDefaultPostHeader('application/json');
            Connect.asyncRequest('POST', '<c:url value="/core/create" />' , {
                success: function (o) {
                    if (!UI.alertErrors(o)) {
                        window.location.reload();
                    }
                },
                failure: function (e) {
                    alert("Could not create new collection");
                }
            }, YAHOO.lang.JSON.stringify(newcoreinfo));
        });

    })();
    </script>
</layout:main>
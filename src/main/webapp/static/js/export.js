var EXPORT = {};
EXPORT.ui = {};
EXPORT.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,           Json = YAHOO.lang.JSON,
        Event           = YAHOO.util.Event,          Overlay = YAHOO.widget.Overlay,
        Dom             = YAHOO.util.Dom;

    var exportDialogElName      = "export_dialog",
        exportDialogCSSClass    = "yui-pe-content",
        exportInputFileElName   = "export_file_name";

    function exportFile(type, dlg) {
        window.open(exportUrl + "?type=" + type + "&file=" + Dom.get(exportFileDomElName).value);
        window.focus();
        dlg.hide();
    }

    /* Export dialog code */
    Event.onContentReady(exportDialogElName, function() {
        function exportFile(type, dlg) {
            window.open(exportUrl + "?type=" + type + "&file=" + Dom.get(exportDialogElName).value);
            window.focus();
            dlg.hide();
        }
        function handleCSVExport()  { exportFile("csv", this); }
        function handleJSONExport() { exportFile("json", this); }
        function hideDlg()          { this.hide(); }

        var exportDlg = new SimpleDialog("exportDialog", {
            width: "20em",
            fixedcenter: true,
            modal: true,
            visible: false,
            buttons : [
                { text: "CSV", handler: handleCSVExport, isDefault:true },
                { text: "JSON", handler: handleJSONExport },
                { text: "Cancel", handler: hideDlg }
            ]
        });
        exportDlg.render(document.body);
    });

    Event.addListener("export", "click", function (e) {
        Event.stopEvent(e);
        if (SEARCH.ui.urlSearchParams == "") {
            alert("You need to search first!");
        } else {
            var s = (exportUrl.indexOf("?") != -1) ? SEARCH.ui.urlSearchParams.replace("?", "&") : SEARCH.ui.urlSearchParams;
            window.open(exportUrl + s + "&numfound=" + numFound);
        }
    });

    EXPORT.ui.buildExportHTML = function(siblingElName) {
        var sibling = Dom.get(siblingElName);

        var exportDiv = UI.insertDomElementAfter('div', sibling, {id : exportDialogElName }, {class : exportDialogCSSClass});
        UI.addDomElementChild('div', exportDiv, null, {class : "hd"});
        var bd = UI.addDomElementChild('div', exportDiv, null, {class : "bd"});
        UI.addDomElementChild('label', bd, { for: exportInputFileElName, innerHTML: "File Name:"});
        UI.addDomElementChild('input', bd, { id : exportInputFileElName, type: "text", value: "test"}, { width: "100%"});
    };
})();
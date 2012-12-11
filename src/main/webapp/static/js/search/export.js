var EXPORT  = {};
EXPORT.ui   = {};
EXPORT.util = {};

(function() {
    var Event           = YAHOO.util.Event,
        Dom             = YAHOO.util.Dom;

    var exportDialogElName      = "export_dialog",
        exportDialogCSSClass    = "yui-pe-content",
        exportInputFileElName   = "export_file_name",
        exportUrl               = "",
        exportButtonElId        = "",
        openSeparateExportPage  = false,
        getExportUrlParamsFn    = undefined,
        exportDlg               = undefined;

    EXPORT.ui.init = function(vars) {
        exportUrl               = vars.exportUrl;
        openSeparateExportPage  = vars.openSeparateExportPage;
        exportButtonElId        = vars.exportButtonElId;
        getExportUrlParamsFn    = vars.getExportUrlParamsFn;

        Event.addListener(exportButtonElId, "click", function (e) {
            Event.stopEvent(e);

            if (openSeparateExportPage) {
                var urlParams = getExportUrlParamsFn();
                if (urlParams == "") {
                    alert("You need to search first!");
                } else {
                    window.open(exportUrl + urlParams);
                }
            } else {
                exportDlg.show();
            }
        });
    };

    function exportFile(type, dlg) {
        window.open(exportUrl + "?type=" + type + "&file=" + Dom.get(exportInputFileElName).value);
        window.focus();
        dlg.hide();
    }

    function handleZipExport() { exportFile("zip", this); }

    /* Export dialog code */
    Event.onContentReady(exportDialogElName, function() {
        exportDlg = UI.createSimpleDlg(exportDialogElName, "Export", "none", handleZipExport, "Export");
        exportDlg.render(document.body);
    });



    EXPORT.ui.buildHTML = function(siblingElName) {
        var sibling = Dom.get(siblingElName);

        var exportDiv = UI.insertDomElementAfter('div', sibling, {id : exportDialogElName}, {class : exportDialogCSSClass});
        UI.addDomElementChild('div', exportDiv, null, {class : "hd"});
        var bd = UI.addDomElementChild('div', exportDiv, null, {class : "bd"});
        UI.addDomElementChild('label', bd, {for: exportInputFileElName, innerHTML: "File Name: "});
        UI.addDomElementChild('input', bd, {id : exportInputFileElName, type: "text", value: "test"}, { width: "100%"});
    };
})();
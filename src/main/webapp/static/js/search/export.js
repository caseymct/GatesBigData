var EXPORT  = {};
EXPORT.ui   = {};
EXPORT.util = {};

(function() {
    var Event           = YAHOO.util.Event,
        Dom             = YAHOO.util.Dom;

    var exportDialogElName       = "export_dialog",
        exportDialogCSSClass     = "yui-pe-content",
        exportInputFileElName    = "export_file_name",
        exportUrl                = "",
        exportButtonElId         = "",
        openSeparateExportPage   = false,
        getSearchRequestParamsFn = undefined,
        exportDlg                = undefined;

    EXPORT.ui.init = function(vars) {
        openSeparateExportPage   = vars[UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY];
        exportButtonElId         = vars[UI.EXPORT.EXPORT_BUTTON_EL_ID_KEY];
        getSearchRequestParamsFn = vars[UI.SEARCH.GET_SEARCH_REQ_PARAMS_FN_KEY];
        exportUrl                = vars[UI.EXPORT_URL_KEY];

        if (openSeparateExportPage == false) {
            buildHTML(vars[UI.EXPORT.HTML_SIBLING_NAME_KEY]);
        }

        Event.addListener(exportButtonElId, "click", function (e) {
            Event.stopEvent(e);

            if (openSeparateExportPage) {
                var urlParams = getSearchRequestParamsFn();
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
        var urlParams = getSearchRequestParamsFn() + "&type=" + type + "&file=" + Dom.get(exportInputFileElName).value;
        window.open(exportUrl + urlParams);
        window.focus();
        dlg.hide();
    }

    function handleZipExport() { exportFile("zip", this); }

    /* Export dialog code */
    Event.onContentReady(exportDialogElName, function() {
        exportDlg = UI.createSimpleDlg(exportDialogElName, "Export", "none", handleZipExport, "Export");
        exportDlg.render(document.body);
    });

    function buildHTML(siblingElName) {
        var sibling = Dom.get(siblingElName);

        var exportDiv = UI.insertDomElementAfter('div', sibling, {id : exportDialogElName}, { "class" : exportDialogCSSClass});
        UI.addDomElementChild('div', exportDiv, null, { "class" : "hd"});
        var bd = UI.addDomElementChild('div', exportDiv, null, { "class" : "bd"});
        UI.addDomElementChild('label', bd, { "for" : exportInputFileElName, innerHTML: "File Name: "});
        UI.addDomElementChild('input', bd, { id : exportInputFileElName, type: "text", value: "test"}, { width: "100%"});
    }
})();
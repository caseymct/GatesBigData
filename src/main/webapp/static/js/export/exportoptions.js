var EXPORTOPTIONS  = {};
EXPORTOPTIONS.util = {};

(function() {
    var Event = YAHOO.util.Event,     Connect = YAHOO.util.Connect,
        Dom   = YAHOO.util.Dom,       Overlay = YAHOO.widget.Overlay,
        Json  = YAHOO.lang.JSON;

    var headerElName           = "export_header",          contentEl                   = Dom.get(UI.CONTENT_EL_NAME),
        searchInfoElName       = 'search_info',
        infoLabelCSSClass      = "info_label",             infoValueCSSClass           = "info_value",
        overlayElCSSClass      = "overlay_el",             overlayInnerElCSSClass      = "overlay_inner_el",
        overlayDivCSSClass     = "overlay_div",            buttonShowOverlayCSSClass   = "button show_overlay",
        buttonCollapseCSSClass = "button collapse",        buttonSmallCSSClass         = "button small",

        checkAllButtonElName   = "check_all",              checkNoneButtonElName       = "check_none",
        exportOptionsElName    = "export_options",         exportInputFileElName       = "export_file_name",
        exportFileDivElName    = "export_file_div",        exportButtonElName          = "export",
        exportButtonCSSClass   = "button small",           checkboxGrpElName           = "export_fields",
        checkAuditButtonElName = "check_audit";

    var urls            = "",
        auditFields     = [],
        exportOptionsEl = null;

    var urlStr          = window.location.href.substring(window.location.href.indexOf("?") + 1),
        requestParams   = UI.util.getRequestParameters(),
        requestKeys     = Object.keys(requestParams),
        coreName        = UI.util.getRequestParamDisplayString(requestParams, UI.util.REQUEST_CORE_KEY);

    EXPORTOPTIONS.init = function(vars) {
        urls = vars[UI.URLS_KEY];
        buildHtml();
        initVars();
        registerListeners();
        buildFieldsHtml();
    };

    function getRequestInfoDivId(k)            { return k + '_info'; }
    function getRequestInfoContainerDivId(k)   { return k + '_info_container'; }
    function getRequestInfoDivInnerHtml(k)     { return '<b>' + UI.util.getRequestParamDisplayString(requestParams, k) + '</b>'; }

    function buildHtml() {
        var i, div;

        UI.addDomElementChild('h2', contentEl, { id : headerElName, innerHTML: "Export Search Results" });
        var searchInfo = UI.addDomElementChild('div', contentEl, { id : searchInfoElName });

        for(i = 0; i < requestKeys.length; i++) {
            var k = requestKeys[i];
            if (k == UI.util.REQUEST_ORDER_KEY) continue;

            div = UI.addDomElementChild('div', searchInfo, { id : getRequestInfoContainerDivId(k) });
            UI.addDomElementChild('div', div, { innerHTML : k + ':' }, { 'class' : infoLabelCSSClass });
            UI.addDomElementChild('div', div, { id : getRequestInfoDivId(k), innerHTML : getRequestInfoDivInnerHtml(k) },
                                              { 'class' :  infoValueCSSClass });
        }

        UI.addClearBothDiv(searchInfo);

        div = UI.addDivWithClass(contentEl, "buttons");
        UI.addDomElementChild('a', div, { href: "#", id : checkAllButtonElName, innerHTML: "Check All" }, { "class" :  buttonSmallCSSClass });
        UI.addDomElementChild('a', div, { href: "#", id : checkNoneButtonElName, innerHTML: "Check None" }, { "class" :  buttonSmallCSSClass });
        UI.addDomElementChild('a', div, { href: "#", id : checkAuditButtonElName, innerHTML: "Only Audit Fields" }, { "class" :  buttonSmallCSSClass });
        UI.addClearBothDiv(contentEl);

        UI.addDomElementChild('div', contentEl, { id : exportOptionsElName });

        div = UI.addDomElementChild('div', contentEl, { id : exportFileDivElName });
        UI.addDomElementChild('label', div, { for : exportInputFileElName, innerHTML: "Exported file name: "});
        UI.addDomElementChild('input', div, { id : exportInputFileElName, type : "text", value : "export.csv" });

        div = UI.addDivWithClass(contentEl, "buttons");
        UI.addDomElementChild('a', div, { href: "#", id : exportButtonElName, innerHTML: "Export" }, { "class" :  exportButtonCSSClass });

        UI.addClearBothDiv(contentEl);
    }

    function initVars() {
        Connect.asyncRequest('GET', urls[UI.AUDIT_FIELD_NAMES_URL_KEY] + coreName, {
            success : function(o) {
                var fields = Json.parse(o.responseText);
                auditFields = UI.util.specifyReturnValueIfUndefined(fields[UI.INFO_FIELDS_AUDIT_FIELDS_KEY], []);
            }
        });

        exportOptionsEl = Dom.get(exportOptionsElName);
    }

    function registerListeners() {
        Event.addListener(checkAllButtonElName, "click", function(e) {
            Event.stopEvent(e);
            changeAllCheckedStatus(true, []);
        });

        Event.addListener(checkNoneButtonElName, "click", function(e) {
            Event.stopEvent(e);
            changeAllCheckedStatus(false, []);
        });

        Event.addListener(checkAuditButtonElName, "click", function(e) {
            Event.stopEvent(e);
            changeAllCheckedStatus(true, auditFields);
        });

        Event.addListener(exportButtonElName, "click", function(e) {
            Event.stopEvent(e);

            var urlParams = "?type=csv&file=" + Dom.get(exportInputFileElName).value + "&" + urlStr;
            var exportFields = getAllCheckedFields();
            if (exportFields != "") {
                urlParams += "&fields=" + exportFields;
            }

            window.open(urls[UI.EXPORT_URL_KEY] + urlParams);
            window.focus();
        });
    }

    function changeCheckboxLabelColor(checkboxId, status) {
        Dom.setStyle(getCheckboxLabelElName(checkboxId), "color", status ? "black" : "gray");
    }

    function changeAllCheckedStatus(status, nameFilters) {
        var checkboxStatus, checkboxes  = document.getElementsByName(checkboxGrpElName);

        for (var i = 0; i < checkboxes.length; i++) {
            checkboxStatus = status;
            if (nameFilters.length > 0 && nameFilters.indexOf(checkboxes[i].id) == -1) {
                checkboxStatus = !status;
            }
            checkboxes[i].checked = checkboxStatus;
            changeCheckboxLabelColor(checkboxes[i].id, checkboxStatus);
        }
    }

    function getAllCheckedFields() {
        var checked = [], unchecked = [], checkboxes = document.getElementsByName(checkboxGrpElName);

        for (var i = 0; i < checkboxes.length; i++) {
            if (checkboxes[i].checked) {
                checked.push(checkboxes[i].value);
            } else {
                unchecked.push(checkboxes[i].value);
            }
        }

        return (checked.length > unchecked.length) ? "-" + unchecked.join(",") : "%2B" + checked.join(",");
    }

    function getOverlayLinkElName(parentName) {
        return "overlay_" + parentName + "_expand";
    }

    function getCheckboxLabelElName(chkboxId) { return chkboxId + "_label"; }

    function buildSubcategoryOverlay(parent, currOverlay, currCheckboxParent) {
        var overlayEl = "overlay_" + parent;

        if (Dom.get(overlayEl) == null) {
            var overlayLink    = getOverlayLinkElName(parent);
            var overlayInnerEl = overlayEl + "_child";
            var outerDiv = UI.addDomElementChild('div', exportOptionsEl, { id : overlayEl }, { "class" :  overlayElCSSClass });

            UI.addDomElementChild('a', outerDiv, { id: overlayLink },
                { "margin-right" : "2px", float : "left", class: buttonShowOverlayCSSClass });
            UI.addDomElementChild('div', outerDiv, { innerHTML: parent });

            var div = UI.addDomElementChild('div', outerDiv, { id: parent }, { "class" :  overlayDivCSSClass });

            currCheckboxParent = UI.addDomElementChild('div', div, { id: overlayInnerEl }, { "class" :  overlayInnerElCSSClass });

            currOverlay = new Overlay(parent, {
                context: [overlayEl, "tl","bl", ["beforeShow", "windowResize"]],
                visible: false
            });

            currOverlay.render(exportOptionsEl);

            Event.addListener(overlayEl, "click", function(e) {
                var l = getOverlayLinkElName(this.id);
                if (Dom.hasClass(l, buttonShowOverlayCSSClass)) {
                    Dom.removeClass(l, buttonShowOverlayCSSClass);
                    Dom.addClass(l, buttonCollapseCSSClass);
                    this.show();
                } else {
                    Dom.addClass(l, buttonShowOverlayCSSClass);
                    Dom.removeClass(l, buttonCollapseCSSClass);
                    this.hide();
                }
            }, currOverlay, true);
        }

        return { currOverlay : currOverlay, currCheckboxParent : currCheckboxParent };
    }

    function buildFieldsHtml() {
        Connect.asyncRequest('GET', urls[UI.FIELD_NAMES_URL_KEY] + "?core=" + coreName, {
            success : function(o) {
                var names = Json.parse(o.responseText).names, i;
                var currOverlay = null, currCheckboxParent = null;

                for(i = 0; i < names.length; i++) {
                    if (skipName(names[i])) continue;

                    var namesArr = names[i].split("."), parent = namesArr[0];
                    var child = (namesArr.length > 1) ? namesArr[1] : parent;

                    if (names[i].indexOf(".") > -1) {
                        var t = buildSubcategoryOverlay(parent, currOverlay, currCheckboxParent);
                        currOverlay = t['currOverlay'];
                        currCheckboxParent = t['currCheckboxParent'];
                    } else {
                        currCheckboxParent = exportOptionsEl;
                    }

                    var chkboxId = names[i].toUpperCase();
                    var d = UI.addDomElementChild('div', currCheckboxParent, null, { "class" :  overlayElCSSClass });
                    var c = UI.addDomElementChild('input', d, { "type" : "checkbox", name: checkboxGrpElName, value: i, id: chkboxId },
                                                              { "vertical-align" : "bottom" } );
                    if (child.match(/^[A-Z]/) != null) c.checked = true;

                    UI.addDomElementChild('label', d, { id: getCheckboxLabelElName(chkboxId), htmlFor: chkboxId, innerHTML : child },
                                                      { padding: "5px" });
                    UI.addDomElementChild('br', d);

                    Event.addListener(Dom.get(chkboxId), "click", function(o) {
                        changeCheckboxLabelColor(this.id, this.checked);
                    });
                }

                UI.addClearBothDiv(exportOptionsEl);
            }
        });
    }

    function skipName(name) {
        return name.match(/^_|suggest|^HDFS|digest|boost|host|segment|preview|Suggest$|Prefix$|facet$/) != null;
    }

})();
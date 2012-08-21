var LWA = {};
LWA.ui = {};
LWA.util = {};

(function() {

    /* UI and Dom functionality */
    LWA.ui.removeDivChildNodes = function(divName) {
        var div = Dom.get(divName);

        while (div.hasChildNodes()) {
            while (div.childNodes.length >= 1 ) {
                div.removeChild(div.firstChild);
            }
        }
    };

    LWA.ui.removeElement = function(elementId) {
        var el = Dom.get(elementId);
        el.parentNode.removeChild(el);
    };

    LWA.ui.createDomElement = function (elType, parentNode, attributes) {
        var el = document.createElement(elType);
        parentNode.appendChild(el);


        for(var i = 0; i < attributes.length; i++) {
            if (attributes[i].key.match(/text/)) {
                el.innerHTML = attributes[i].value;
            } else {
                el.setAttribute(attributes[i].key, attributes[i].value);
            }
        }
        return el;
    };

    LWA.ui.alertErrors = function(o) {
        try {
            var result = YAHOO.lang.JSON.parse(o.responseText);

            if (result.hasOwnProperty("errors")) {
                var errmsg = "Error message : " + result.errors[0].message + "\n" +
                    "Error key : " + result.errors[0].key + "\n" +
                    "Error code : " + result.errors[0].code;
                alert(errmsg);
                return true;
            }
        } catch (e) {
            return false;
        }
        return false;
    };

    LWA.ui.confirmDelete = new YAHOO.widget.Dialog("dlg", {
        width: "20em",
        fixedcenter: true,
        modal: true,
        visible: false,
        draggable: false
    });

    LWA.ui.confirmDelete.setHeader("Warning!");
    LWA.ui.confirmDelete.setBody("Are you sure you want to delete this item?");
    LWA.ui.confirmDelete.cfg.setProperty("icon", YAHOO.widget.SimpleDialog.ICON_WARN);
    LWA.ui.confirmDeleteHandleNo = function() { this.hide(); };
    LWA.ui.confirmDelete.render(YAHOO.util.Dom.get("content"));

    LWA.ui.buildTable = function(result, tableName) {
        var keys = Object.keys(result);
        var containerDiv, childDiv, resultsTable = Dom.get(tableName);

        for(var i = 0; i < keys.length; i++) {
            containerDiv = LWA.ui.createDomElement("div", resultsTable, [ { key : "class", value : "info_details_results_div" }]);
            childDiv = LWA.ui.createDomElement("div", containerDiv, [ { key : "class", value : "info_details_child_div" },
                { key: "text", value: "<b>" + keys[i] + "</b>: " + result[keys[i]] }]);

            if (i < keys.length) {
                childDiv = LWA.ui.createDomElement("div", containerDiv, [ { key : "class", value : "info_details_child_div" },
                    { key: "text", value: "<b>" + keys[++i] + "</b>: " + result[keys[i]] }]);
            }

            LWA.ui.createDomElement("div", containerDiv, [ { key: "style", value : "clear:both" } ]);
        }
    };

    /* Utility functions */
    LWA.util.stripBrackets = function(s) {
        return (typeof s == "string") ? s.replace(/^\[|\]$/g, '') : s;
    };

    LWA.util.checkXmlReturnValue = function (o) {
        var dp = new DOMParser();
        var xDoc = dp.parseFromString(o.responseText, "text/xml");

        if (xDoc.getElementsByTagName("lst").length > 0) {
            var childNodes = xDoc.getElementsByTagName("lst")[0].childNodes;

            if (childNodes.length > 1 && childNodes[0].nodeName.match("int") && childNodes[0].textContent.match("0")) {
                return true;
            }
        } else {
            var alertString = xDoc.getElementsByTagName("title")[0].textContent;
            var stackTraceStart = alertString.indexOf("java");
            if (stackTraceStart != -1) {
                alertString = alertString.substring(0, stackTraceStart);
            }
            alert("Could not add file: \n" + alertString);
        }
        return false;
    };

})();

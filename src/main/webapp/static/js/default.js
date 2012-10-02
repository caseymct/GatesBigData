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

    LWA.ui.buildTreeRecurse = function(doc, keyname, parentNode) {
        var key, isObject = Object.prototype.toString.call(doc).match("Object") != null;
        var name = isObject ? keyname : keyname + ": <b>" + doc + "</b>";
        var nameNode = new YAHOO.widget.HTMLNode(name, parentNode, false);

        if (!isObject) {
            nameNode.isLeaf = true;
        } else {
            for(key in doc) {
                LWA.ui.buildTreeRecurse(doc[key], key, nameNode);
            }
        }
    };

    LWA.ui.buildTreeViewFromJson = function(docs, treeView) {
        var i, j, key, parent, root = treeView.getRoot();
        treeView.removeChildren(root);

        if (!(docs instanceof Array)) {
            docs = [docs];
        }

        for(i = 0; i < docs.length; i++) {
            parent = (docs[i].hasOwnProperty("name")) ? new YAHOO.widget.TextNode(docs[i].name, root, false) : root;
            for(key in docs[i]) {
                LWA.ui.buildTreeRecurse(docs[i][key], key, parent);
            }
        }
        treeView.render();
        treeView.expandAll();
    };

    LWA.ui.initWait = function () {
        if (!LWA.ui.wait) {
            LWA.ui.wait = new YAHOO.widget.Panel("wait",
                    { width: "240px",
                        fixedcenter: true,
                        close: false,
                        draggable: false,
                        zindex:4,
                        modal: true,
                        visible: false
                    }
                );

            LWA.ui.wait.setHeader("Loading, please wait...");
            LWA.ui.wait.setBody("<img src=\"http://l.yimg.com/a/i/us/per/gr/gp/rel_interstitial_loading.gif\"/>");
            LWA.ui.wait.render(document.body);
        }
    };

    LWA.ui.showWait = function() { LWA.ui.wait.show(); };
    LWA.ui.hideWait = function() { LWA.ui.wait.hide(); };


    /* Utility functions */
    LWA.util.stripBrackets = function(s) {
        return (typeof s == "string") ? s.replace(/^\[|\]$/g, '') : s;
    };

    LWA.util.isValidJSON = function(json) {
        try {
            YAHOO.lang.JSON.parse(json);
            return true;
        } catch (ex) {
            return false;
        }
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

    // formatJson() :: formats and indents JSON string
    LWA.util.formatJson = function(val) {
        var retval = '', str = val, pos = 0, strLen = val.length, ch = '';
        var indentStr = '&nbsp;&nbsp;&nbsp;&nbsp;', newLine = '<br />';

        for (var i=0; i<strLen; i++) {
            ch = str.substring(i,i+1);

            if (ch == '}' || ch == ']') {
                retval = retval + newLine;
                pos = pos - 1;

                for (var j=0; j<pos; j++) {
                    retval = retval + indentStr;
                }
            }

            retval = retval + ch;

            if (ch == '{' || ch == '[' || ch == ',') {
                retval = retval + newLine;
                if (ch == '{' || ch == '[') {
                    pos = pos + 1;
                }
                for (var k=0; k<pos; k++) {
                    retval = retval + indentStr;
                }
            }
        }
        return retval;
    };

    /* Sort keys so that title and author come first */
    LWA.util.sortKeys = function(unsortedkeys) {
        var sortedkeys = [];
        var strings = ["title", "author", "creator", "url"];

        for(var i = 0; i < strings.length; i++) {
            var index = unsortedkeys.indexOf(strings[i]);
            if (index != -1) {
                sortedkeys.push(strings[i]);
                unsortedkeys.splice(index, 1);
            }
        }

        return sortedkeys.concat(unsortedkeys.sort());
    };

})();

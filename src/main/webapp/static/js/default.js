var UI = {};
UI.util = {};

(function() {
    var Dom = YAHOO.util.Dom;

    /* UI and Dom functionality */
    UI.removeDivChildNodes = function(divName) {
        var div = Dom.get(divName);

        while (div.hasChildNodes()) {
            while (div.childNodes.length >= 1 ) {
                div.removeChild(div.firstChild);
            }
        }
    };

    UI.removeElement = function(elementId) {
        var el = Dom.get(elementId);
        el.parentNode.removeChild(el);
    };

    function createDomElement(type, attributes, styles) {
        var i, el = document.createElement(type);

        if (attributes != undefined && attributes != null) {
            for(i = 0; i < attributes.length; i++) {
                el[attributes[i].key] = attributes[i].value;
            }
        }
        if (styles != undefined && styles != null) {
            for(i = 0; i < styles.length; i++) {
                if (styles[i].key == "class") {
                    Dom.addClass(el, styles[i].value);
                } else {
                    Dom.setStyle(el, styles[i].key, styles[i].value);
                }
            }
        }
        return el;
    }

    UI.addDomElementChild = function(type, parentNode, attributes, styles) {
        var el = createDomElement(type, attributes, styles);
        parentNode.appendChild(el);
        return el;
    };

    UI.insertDomElementAfter = function(type, siblingNode, attributes, styles) {
        var el = createDomElement(type, attributes, styles);
        siblingNode.parentNode.insertBefore(el, siblingNode.nextSibling);
        return el;
    };

    UI.alertErrors = function(o) {
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

    UI.confirmDelete = new YAHOO.widget.Dialog("dlg", {
        width: "20em",
        fixedcenter: true,
        modal: true,
        visible: false,
        draggable: false
    });

    UI.confirmDelete.setHeader("Warning!");
    UI.confirmDelete.setBody("Are you sure you want to delete this item?");
    UI.confirmDelete.cfg.setProperty("icon", YAHOO.widget.SimpleDialog.ICON_WARN);
    UI.confirmDeleteHandleNo = function() { this.hide(); };
    UI.confirmDelete.render(YAHOO.util.Dom.get("content"));

    UI.buildTable = function(result, tableName) {
        var keys = Object.keys(result);
        var containerDiv, childDiv, resultsTable = Dom.get(tableName);

        for(var i = 0; i < keys.length; i++) {
            containerDiv = UI.addDomElementChild("div", resultsTable,
                [ { key : "class", value : "info_details_results_div" }], []);
            childDiv = UI.addDomElementChild("div", containerDiv,
                [ { key : "class", value : "info_details_child_div" },
                  { key: "text", value: "<b>" + keys[i] + "</b>: " + result[keys[i]] }], []);

            if (i < keys.length) {
                childDiv = UI.addDomElementChild("div", containerDiv,
                    [ { key : "class", value : "info_details_child_div" },
                      { key: "text", value: "<b>" + keys[++i] + "</b>: " + result[keys[i]] }], []);
            }

            UI.addDomElementChild("div", containerDiv, [], [{ key: "clear", value: "both" } ]);
        }
    };

    UI.buildTreeRecurse = function(doc, keyname, parentNode) {
        var key, isObject = Object.prototype.toString.call(doc).match("Object") != null;
        var name = isObject ? keyname : keyname + ": <b>" + doc + "</b>";
        var nameNode = new YAHOO.widget.HTMLNode(name, parentNode, false);

        if (!isObject) {
            nameNode.isLeaf = true;
        } else {
            for(key in doc) {
                UI.buildTreeRecurse(doc[key], key, nameNode);
            }
        }
    };

    UI.buildTreeViewFromJson = function(docs, treeView) {
        var i, j, key, parent, root = treeView.getRoot();
        treeView.removeChildren(root);

        if (!(docs instanceof Array)) {
            docs = [docs];
        }

        for(i = 0; i < docs.length; i++) {
            parent = (docs[i].hasOwnProperty("name")) ? new YAHOO.widget.TextNode(docs[i].name, root, false) : root;
            for(key in docs[i]) {
                UI.buildTreeRecurse(docs[i][key], key, parent);
            }
        }
        treeView.render();
        treeView.expandAll();
    };

    UI.initWait = function () {
        if (!UI.wait) {
            UI.wait = new YAHOO.widget.Panel("wait",
                    { width: "240px",
                        fixedcenter: true,
                        close: false,
                        draggable: false,
                        zindex:4,
                        modal: true,
                        visible: false
                    }
                );

            UI.wait.setHeader("Loading, please wait...");
            UI.wait.setBody("<img src=\"http://l.yimg.com/a/i/us/per/gr/gp/rel_interstitial_loading.gif\"/>");
            UI.wait.render(document.body);
        }
    };

    UI.showWait = function() { UI.wait.show(); };
    UI.hideWait = function() { UI.wait.hide(); };

    UI.getButtonGroupCheckedButtonValue = function(bg) {
        return bg.get('checkedButton').get('value');
    };

    /* Utility functions */
    UI.util.stripBrackets = function(s) {
        return (typeof s == "string") ? s.replace(/^\[|\]$/g, '') : s;
    };

    UI.util.isValidJSON = function(json) {
        try {
            YAHOO.lang.JSON.parse(json);
            return true;
        } catch (ex) {
            return false;
        }
    };

    UI.util.checkXmlReturnValue = function (o) {
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
    UI.util.formatJson = function(val) {
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
    UI.util.sortKeys = function(unsortedkeys) {
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

    String.prototype.endsWith = function(suffixList) {
        if (!(suffixList instanceof Array)) {
            suffixList = [suffixList];
        }

        for(var i = 0; i < suffixList.length; i++) {
            var suffix = suffixList[i];
            if (this.indexOf(suffix, this.length - suffix.length) !== -1) {
                return true;
            }
        }
        return false;
    };

    String.prototype.startsWith = function(prefix) {
        return this.indexOf(prefix) == 0;
    };

    String.prototype.encodeForRequest = function() {
        return encodeURIComponent(this.replace(/&amp;/g, "&"));
    };

    UI.util.jsonSyntaxHighlight = function (json) {
        var json = YAHOO.lang.JSON.stringify(json, undefined, 3);

        json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        json = json.replace(/\n/g, "<br>").replace(/\s/g, "&nbsp;");

        return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            var cls = 'number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'key';
                } else {
                    cls = 'string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'boolean';
            } else if (/null/.test(match)) {
                cls = 'null';
            }

            if (match.length > 40) {
                var m = match.split("&nbsp;"), newmatch = "", i = 0, newlines = [];
                while (i < m.length) {
                    if (newmatch.length + m[i].length > 40) {
                        newlines.push(newmatch);
                        newmatch = "";
                    } else {
                        newmatch += m[i] + " ";
                    }
                    i++;
                }
                if (newmatch != "") {
                    newlines.push(newmatch);
                }
                match = newlines.join("<br>&nbsp;&nbsp;");
            }
            return '<span class="' + cls + '">' + match + '</span>';
        });
    };

    Array.prototype.prependToEach = function(l, ch) {
        if (!(l.length == 1 && l[0] == '')) {
            for(var i = 0; i < l.length; i++) {
                l[i] = ch + l[i];
            }
        }
        return l;
    };

})();

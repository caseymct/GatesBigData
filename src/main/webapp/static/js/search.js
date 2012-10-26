var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    var TextNode = YAHOO.widget.TextNode,      Overlay = YAHOO.widget.Overlay,
           Event = YAHOO.util.Event,           Tooltip = YAHOO.widget.Tooltip,
             Dom = YAHOO.util.Dom,              Button = YAHOO.widget.Button,
        TreeView = YAHOO.widget.TreeView,         Json = YAHOO.lang.JSON,
       Paginator = YAHOO.widget.Paginator,     TabView = YAHOO.widget.TabView,
     ButtonGroup = YAHOO.widget.ButtonGroup,   Connect = YAHOO.util.Connect,
    SimpleDialog = YAHOO.widget.SimpleDialog;

    SEARCH.ui.longStringWidth  = 210;
    SEARCH.ui.shortStringWidth = 80;
    SEARCH.ui.facetContainerEl = "facet_container";
    SEARCH.ui.facetTreeViewEl  = "tree_view";
    SEARCH.ui.contentContainer = Dom.get("content_container");
    SEARCH.ui.contentContainerMinHeight = parseInt(Dom.getStyle("content_container", "height").replace("px", ""));

    /* Set core name and header for this search file */
    Dom.setStyle(SEARCH.ui.facetContainerEl, "visibility", "visible");
    var url = window.location.href.split('/');
    SEARCH.ui.coreName = url[url.length - 1];

    SEARCH.ui.setSearchHeader = function(containerEl) {
        Dom.get(containerEl).innerHTML = "Search Core: " + SEARCH.ui.coreName;
    };

    SEARCH.ui.beginDatePick  = null;
    SEARCH.ui.endDatePick    = null;
    SEARCH.ui.beginDateInput = null;
    SEARCH.ui.endDateInput   = null;
    SEARCH.ui.dateConstraintField = "";
    SEARCH.ui.initDatePickers = function(beginDateEl, endDateEl, constrainByEl, dateConstraintField) {
        SEARCH.ui.beginDateInput = beginDateEl;
        SEARCH.ui.beginDatePick = new JsDatePick({ useMode:2, dateFormat:"%M-%d-%Y", imgPath:"../static/images/jsdatepick/",
            target:beginDateEl
        });

        SEARCH.ui.endDateInput = endDateEl;
        SEARCH.ui.endDatePick = new JsDatePick({ useMode:2, dateFormat:"%M-%d-%Y", imgPath:"../static/images/jsdatepick/",
            target:endDateEl
        });
        SEARCH.ui.dateConstraintField = dateConstraintField;
        Dom.get(constrainByEl).innerHTML = SEARCH.ui.dateConstraintField;
    };

    SEARCH.ui.initDateRangeText = function(url, dateEl) {
        Connect.asyncRequest('GET', url + "?core=" + SEARCH.ui.coreName + "&field=" + SEARCH.ui.dateConstraintField, {
            success : function(o) {
                var dateStr = o.responseText.split(" to ");
                Dom.get(dateEl).innerHTML = SEARCH.ui.dateConstraintField + ": <i>" + dateStr[0] + "</i> to " +
                    "<i>" + dateStr[1] + "</i>";
                var start = dateStr[0].match(/([0-9]+)-([0-9]+)-([0-9]+)/),
                      end = dateStr[1].match(/([0-9]+)-([0-9]+)-([0-9]+)/);

                SEARCH.ui.beginDatePick.setSelectedDay({ day:start[2], month:start[1], year:start[3] });
                SEARCH.ui.endDatePick.setSelectedDay({ day:end[2], month:end[1], year:end[3] });
            }
        });
    };

    SEARCH.ui.previewContainer = null;
    SEARCH.ui.previewDataName = "preview_image";
    SEARCH.ui.initPreviewContainer = function(containerEl) {
        SEARCH.ui.previewContainer = Dom.get(containerEl);
    };
    SEARCH.ui.setPreviewContainerData = function(title, dataString, dataType) {
        Dom.setStyle(SEARCH.ui.previewContainer, "border", "1px solid gray");
        var titleString = (title == undefined) ? "<b>Search result preview</b>" : "Document: <b>" + title + "</b>";
        var htmlString =
            "<span style='margin-left: 5px; font-size: 10px'>" + titleString +
                "<a style='float: right' class='button zoom_out' id='previewzoomout'></a>" +
                "<a style='float: right' class='button zoom_in' id='previewzoomin'></a>" +
                "</span><hr>";

        if (dataType == "image") {
            htmlString += "<img src='" + dataString + "' height='400px' width='400px' id='" + SEARCH.ui.previewDataName + "'>";
        } else {
            htmlString += "<div id='" + SEARCH.ui.previewDataName + "'>" + SEARCH.util.jsonSyntaxHighlight(dataString) + "</div>";
        }

        SEARCH.ui.previewContainer.innerHTML = htmlString;
        Dom.setStyle(SEARCH.ui.previewContainer, "background-image", "none");

        if (dataType == "image") {
            Dom.setStyle(SEARCH.ui.previewDataName, 'zoom', 2);
        } else {
            Dom.setStyle(SEARCH.ui.previewDataName, 'font-size', '10px');
        }

        Event.addListener('previewzoomin', 'click', function(e) {
            Dom.setStyle(SEARCH.ui.previewDataName, 'zoom', parseInt(Dom.getStyle(SEARCH.ui.previewDataName, 'zoom')) + 1);
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var zoom = parseInt(Dom.getStyle(SEARCH.ui.previewDataName, 'zoom'));
            Dom.setStyle(SEARCH.ui.previewDataName, 'zoom', (zoom > 1) ? zoom - 1 : zoom);
        });
    };

    SEARCH.ui.setPreviewContainerLoadingImage = function(img) {
        SEARCH.ui.previewContainer.innerHTML = "";
        Dom.setStyle(SEARCH.ui.previewContainer, "background-image", "url(\"" + img + "\")");
        Dom.setStyle(SEARCH.ui.previewContainer, "background-position", "50% 10%");
        Dom.setStyle(SEARCH.ui.previewContainer, "background-repeat", "no-repeat");
    };

    SEARCH.ui.clearPreviewContainerImage = function() {
        Dom.setStyle(SEARCH.ui.previewContainer, "border", "none");
        SEARCH.ui.previewContainer.innerHTML = "";
    };

    /* Set a variable for the general query search input */
    SEARCH.ui.generalQuerySearchInput = null;
    SEARCH.ui.generalQueryDefaultValue = "*:*";
    SEARCH.ui.initGeneralQuerySearchInput = function(containerEl) {
        SEARCH.ui.generalQuerySearchInput = Dom.get(containerEl);
    };
    SEARCH.ui.resetGeneralQuerySearchInput = function() {
        SEARCH.ui.generalQuerySearchInput.value = SEARCH.ui.generalQueryDefaultValue;
    };

    /* Search tab */
    SEARCH.ui.searchTab = null;
    SEARCH.ui.initSearchTab = function(containerEl) {
        SEARCH.ui.searchTab = new TabView(containerEl);
    };
    SEARCH.ui.getSearchTabActiveIndex = function() {
        return SEARCH.ui.searchTab.get('activeIndex');
    };
    SEARCH.ui.setSearchTabToGeneralQuery = function() {
        SEARCH.ui.searchTab.set('activeIndex', SEARCH.ui.searchTab.get("tabs").length - 1);
    };
    SEARCH.ui.getQueryTerms = function() {
        return (SEARCH.ui.searchTab == null ||
                SEARCH.ui.searchTab.get('activeIndex') == (SEARCH.ui.searchTab.get("tabs").length - 1)) ?
                SEARCH.ui.generalQuerySearchInput.value : SEARCH.ui.generalQueryDefaultValue;
    };

    /* initialize facet TreeView */
    SEARCH.ui.facetTreeView  = new TreeView(SEARCH.ui.facetTreeViewEl);

    /* Sort order button code */
    SEARCH.ui.sortOrder = "desc";
    SEARCH.ui.initSortOrderButtonGroup = function(containerEl) {
        var ascTypeButtonGroup = new ButtonGroup(containerEl);
        ascTypeButtonGroup.check(1);
        ascTypeButtonGroup.on("checkedButtonChange", function (o) {
            SEARCH.ui.sortOrder = o.newValue.get("value");
        });
    };

    /* Tooltip code */
    SEARCH.ui.tooltip = new Tooltip("tool_tip", { zIndex : 20 });

    SEARCH.ui.showTooltip = function (target, record, column, x, y) {
        //var target = oArgs.target;
        //var column = this.getColumn(target), record = this.getRecord(target);
        var s = target.innerText.replace(/\n$/, '');
        if (s.substring(s.length - 3) == "...") {
            SEARCH.ui.tooltip.setBody(record.getData()[column.getField()]);
            SEARCH.ui.tooltip.cfg.setProperty('xy', [x, y]);
            SEARCH.ui.tooltip.show();
        }
    };

    SEARCH.ui.hideTooltip = function() {
        SEARCH.ui.tooltip.hide();
    };

    /* Export dialog code */
    SEARCH.ui.exportUrl = "";
    SEARCH.ui.initExportUrl = function(url) { SEARCH.ui.exportUrl = url; };
    SEARCH.ui.exportFile = function(type, dlg) {
        window.open(SEARCH.ui.exportUrl + "?type=" + type + "&file=" + Dom.get("export_file_name").value);
        window.focus();
        dlg.hide();
    };
    SEARCH.ui.handleCSVExport  = function() { SEARCH.ui.exportFile("csv", this); };
    SEARCH.ui.handleJSONExport = function() { SEARCH.ui.exportFile("json", this); };
    SEARCH.ui.hideDlg          = function() { this.hide() };

    SEARCH.ui.exportDlg = new SimpleDialog("exportDialog", {
        width: "20em",
        fixedcenter: true,
        modal: true,
        visible: false,
        buttons : [
            { text: "CSV", handler: SEARCH.ui.handleCSVExport, isDefault:true },
            { text: "JSON", handler: SEARCH.ui.handleJSONExport },
            { text: "Cancel", handler: SEARCH.ui.hideDlg }
        ]
    });
    SEARCH.ui.exportDlg.render(document.body);

    Event.addListener("export", "click", function (e) {
        Event.stopEvent(e);
        SEARCH.ui.exportDlg.show();
    });

    /* Field select code */
    SEARCH.ui.selectData = [];
    SEARCH.ui.initSelectData = function(columnDefs) {
        for(var idx = 0; idx < columnDefs.length; idx++) {
            var split = columnDefs[idx].key.split(".");
            var txt = split[split.length - 1];
            var val = split.length == 2 ? columnDefs[idx].key + ".facet" : columnDefs[idx].key;
            SEARCH.ui.selectData.push({ text: txt, value: val });
        }
    };

    SEARCH.ui.initSortBySelect = function(containerName, initialSelectIndex) {
        SEARCH.ui.sortBySelect = new Button({
                id: "sortByMenu", name: "sortByMenu", type: "menu", lazyloadmenu: false,
                menu: SEARCH.ui.selectData, container: containerName
        });
        SEARCH.ui.initialSelectIndex = initialSelectIndex;

        var sortBySelectMenu = SEARCH.ui.sortBySelect.getMenu();
        sortBySelectMenu.subscribe("render", function (type, args, button) {
            button.set("selectedMenuItem", this.getItem(SEARCH.ui.initialSelectIndex));
        }, SEARCH.ui.sortBySelect);

        SEARCH.ui.sortBySelect.on("selectedMenuItemChange", function (event) {
            this.set("label", ("<em class=\"yui-button-label\">" + event.newValue.cfg.getProperty("text") + "</em>"));
        });

        return SEARCH.ui.sortBySelect;
    };

    /* Paginator code */
    SEARCH.ui.pag = new Paginator( {rowsPerPage : 10, containers : [ "pag1" ] });
    SEARCH.ui.initPaginator = function(containerEl) {
        SEARCH.ui.pag.set('containers', containerEl);
    };

    SEARCH.ui.updatePaginatorAfterSearch = function(numFound) {
        SEARCH.ui.pag.set('totalRecords', numFound);
        SEARCH.ui.pag.render();
        SEARCH.ui.pag.setStartIndex(0);
    };

    /* Data table event handlers */
    SEARCH.ui.dataTableSortedByChange = function(sortField) {
        var index = -1;
        for(var i = 0; i < SEARCH.ui.selectData.length; i++) {
            if (SEARCH.ui.selectData[i].value.replace(/\.facet$/, "") == sortField) {
                index = i;
                break;
            }
        }
        SEARCH.ui.sortBySelect.set("selectedMenuItem", SEARCH.ui.sortBySelect.getMenu().getItem(index));
        SEARCH.ui.pag.setStartIndex(0);
    };

    SEARCH.ui.dataTableCellMouseoverEvent = function (e, dataTable, loadingImg, thumbnailUrl) {
        var target = e.target;
        var record = dataTable.getRecord(target),
           // column = dataTable.getColumn(target),
            data = record.getData();

        var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + data.HDFSSegment + "&file=" + data.HDFSKey;
        var callback = {
            success: function(o) {
                var response = Json.parse(o.responseText);
                this.argument.table.updateCell(record, "thumbnail", response.Contents);
                this.argument.table.updateCell(record, "thumbnailType", response.contentType);
                SEARCH.ui.setPreviewContainerData(record.getData().title, response.Contents, response.contentType);
            },
            argument: { table: dataTable, record: record}
        };

        if (data.thumbnail == undefined) {
            SEARCH.ui.setPreviewContainerLoadingImage(loadingImg);
            Connect.asyncRequest('GET', thumbnailUrl + urlParams, callback);
        } else {
            SEARCH.ui.setPreviewContainerData(data.title, data.thumbnail, data.thumbnailType);
        }

       // SEARCH.ui.showTooltip(target, record, column, e.event.pageX, e.event.pageY);
    };

    SEARCH.ui.dataTableCellClickEvent = function(data, baseUrl) {
        window.open(baseUrl + "?core=" + SEARCH.ui.coreName + "&segment=" +
            data.HDFSSegment + "&file=" + data.HDFSKey + "&view=preview", "_blank");
    };

    SEARCH.ui.dataTableCellClickEventOLD = function(record, column, linkedCol, viewFullUrl) {
        var data = record.getData();

        switch (column.key) {
            case linkedCol :
                var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + data.HDFSSegment + "&file=" + data.HDFSKey + "&view=preview";
                window.open(viewFullUrl + urlParams, "_blank");
            default:
                var currentValue = SEARCH.ui.generalQuerySearchInput.value;
                var newSearchKey = column.getField();

                if (data[newSearchKey] != undefined) {
                    var newSearchVal = "\"" + data[newSearchKey] + "\"";
                    var newValue = currentValue;

                    if (currentValue == "*:*" || currentValue == "") {
                        newValue = newSearchKey + ":(" + newSearchVal + ")";

                    } else if (currentValue.indexOf(newSearchVal) == -1 ) {
                        var keyIdx = currentValue.indexOf(newSearchKey);
                        if (keyIdx >= 0) {
                            var sliceIdx = keyIdx + newSearchKey.length + 2;
                            newValue = currentValue.slice(0, sliceIdx) + newSearchVal + " OR " + currentValue.slice(sliceIdx);
                        } else {
                            newValue = currentValue + " AND " + newSearchKey + ":(" + newSearchVal + ")";
                        }
                    }

                    SEARCH.ui.generalQuerySearchInput.value = newValue;
                }
                break;
        }
    };

    SEARCH.ui.colStringMaxChars = 20;
    SEARCH.ui.colDateStringMaxChars = 30;
    /* Formatters for the search results table */
    SEARCH.ui.formatLink = function(el, record, column, data) {
        var max = (column.label.match(/ Date/) != null) ? SEARCH.ui.colDateStringMaxChars : SEARCH.ui.colStringMaxChars;
        if (data == undefined) {
            data = "<i>No value</i>";
        } else if (data.length > max) {
            data = data.substring(0, max) + " ...";
        }
        el.innerHTML = '<a href="#">' + data + '</a>';
    };

    SEARCH.ui.formatDate = function(el, record, column, data) {
        data = (data == undefined) ? "<i>No value</i>" : YAHOO.util.Date.format(data, {format: "%I:%M:%S %p %Y-%m-%d %Z"});
        el.innerHTML = '<a href="#">' + data + '</a>';
    };


    /*
    Overlay code
     */
    SEARCH.ui.showOverlayButtonElName = "show_overlay";
    SEARCH.ui.changeShowOverlayButtonVisibility = function(visible) {
        Dom.setStyle(SEARCH.ui.showOverlayButtonElName, "visibility", visible ? "visible" : "hidden");
    };

    SEARCH.ui.overlay = new Overlay("overlay", {
        context: [SEARCH.ui.showOverlayButtonElName, "tl","bl", ["beforeShow", "windowResize"]],
        visible: false
    });

    SEARCH.ui.overlay.render(SEARCH.ui.contentContainer);

    SEARCH.ui.showOverlay = function() {
        SEARCH.ui.overlay.show();
        Dom.removeClass(SEARCH.ui.showOverlayButtonElName, "button down-big-overlay");
        Dom.addClass(SEARCH.ui.showOverlayButtonElName, "button up-big-overlay");
    };
    SEARCH.ui.hideOverlay = function() {
        SEARCH.ui.overlay.hide();
        Dom.addClass(SEARCH.ui.showOverlayButtonElName, "button down-big-overlay");
        Dom.removeClass(SEARCH.ui.showOverlayButtonElName, "button up-big-overlay");
    };

    Event.addListener(SEARCH.ui.showOverlayButtonElName, "click", function(e) {
        if (Dom.hasClass(SEARCH.ui.showOverlayButtonElName, "button down-big-overlay")) {
            SEARCH.ui.showOverlay();
        } else {
            SEARCH.ui.hideOverlay();
        }
    }, SEARCH.ui.overlay, true);


    SEARCH.ui.adjustContentContainerHeight = function () {
        var oh = parseInt(Dom.getStyle("overlay", "height").replace("px", "")) + 300;
        var h = (oh > SEARCH.ui.contentContainerMinHeight) ? oh : SEARCH.ui.contentContainerMinHeight;
        Dom.setStyle(SEARCH.ui.contentContainer, "height", h + "px");
    };

    /* Facet Tree code
     */
    SEARCH.ui.buildFacetTree = function(facets) {
        var i, j, nameNode, valueNode, root = SEARCH.ui.facetTreeView.getRoot();

        SEARCH.ui.facetTreeView.removeChildren(root);

        for(i = 0; i < facets.length; i++) {
            var parent = root;
            if (facets[i].name.indexOf(".") > 0) {
                var facetNameArray = facets[i].name.split(".");
                var facetParentName = facetNameArray[0], facetChildName = facetNameArray[1];
                var parentNode = SEARCH.ui.facetTreeView.getNodeByProperty("label", facetParentName);
                if (parentNode == null) {
                    parentNode = new TextNode(facetParentName, root, false);
                }
                parent = parentNode;
                facets[i].name = facetChildName;
            }

            if (facets[i].values.length > 0 &&
                  !(facets[i].values.length == 1 && facets[i].values[0].match(/^null\s\([0-9]+\)$/))) {
                nameNode = new TextNode(facets[i].name, parent, false);

                for(j = 0; j < facets[i].values.length; j++) {
                    valueNode = new TextNode(facets[i].values[j], nameNode, false);
                    valueNode.isLeaf = true;
                }
            }
        }

        SEARCH.ui.facetTreeView.render();

        SEARCH.ui.facetTreeView.subscribe("expandComplete", SEARCH.ui.adjustContentContainerHeight);
        SEARCH.ui.facetTreeView.subscribe("collapseComplete", SEARCH.ui.adjustContentContainerHeight);

        SEARCH.ui.facetTreeView.subscribe("clickEvent", function(e) {
            Event.stopEvent(e);
            if (SEARCH.ui.facetTreeView.getRoot() == e.node.parent) {
                return;
            }

            var node = e.node;
            var anchorText = (!(node.parent.parent instanceof YAHOO.widget.RootNode) ? node.parent.parent.label + "." : "")
                + node.parent.label + " : " + node.label.substring(0, node.label.lastIndexOf("(") - 1);

            if (Dom.inDocument("treeNode" + node.index) == false) {
                var anchor = LWA.ui.createDomElement("a", Dom.get("facet_options"), [
                    { key : "class", value: "button delete" },
                    { key : "id", value : "treeNode" + node.index },
                    { key : "style", value: "margin: 2px" }]);
                anchor.appendChild(document.createTextNode(anchorText));

                Event.addListener("treeNode" + node.index, "click", function(e) {
                    LWA.ui.removeElement("treeNode" + node.index);
                });
            }
        });
    };

    /*
     Get facets code
     */
    SEARCH.ui.buildInitialFacetTree = function(o) {
        var result = Json.parse(o.responseText);
        SEARCH.ui.changeShowOverlayButtonVisibility(true);
        SEARCH.ui.showOverlay();
        SEARCH.ui.buildFacetTree(result.response.facets);
    };

    SEARCH.ui.updateSolrQueryDiv = function(containerEl, fq) {
        Dom.get(containerEl).innerHTML = "Submitting to solr with<br>q: " + SEARCH.ui.getQueryTerms() +
                                         "<br>fq: " + decodeURIComponent(fq);
    };


    /* Util */
    SEARCH.util.encodeForRequest = function(s) {
        return encodeURIComponent(s.replace(/&amp;/g, "&"));
    };

    SEARCH.util.constructSearchUrlParams = function(fq, start) {
         var sortType = SEARCH.ui.sortBySelect.get("selectedMenuItem").value,
             coreName = SEARCH.ui.coreName,
            sortOrder = SEARCH.ui.sortOrder,
           queryTerms = SEARCH.ui.getQueryTerms();

        var urlParams = "?query=" + queryTerms + "&core=" + coreName + "&sort=" + sortType + "&order=" + sortOrder;
        if (fq != "") {
            urlParams += "&fq=" + fq;
        }
        if (start != undefined && start != "") {
            urlParams += "&start=" + start;
        }

        return urlParams;
    };

    var formatDateString = function(day, month, year) {
        return year + "-" + (((month < 10) ? "0" : "") + month) + "-" + (((day < 10) ? "0" : "") + day);
    };

    var monthAbbrev = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
    var getDateConstraint = function(inputName, datePicker) {
        if (datePicker == false) return "*";

        //first check the input
        var input = Dom.get(inputName).value.trim();
        if (input.match(/^[\s]*[\\*]*$/) != null) return "*";

        var datePickerRep = formatDateString(parseInt(datePicker.day), parseInt(datePicker.month), parseInt(datePicker.year));
        var match = input.match(/^([a-zA-Z]{3})-([0-9]{2})-([0-9]{4})$/);
        if (match != null) {
            var inputMonth = monthAbbrev.indexOf(match[1]) + 1;
            var inputDay = parseInt(match[2].replace(/^0/,""));
            var inputYear = parseInt(match[3]);
            if (inputMonth != parseInt(datePicker.month) || inputDay != parseInt(datePicker.day) || inputYear != datePicker.year) {
                return formatDateString(inputDay, inputMonth, inputYear);
            }
        }
        return datePickerRep;
    };

    SEARCH.util.getFilterQueryString = function() {
        var facetOptions = {}, domFacets = Dom.get("facet_options").children;
        for (var i = 0; i < domFacets.length; i++) {
            if (!Dom.hasClass(domFacets[i], "clearboth")) {
                var t = domFacets[i].innerHTML.split(" : ");
                var o = "\"" + t[1] + "\"";
                facetOptions[t[0]] = facetOptions.hasOwnProperty(t[0]) ? facetOptions[t[0]] + " " + o: o;
            }
        }

        var fqStr = "";
        if (Object.keys(facetOptions).length > 0) {
            for (var key in facetOptions) {
                fqStr += "%2B" + key + ":(" + SEARCH.util.encodeForRequest(facetOptions[key]) + ")";
            }
        }

        var startDate = getDateConstraint(SEARCH.ui.beginDateInput, SEARCH.ui.beginDatePick.getSelectedDay());
        var endDate = getDateConstraint(SEARCH.ui.endDateInput, SEARCH.ui.endDatePick.getSelectedDay());
        if (!(startDate == "*" && endDate == "*")) {
            fqStr += "%2B" + SEARCH.ui.dateConstraintField + ":[" + startDate + " TO " + endDate + "] ";
        }
        return fqStr;
    };

    SEARCH.util.jsonSyntaxHighlight = function (json) {
        json = JSON.stringify(json, undefined, 3);

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
    }
})();
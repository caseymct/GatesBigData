var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    var TextNode = YAHOO.widget.TextNode,      Overlay = YAHOO.widget.Overlay,
           Event = YAHOO.util.Event,           Tooltip = YAHOO.widget.Tooltip,
             Dom = YAHOO.util.Dom,              Button = YAHOO.widget.Button,
        TreeView = YAHOO.widget.TreeView,         Json = YAHOO.lang.JSON,
       Paginator = YAHOO.widget.Paginator,     TabView = YAHOO.widget.TabView,
     ButtonGroup = YAHOO.widget.ButtonGroup,
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

    SEARCH.ui.previewContainer = null;
    SEARCH.ui.previewImageName = "preview_image";
    SEARCH.ui.initPreviewContainer = function(containerEl) {
        SEARCH.ui.previewContainer = Dom.get(containerEl);
    };
    SEARCH.ui.setPreviewContainerImage = function(title, imgString) {
        Dom.setStyle(SEARCH.ui.previewContainer, "border", "1px solid gray");
        SEARCH.ui.previewContainer.innerHTML =
            "<span style='margin-left: 5px; font-size: 10px'>Document: <b>" + title + "</b>" +
                "<a style='float: right' class='button zoom_out' id='previewzoomout'></a>" +
                "<a style='float: right' class='button zoom_in' id='previewzoomin'></a>" +
                "</span><hr><br>" +
            "<img src=\"" + imgString + "\" height='400px' width='400px' id='preview_image'>";

        Dom.setStyle(SEARCH.ui.previewImageName, 'zoom', 2);
        Dom.setStyle(SEARCH.ui.previewImageName, 'margin-left', '-50px');

        Event.addListener('previewzoomin', 'click', function(e) {
            Dom.setStyle(SEARCH.ui.previewImageName, 'zoom', parseInt(Dom.getStyle(SEARCH.ui.previewImageName, 'zoom')) + 1);
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var zoom = parseInt(Dom.getStyle(SEARCH.ui.previewImageName, 'zoom'));
            Dom.setStyle(SEARCH.ui.previewImageName, 'zoom', (zoom > 1) ? zoom - 1 : zoom);
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
            column = dataTable.getColumn(target),
            data = record.getData();

        var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + data.HDFSSegment + "&file=" + data.HDFSKey;
        var callback = {
            success: function(o) {
                this.argument.table.updateCell(record, "thumbnail", o.responseText);
                SEARCH.ui.setPreviewContainerImage(record.getData().title, o.responseText);
            },
            argument: { table: dataTable, record: record}
        };

        if (column.getField() == "preview") {
            if (data.thumbnail == undefined) {
                SEARCH.ui.setPreviewContainerLoadingImage(loadingImg);
                YAHOO.util.Connect.asyncRequest('GET', thumbnailUrl + urlParams, callback);
            } else {
                SEARCH.ui.setPreviewContainerImage(data.title, data.thumbnail);
            }
        } else {
            SEARCH.ui.showTooltip(target, record, column, e.event.pageX, e.event.pageY);
        }
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

    /* Formatters for the search results table */
    SEARCH.ui.formatLink = function(el, record, column, data) {
        if (data == undefined) {
            data = "<i>No value</i>";
        } else if (data.length > 20) {
            data = data.substring(0, 20) + " ...";
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

        var adjustContentContainerHeight = function () {
            var oh = parseInt(Dom.getStyle("overlay", "height").replace("px", "")) + 300;
            var h = (oh > SEARCH.ui.contentContainerMinHeight) ? oh : SEARCH.ui.contentContainerMinHeight;
            Dom.setStyle(SEARCH.ui.contentContainer, "height", h + "px");
        };
        SEARCH.ui.facetTreeView.subscribe("expandComplete", adjustContentContainerHeight);
        SEARCH.ui.facetTreeView.subscribe("collapseComplete", adjustContentContainerHeight);

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

        return fqStr;
    };

})();
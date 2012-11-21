var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    var Event = YAHOO.util.Event,           Tooltip = YAHOO.widget.Tooltip,
          Dom = YAHOO.util.Dom,              Button = YAHOO.widget.Button,
         Json = YAHOO.lang.JSON,          Paginator = YAHOO.widget.Paginator,
      TabView = YAHOO.widget.TabView,   ButtonGroup = YAHOO.widget.ButtonGroup,
      Connect = YAHOO.util.Connect,    SimpleDialog = YAHOO.widget.SimpleDialog,
      LocalDS = YAHOO.util.LocalDataSource, ScrollingDataTable = YAHOO.widget.ScrollingDataTable;

    SEARCH.ui.longStringWidth       = 210;
    SEARCH.ui.shortStringWidth      = 80;
    SEARCH.ui.urlSearchParams       = "";
    SEARCH.ui.facetsFromLastSearch  = null;

    var colStringMaxChars      = 20,
        colDateStringMaxChars  = 30;

    var numFoundElName = "num_found",
        numFoundEl     = Dom.get(numFoundElName),
        numFound       = 0;

    var searchInputEls       = [],
        queryDefaultValue    = "*:*",
        selectData           = [],
        selectDataColumnDefs = [],
        dataSourceFields     = [],
        exportFileDomElName  = "",
        tooltip              = null,
        searchTab            = null,
        sortOrderButtonGroup = null,
        sortBySelect         = null,
        pag                  = null,
        rowsPerPage          = 10,
        sortBySelectElName   = "",
        initialSelectIndex   = 0,
        paginatorElName      = "";

    var exportUrl            = "",
        loadingImgUrl        = "",
        thumbnailUrl         = "",
        viewDocUrl           = "",
        searchUrl            = "",
        suggestUrl           = "";

    var previewContainerElName  = "",
        previewContainer        = null,
        previewDataName         = "preview_image",
        showQueryElName         = "show_query",
        showQueryEl             = Dom.get(showQueryElName),
        searchHeaderElName      = "";

    /* Set core name and header for this search file */
    var url = window.location.href.split('/');
    SEARCH.ui.coreName = url[url.length - 1];

    SEARCH.ui.initHTML = function(names) {
        searchTab = new TabView(names.searchTabElName);
        sortOrderButtonGroup = new ButtonGroup(names.sortOrderButtonGroupElName);
        sortOrderButtonGroup.check(1);

        searchHeaderElName      = names.searchHeaderElName;
        setSearchHeader();

        dataSourceFields        = names.dataSourceFields;
        previewContainerElName  = names.previewContainerElName;
        previewContainer        = Dom.get(previewContainerElName);

        searchInputEls          = UI.util.getDomElementsFromDomNames(names.searchInputElNames);

        paginatorElName         = names.paginatorElName;
        pag                     = new Paginator( { rowsPerPage : rowsPerPage, containers : [ paginatorElName ] });
        pag.subscribe('changeRequest', handlePagination);

        selectDataColumnDefs    = names.selectDataColumnDefs;
        initSelectData(names.selectDataRegexIgnore);

        sortBySelectElName      = names.sortBySelectElName;
        if (names.initialSelectIndex !== undefined) initialSelectIndex = names.initialSelectIndex;
        initSortBySelect();

        if (names.tooltipElName !== undefined) tooltip = new Tooltip(tooltipElName, { zIndex : 20 });
        if (names.exportFileName !== undefined) exportFileDomElName = names.exportFileName;
    };

    SEARCH.ui.initUrls = function(urls) {
        exportUrl       = urls.exportUrl;
        viewDocUrl      = urls.viewDocUrl;
        loadingImgUrl   = urls.loadingImgUrl;
        thumbnailUrl    = urls.thumbnailUrl;
        searchUrl       = urls.searchUrl;
    };

    SEARCH.ui.resetQuerySearchInputs = function() {
        for(var i = 0; i < searchInputEls.length; i++) {
            searchInputEls[i].value = queryDefaultValue;
        }
    };

    /* Formatters for the search results table */
    SEARCH.ui.formatLink = function(el, record, column, data) {
        var max = (column.label.match(/ Date/) != null) ? colDateStringMaxChars : colStringMaxChars;
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

    SEARCH.ui.search = function(fq) {
        UI.showWait();

        var queryTerms = getQueryTerms();
        SEARCH.ui.urlSearchParams = constructUrlSearchParams(queryTerms, fq, 0);
        updateSolrQueryDiv(queryTerms, fq);
        clearPreviewContainerImage();

        Connect.asyncRequest('GET', searchUrl + SEARCH.ui.urlSearchParams, {
            success : function(o) {
                UI.hideWait();

                var result = Json.parse(o.responseText);
                SEARCH.ui.facetsFromLastSearch = result.response.facets;

                updateNumFound(result.response.numFound, 1);
                updatePaginatorAfterSearch();

                buildSearchResultHtml(result);

                return numFound;
            },
            failure : function(o) {
                alert("Could not connect.");
            }
        });
        return -1;
    };

    function setSearchHeader() {
        Dom.get(searchHeaderElName).innerHTML = "Search Core: " + SEARCH.ui.coreName;
    }

    function updateNumFound (n, start) {
        numFound = n;
        var s = "Found " + numFound + " document" + ((numFound != 1) ? "s" : "");
        if (numFound > 0) {
            s += ", showing items " + start + " through " + Math.min(start + rowsPerPage - 1, numFound);
        }
        numFoundEl.innerHTML = s;
    }

    /* Paginator code */
    function updatePaginatorAfterSearch() {
        pag.set('totalRecords', numFound);
        pag.render();
        pag.setStartIndex(0);
    }

    /* Preview container code */
    function getPreviewHtmlTitleString(title) {
        var titleString = (title == undefined) ? "<b>Search result preview</b>" : "Document: <b>" + title + "</b>";
        return "<span style='margin-left: 5px; font-size: 10px'>" + titleString +
                "<a style='float: right' class='button zoom_out' id='previewzoomout'></a>" +
                "<a style='float: right' class='button zoom_in' id='previewzoomin'></a>" +
                "</span><hr>";
    }

    function setPreviewContainerImageData(title, dataStr) {
        var htmlStr = getPreviewHtmlTitleString(title);
        htmlStr += "<img src='" + dataStr + "' height='400px' width='400px' id='" + previewDataName + "'>";
        previewContainer.innerHTML = htmlStr;

        Dom.setStyle(previewDataName, 'zoom', 2);
        Event.addListener('previewzoomin', 'click', function(e) {
            Dom.setStyle(previewDataName, 'zoom', parseInt(Dom.getStyle(previewDataName, 'zoom')) + 1);
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var zoom = parseInt(Dom.getStyle(previewDataName, 'zoom'));
            Dom.setStyle(previewDataName, 'zoom', (zoom > 1) ? zoom - 1 : zoom);
        });
    }

    function setPreviewContainerTextData(title, dataStr) {
        var htmlStr = getPreviewHtmlTitleString(title);
        htmlStr += "<div id='" + previewDataName + "'>" + UI.util.jsonSyntaxHighlight(dataStr) + "</div>";
        previewContainer.innerHTML = htmlStr;

        Dom.setStyle(previewDataName, 'font-size', '10px');
        Event.addListener('previewzoomin', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataName, 'font-size'));
            Dom.setStyle(previewDataName, 'font-size', (px + 1) + 'px');
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataName, 'font-size'));
            Dom.setStyle(previewDataName, 'font-size', (px - 1) + 'px');
        });
    }

    function setPreviewContainerData(title, dataStr, dataType) {
        Dom.setStyle(previewContainer, "border", "1px solid gray");
        Dom.setStyle(previewContainer, "background-image", "none");

        if (dataType == "image") {
            setPreviewContainerImageData(title, dataStr);
        } else {
            setPreviewContainerTextData(title, dataStr);
        }
    }

    function setPreviewContainerLoadingImage(img) {
        previewContainer.innerHTML = "";
        Dom.setStyle(previewContainer, "background-image", "url(\"" + img + "\")");
        Dom.setStyle(previewContainer, "background-position", "50% 10%");
        Dom.setStyle(previewContainer, "background-repeat", "no-repeat");
    }

    function clearPreviewContainerImage() {
        Dom.setStyle(previewContainer, "border", "none");
        previewContainer.innerHTML = "";
    }

    /* Search tab */
    function getSearchTabActiveIndex() {
        return searchTab.get('activeIndex');
    }

    function setSearchTabToGeneralQuery() {
        searchTab.set('activeIndex', searchTab.get("tabs").length - 1);
    }

    function getQueryTerms() {
        var val = searchInputEls[getSearchTabActiveIndex()].value;
        return (searchTab == null || val == "") ? queryDefaultValue : val.encodeForRequest();
    }

    /* Sort order button code */
    function getSortOrder() {
        return UI.getButtonGroupCheckedButtonValue(sortOrderButtonGroup);
    }

    /* Tooltip code */
    function showTooltip(target, record, column, x, y) {
        //var target = oArgs.target;
        //var column = this.getColumn(target), record = this.getRecord(target);
        var s = target.innerText.replace(/\n$/, '');
        if (s.substring(s.length - 3) == "...") {
            SEARCH.ui.tooltip.setBody(record.getData()[column.getField()]);
            SEARCH.ui.tooltip.cfg.setProperty('xy', [x, y]);
            SEARCH.ui.tooltip.show();
        }
    }

    function hideTooltip() {
        tooltip.hide();
    }

    /* Export dialog code */
    Event.onContentReady(exportFileDomElName, function() {
        function exportFile(type, dlg) {
            window.open(exportUrl + "?type=" + type + "&file=" + Dom.get(exportFileDomElName).value);
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

    /* Field select code */
    function initSelectData(regexIgnore) {
        for(var i = 0; i < selectDataColumnDefs.length; i++) {
            var text = selectDataColumnDefs[i].key;
            if (regexIgnore == undefined || !text.match(regexIgnore)) {
                var split = text.split(".");
                if (split.length == 2) {
                    text += ".facet";
                }
                selectData.push({ text: split[split.length - 1], value: text });
            }
        }
    }

    function initSortBySelect() {
        sortBySelect = new Button({
            id          : "sortByMenu",
            name        : "sortByMenu",
            type        : "menu",
            lazyloadmenu: false,
            menu        : selectData,
            container   : sortBySelectElName
        });

        var sortBySelectMenu = sortBySelect.getMenu();
        sortBySelectMenu.subscribe("render", function (type, args, button) {
            button.set("selectedMenuItem", this.getItem(initialSelectIndex));
        }, sortBySelect);

        sortBySelect.on("selectedMenuItemChange", function (event) {
            this.set("label", ("<em class=\"yui-button-label\">" + event.newValue.cfg.getProperty("text") + "</em>"));
        });
    }

    function handlePagination(newState) {
        UI.showWait();
        var newStart = newState.records[0], newStartStr = "&start=" + newStart;
        var match = SEARCH.ui.urlSearchParams.match(/(&start=[0-9]+)/);

        SEARCH.ui.urlSearchParams = (match == null) ? SEARCH.ui.urlSearchParams + newStartStr :
                                     SEARCH.ui.urlSearchParams.replace(match[1], newStartStr);
        updateNumFound(numFound, newStart);
        Connect.asyncRequest('GET', searchUrl + SEARCH.ui.urlSearchParams, {
            success: function(o) {
                UI.hideWait();
                buildSearchResultHtml(Json.parse(o.responseText));
            }
        });

        pag.setState(newState);
    }

    function updateSolrQueryDiv (queryTerms, fq) {
        showQueryEl.innerHTML = "Submitting to solr with<br>q: " + queryTerms + "<br>fq: " + decodeURIComponent(fq);
    }

    function constructUrlSearchParams(queryTerms, fq, start) {
        var sortType = sortBySelect.get("selectedMenuItem").value,
            coreName = SEARCH.ui.coreName,
            sortOrder = getSortOrder();

        var urlParams = "?query=" + queryTerms + "&core=" + coreName + "&sort=" + sortType + "&order=" + sortOrder;
        if (fq != "") {
            urlParams += "&fq=" + fq;
        }
        if (start != undefined && start != "") {
            urlParams += "&start=" + start;
        }

        return urlParams;
    }

    /* Data table event handlers */
    function dataTableCellMouseoverEvent(e, dataTable) {
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
                setPreviewContainerData(record.getData().title, response.Contents, response.contentType);
            },
            argument: { table: dataTable, record: record}
        };

        if (data.thumbnail == undefined) {
            setPreviewContainerLoadingImage(loadingImgUrl);
            Connect.asyncRequest('GET', thumbnailUrl + urlParams, callback);
        } else {
            setPreviewContainerData(data.title, data.thumbnail, data.thumbnailType);
        }
    }

    function buildSearchResultHtml(result) {
        var dataSource = new LocalDS(result.response, {
            responseSchema : {
                resultsList:'docs',
                fields: dataSourceFields
            }
        });

        var dataTable = new ScrollingDataTable('search_results', selectDataColumnDefs, dataSource, { width:"100%" });

        dataTable.subscribe("sortedByChange", function(e) {
            var sortField = e.newValue.column.key;
            var index = -1;
            for(var i = 0; i < selectData.length; i++) {
                if (selectData[i].value.replace(/\.facet$/, "") == sortField) {
                    index = i;
                    break;
                }
            }
            sortBySelect.set("selectedMenuItem", sortBySelect.getMenu().getItem(index));
            pag.setStartIndex(0);

            SEARCH.ui.search();
        });

        dataTable.subscribe("cellClickEvent", function (e) {
            Event.stopEvent(e.event);
            var data = this.getRecord(e.target).getData();

            window.open(viewDocUrl + "?core=" + SEARCH.ui.coreName + "&segment=" +
                data.HDFSSegment + "&file=" + data.HDFSKey + "&view=preview", "_blank");
        });

        dataTable.on('rowMouseoverEvent', function (e) {
            dataTableCellMouseoverEvent(e, dataTable, loadingImgUrl, thumbnailUrl);
        });
    }

})();
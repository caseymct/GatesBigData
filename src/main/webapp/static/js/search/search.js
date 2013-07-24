var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    /* Set core name and header for this search file */
    var url = window.location.href.split('/'), lastUrlComponent = url[url.length - 1],
        idx = lastUrlComponent.indexOf('?');
    SEARCH.ui.coreName = (idx == -1) ? lastUrlComponent : lastUrlComponent.substring(0, idx);

    SEARCH.ui.longStringWidth  = 210;
    SEARCH.ui.shortStringWidth = 80;

    var Event   = YAHOO.util.Event,     Tooltip     = YAHOO.widget.Tooltip,
        Dom     = YAHOO.util.Dom,       DataTable   = YAHOO.widget.ScrollingDataTable,
        Json    = YAHOO.lang.JSON,      Paginator   = YAHOO.widget.Paginator,
        TabView = YAHOO.widget.TabView, ButtonGroup = YAHOO.widget.ButtonGroup,
        Connect = YAHOO.util.Connect,   LocalDS     = YAHOO.util.LocalDataSource,
        Button  = YAHOO.widget.Button,  Panel       = YAHOO.widget.Panel;

    var rowsPerPage           = 10,                  queryDefaultValue           = '*:*',

        // button IDs
        submitButtonElId      = 'submit',            resetButtonElId             = 'reset',
        exportButtonElId      = 'export',            analyzeButtonElId           = 'analyze',
        reportsButtonElId     = 'reports',

        // other DOM IDs
		previewDataId         = 'preview_image',     showQueryElId               = 'show_query',
        paginatorElId         = 'pag',               searchResultContainerElId   = 'search_result_container',
        numFoundElId          = 'num_found',         sortOrderButtonGroupElId    = 'sort_ascdesc_buttongroup',
        sortByDivElId         = 'sort_by_div',       searchResultsElId           = 'search_results',
        previewContainerElId  = 'preview_container', sortBySelectElId            = 'sort_date_label',
        showQueryElIdBody     = 'show_query_text',   showQueryButtonElId         = 'show_query_button',
        previewZoomInElId     = 'preview_zoom_in',   previewZoomOutElId          = 'preview_zoom_out',
        previewRecordRowDivElId = 'preview_row',     previewRecordsElId          = 'preview_records',

        // solr
        solrDocIdFieldName    = 'id',
        solrDocContentsKey    = 'content',           solrDocContentTypeKey       = 'content_type',
        thumbnailKey          = 'thumbnail',         thumbnailTypeKey            = 'thumbnail_type',

        //preview
        noPreviewText         = "No preview available",

        // CSS variables
        searchResultSubheadingCSSClass  = 'search_subheading',      searchResultElWidthStructured   = '56%',
        searchResultDivCSSClass         = 'search_result_div',      searchResultElWidthUnstructured = '100%',   
        yuiButtonLabelCSSClass          = 'yui-button-label',       previewDivCSSClass              = 'preview_result_div',
        buttonZoomOutCSSClass           = 'button zoom_out',        buttonZoomInCSSClass           = 'button zoom_in',
        previewLoadingCSSClass          = 'preview_loading';

    var searchInputEls                  = [],       dataSourceFields                = [],
        thumbnailData                   = {},       initialSelectIndex              = 0,
        facetsFromLastSearch            = null,     docsFromLastSearch              = null,
        numFound                        = 0,        urlSearchParams                 = "",

        // URLs
        thumbnailUrl  = "", analyzeUrl = "", viewDocUrl    = "", searchUrl     = "", reportsUrl = "",

        // set in init
        unstructuredData                = false,    displayName                     = "",
        searchTabElId                   = "",       searchHeaderElName              = "",
        insertSearchResultsAfterElName  = "",       insertSortButtonsAfterElName    = "",
        insertSearchButtonsAfterElName  = "",       selectData                      = [],
        selectDataColumnDefs            = [],       selectDataRegexIgnore           = "",
        searchResultContainerElWidth    = "",       showQueryPanel                  = null,

        // widgets
        paginator                       = null,     currPreviewContainerEl          = null,
        searchTab                       = null,     sortBySelect                    = null,
        sortOrderButtonGroup            = null,     tooltip                         = null,

        // functions
        buildSearchResultHtmlFn         = buildSearchResultDataTableHtml,
        submitFn                        = null,
        resetFn                         = null,
        getFilterQueryFn                = null,
        updateFacetFn                   = null,
        formatSearchResultFn            = function(a) { return a;};


    SEARCH.ui.init = function(names) {
        displayName           = names[UI.DISPLAY_NAME_KEY];
        dataSourceFields      = names[UI.SEARCH.DATA_SOURCE_FIELDS_KEY];
        selectDataColumnDefs  = names[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY];
        selectDataRegexIgnore = names[UI.SEARCH.SELECT_DATA_REGEX_IGNORE_KEY];
        searchTabElId         = names[UI.SEARCH.SEARCH_TAB_EL_NAME_KEY];
        searchHeaderElName    = names[UI.SEARCH.SEARCH_HEADER_EL_NAME_KEY];
        searchInputEls        = names[UI.SEARCH.SEARCH_INPUT_ELS_KEY];

        insertSearchResultsAfterElName = names[UI.SEARCH.INSERT_SEARCH_RESULTS_AFTER_EL_NAME_KEY];
        insertSortButtonsAfterElName   = names[UI.SEARCH.INSERT_SORT_BUTTONS_AFTER_EL_NAME_KEY];
        insertSearchButtonsAfterElName = names[UI.SEARCH.INSERT_SEARCH_BUTTONS_AFTER_EL_NAME_KEY];

        submitFn             = names[UI.SEARCH.SUBMIT_FN_KEY];
        resetFn              = names[UI.SEARCH.RESET_FN_KEY];
        getFilterQueryFn     = names[UI.SEARCH.GET_FILTER_QUERY_FN_KEY];
        updateFacetFn        = names[UI.FACET.UPDATE_FACET_FN];
        formatSearchResultFn = UI.util.returnIfDefined(formatSearchResultFn, names[UI.SEARCH.FORMAT_SEARCH_RESULT_FN_KEY]);

        var urls      = names[UI.URLS_KEY];
        viewDocUrl    = urls[UI.VIEW_DOC_URL_KEY];
        thumbnailUrl  = urls[UI.THUMBNAIL_URL_KEY];
        searchUrl     = urls[UI.SEARCH_URL_KEY];
        analyzeUrl    = urls[UI.ANALYZE_URL_KEY];
        reportsUrl    = urls[UI.REPORTS_URL_KEY];

        initialSelectIndex = UI.util.returnIfDefined(initialSelectIndex, names[UI.SEARCH.INITIAL_SELECT_INDEX_KEY]);
        if (names[UI.SEARCH.TOOLTIP_EL_NAME_KEY] !== undefined) {
            initToolTip(names[UI.SEARCH.TOOLTIP_EL_NAME_KEY]);
        }

        unstructuredData = (names[UI.DATA_TYPE_KEY] == UI.DATA_TYPE_UNSTRUCTURED);
        searchResultContainerElWidth = unstructuredData ? searchResultElWidthUnstructured : searchResultElWidthStructured;
        if (unstructuredData) {
            buildSearchResultHtmlFn = buildSearchResultList;
        }

        buildHTML();
        searchIfInitialQuerySpecified();
    };

    function buildHTML() {
        // sort order buttongroup code
        var sortButtonsInsertAfterEl = Dom.get(insertSortButtonsAfterElName);
        var el = UI.insertDomElementAfter('div', sortButtonsInsertAfterEl, { id: sortByDivElId }, { "class" :  "row" });
        var div = UI.addDomElementChild('div', el);
        UI.addDomElementChild('label', div, { id: sortBySelectElId, innerHTML: "Order by: " });
        div = UI.addDomElementChild('div', el, { id: sortOrderButtonGroupElId }, { "class" :  "yui-buttongroup search-button-style" });
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "asc", id: "sort_asc"});
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "desc", id: "sort_desc", checked: "checked"});
        UI.addDomElementChild('div', el, null, { "class" :  "clearboth" });

        // Search result HTML
        var searchResultsInsertAfterEl = Dom.get(insertSearchResultsAfterElName);
        el = UI.insertDomElementAfter('div', searchResultsInsertAfterEl);
        div = UI.addDomElementChild('div', el, { id : searchResultContainerElId }, { width: searchResultContainerElWidth } );

        UI.addDomElementChild('div', div, { id : numFoundElId });
        UI.addDomElementChild('div', div, { id : searchResultsElId });
        UI.addDomElementChild('div', el,  { id : previewContainerElId });
        UI.addClearBothDiv(el);

        UI.insertDomElementAfter('div', searchResultsInsertAfterEl, { id: paginatorElId });

        // Show solr query
        UI.insertDomElementAfter('a', searchResultsInsertAfterEl, { id : showQueryButtonElId, innerHTML: "Show Solr Query" },
            { 'class' : 'button small' } );
        var showQueryEl = UI.addDomElementChild('div', Dom.get("collections_tab_content"), { id : showQueryElId });
        UI.addDomElementChild('div', showQueryEl, { innerHTML: "Query submitted to Solr"}, { "class" : "hd" } );
        UI.addDomElementChild('div', showQueryEl, { id : showQueryElIdBody }, { "class" : "bd" } );

        // Build buttons
        var searchButtonsInsertAfterEl = Dom.get(insertSearchButtonsAfterElName);
        el = UI.insertDomElementAfter('div', searchButtonsInsertAfterEl, null, { "class" :  "buttons", "padding-bottom" : "5px" });
        UI.addDomElementChild('a', el, { id: submitButtonElId,  innerHTML: "Search",  href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: resetButtonElId,   innerHTML: "Reset",   href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: exportButtonElId,  innerHTML: "Export",  href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: analyzeButtonElId, innerHTML: "Analyze", href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: reportsButtonElId, innerHTML: "Reports", href: "#"}, { "class" :  "button small"});

        UI.insertDomElementAfter('div', el, null, { "class" :  "row" });

        currPreviewContainerEl = Dom.get(previewContainerElId);

        setSearchHeader();
        initSearchTab();
        initSelectData(selectDataRegexIgnore);
        initSortBySelect();
        initControlButtonListeners();
        initPaginator();
        initSortOrderButtonGroup();
        initShowQueryPanel();
    }

    function searchIfInitialQuerySpecified() {
        var initialQuery = YAHOO.deconcept.util.getRequestParameter(UI.INITIAL_QUERY_KEY);
        if (initialQuery == '') return;

        Event.onContentReady(UI.FACET.FACET_OPTIONS_DIV_EL_NAME, function() {
            searchInputEls[getSearchTabActiveIndex()].value = decodeURIComponent(initialQuery);
            SEARCH.ui.search();
        });
    }

    SEARCH.ui.getUrlSearchParams = function() {
        return urlSearchParams;
    };

    SEARCH.ui.getExportButtonElId = function() {
        return exportButtonElId;
    };

    SEARCH.ui.resetQuerySearchInputs = function() {
        for(var i = 0; i < searchInputEls.length; i++) {
            searchInputEls[i].value = queryDefaultValue;
        }
    };

    function updateDocsFromLastSearch(docs) {
        var i, docId, doc;
        docsFromLastSearch = {};

        for(i = 0; i < docs.length; i++) {
            doc = docs[i];
            docId = doc.id;
            delete doc.id;
            delete doc.url;
            docsFromLastSearch[docId] = doc;
        }
    }

    var searchSuccessCallback = function(o) {
        UI.hideWait();
        var result = Json.parse(o.responseText);
        facetsFromLastSearch = UI.util.getSolrResponseFacets(result);   //result[solrResponseKey][solrResponseFacetKey];
        buildSearchResultHtmlFn(result);

        updateSolrQueryDiv(decodeURIComponent(result.response.q));
        updateDocsFromLastSearch(UI.util.getSolrResponseDocs(result));  //result[solrResponseKey][solrResponseDocsKey]);
        updateNumFound(UI.util.getSolrResponseNumFound(result), 1);     //result[solrResponseKey][solrResponseNumFoundKey], 1);
        updatePaginatorAfterSearch();

        if (numFound > 0 && updateFacetFn != null) {
            updateFacetFn(facetsFromLastSearch);
        }
    };

    var searchFailureCallback = function() {
        alert("Could not connect.");
    };

    SEARCH.ui.search = function() {
        UI.showWait();

        //var fq = getFilterQueryFn();
        //var queryTerms = getQueryTerms();
        urlSearchParams = constructUrlSearchParams();

        clearPreviewContainerImage();

        Connect.asyncRequest('GET', searchUrl + urlSearchParams, {
            success : searchSuccessCallback,
            failure : searchFailureCallback
        });
    };

    SEARCH.ui.getUrlSearchParams = function() {
        return (urlSearchParams == "") ? "" : urlSearchParams + "&" + UI.util.REQUEST_NUM_FOUND_KEY + "=" + numFound;
    };

    function initControlButtonListeners() {
        Event.addListener(resetButtonElId, "click", function (e) {
            Event.stopEvent(e);
            resetFn();
        });

        Event.addListener(submitButtonElId, "click", function (e) {
            Event.stopEvent(e);
            submitFn();
        });

        Event.addListener(analyzeButtonElId, "click", function (e) {
            Event.stopEvent(e);
            var url = analyzeUrl + (unstructuredData ? "/wordtree" + SEARCH.ui.getUrlSearchParams() :
                                                       SEARCH.ui.getUrlSearchParams() + '&structured=true');
            window.open(url, "_blank");
        });

        Event.addListener(reportsButtonElId, "click", function (e) {
            Event.stopEvent(e);
            window.open(reportsUrl + SEARCH.ui.getUrlSearchParams(), "_blank");
        });
    }

    function initShowQueryPanel() {
        showQueryPanel = new Panel(showQueryElId, { width : "500px", visible : false, constraintoviewport: true } );
        showQueryPanel.render();

        Event.addListener(showQueryButtonElId, "click", function(o) { showQueryPanel.show(); });
    }

    function initSearchTab() {
        searchTab = new TabView(searchTabElId);
    }

    function initToolTip(tooltipElName) {
        tooltip = new Tooltip(tooltipElName, { zIndex : 20 });
    }
    
    function initSortOrderButtonGroup() {
        sortOrderButtonGroup = new ButtonGroup(sortOrderButtonGroupElId);
        sortOrderButtonGroup.check(1);
    }

    function initPaginator() {
        paginator = new Paginator( { rowsPerPage : rowsPerPage, containers : [ paginatorElId ] });
        paginator.subscribe('changeRequest', handlePagination);
    }

    function setSearchHeader() {
        Dom.get(searchHeaderElName).innerHTML = "Search " + displayName;
    }

    function updateNumFound (n, start) {
        numFound = n;
        var s = "Found " + numFound + " document" + ((numFound != 1) ? "s" : "");
        if (numFound > 0) {
            s += ", showing items " + start + " through " + Math.min(start + rowsPerPage - 1, numFound);
        }
        Dom.get(numFoundElId).innerHTML = s;
    }

    /* Paginator code */
    function updatePaginatorAfterSearch() {
        paginator.set('totalRecords', numFound);
        paginator.render();
        paginator.setStartIndex(0);
    }

    /* Preview container code */
    function changeFontSize(f) {
        var px = UI.util.getNPixels(Dom.getStyle(previewDataId, 'font-size'));
        UI.setStyleOnElementId(previewDataId, 'font-size', (px + f) + 'px');
    }

    function imageZoom(f) {
        UI.setStyleOnElementId(previewDataId, 'zoom', Math.max(parseInt(Dom.getStyle(previewDataId, 'zoom')) + f, 1));
    }

    function addPreviewTitleString(title) {
        var titleString = 'Document ' + UI.util.returnEmptyIfUndefined(title) + ' preview';
        var span = UI.addDomElementChild('span', currPreviewContainerEl, { innerHTML : titleString });
        UI.addDomElementChild('a', span, { id : previewZoomOutElId }, { 'class' : buttonZoomOutCSSClass });
        UI.addDomElementChild('a', span, { id : previewZoomOutElId }, { 'class' : buttonZoomInCSSClass });
        UI.addDomElementChild('hr', currPreviewContainerEl);
    }

    function setPreviewContainerImageData(title, dataStr) {
        addPreviewTitleString(title);
        UI.addDomElementChild('img', currPreviewContainerEl, { src : dataStr, id: previewDataId }, { zoom : 2 });

        Event.addListener(previewZoomInElId, 'click', function(e) { imageZoom(1)  });
        Event.addListener(previewZoomInElId, 'click', function(e) { imageZoom(-1) });
    }

    function setPreviewContainerTextData(title, dataStr) {
        addPreviewTitleString(title);

        var d = UI.addDomElementChild('div', currPreviewContainerEl, { id: previewDataId });
        var d2 = UI.addDomElementChild('div', d, { id: previewRecordsElId });
        if (dataStr == noPreviewText) {
            d.innerHTML = dataStr;
        } else {
            var keys = Object.keys(dataStr);
            for(var i = 0; i < keys.length; i++) {
                var key = keys[i], val = dataStr[keys[i]];

                var d3 = UI.addDomElementChild('div', d2, { id : previewRecordRowDivElId });
                UI.addDomElementChild('div', d3, { innerHTML : key });
                UI.addDomElementChild('div', d3, { innerHTML : val },
                    { 'class' : isNaN(Number(val)) ? (val.match(/true|false/) ? 'boolean' : 'string') : 'number' });

            }
            UI.addClearBothDiv(d2);
        }

        Event.addListener(previewZoomInElId,  'click', changeFontSize(1) );
        Event.addListener(previewZoomOutElId, 'click', function() { changeFontSize(-1); });
    }

    function setPreviewContainerData(title, dataStr, dataType) {
        UI.removeDivChildNodes(currPreviewContainerEl);

        UI.setStyleOnElement(currPreviewContainerEl, "border", "1px solid gray");
        UI.setStyleOnElement(currPreviewContainerEl, "background-image", "none");

        if (dataStr == "" || dataStr == undefined) {
            dataType = "text";
            dataStr  = noPreviewText;
        }

        if (dataType == UI.util.CONTENT_TYPES.image) {
            //console.log(arguments.callee.caller.toString());
            setPreviewContainerImageData(title, dataStr);
        } else {
            setPreviewContainerTextData(title, dataStr);
        }
    }

    function setPreviewContainerLoadingImage() {
        currPreviewContainerEl.innerHTML = "";
        UI.addCSSClass(currPreviewContainerEl, previewLoadingCSSClass);
    }

    function clearPreviewContainerImage() {
        UI.setStyleOnElement(currPreviewContainerEl, "border", "none");
        currPreviewContainerEl.innerHTML = "";
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
        if (searchTab == null || val == "") {
            return queryDefaultValue;
        }
        var match = val.match(/(.*:")(.*?)"$/);
        if (match != null) {
            val = match[1] + match[2].replace(/"/g, '\\"') + '"';
        }
        return val.encodeForRequest();
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
            container   : sortBySelectElId
        });

        var sortBySelectMenu = sortBySelect.getMenu();
        sortBySelectMenu.subscribe("render", function (type, args, button) {
            button.set("selectedMenuItem", this.getItem(initialSelectIndex));
        }, sortBySelect);

        sortBySelect.on("selectedMenuItemChange", function (e) {
            var newVal = e.newValue.cfg.getProperty('text');
            this.set('label', ('<em class="' + yuiButtonLabelCSSClass + '">' + newVal + '</em>'));

            var oldVal = e.prevValue;
            if (docsFromLastSearch != null && oldVal != null && oldVal.cfg.getProperty('text') != newVal) {
                SEARCH.ui.search();
            }
        });
    }

    function handlePagination(newState) {
        UI.showWait();
        var newStart = newState.records[0], newStartStr = "&start=" + newStart;
        updateNumFound(numFound, newStart);

        var match = urlSearchParams.match(/(&start=[0-9]+)/);
        urlSearchParams = (match == null) ? urlSearchParams + newStartStr : urlSearchParams.replace(match[1], newStartStr);

        Connect.asyncRequest('GET', searchUrl + urlSearchParams, {
            success: function(o) {
                UI.hideWait();

                var result = Json.parse(o.responseText);
                buildSearchResultHtmlFn(result);
                updateDocsFromLastSearch(UI.util.getSolrResponseDocs(result)); //result[solrResponseKey][solrResponseDocsKey]);
            }
        });

        paginator.setState(newState);
    }

    function updateSolrQueryDiv (params) {
        var s = [], paramArgs = params.split("&");
        for(var i = 0; i < paramArgs.length; i++) {
            if (paramArgs[i].match(/^(facet|fl|hl)/) == null) {
                s.push(paramArgs[i] + '<br>');
            }
        }
        UI.changeVisibility(showQueryButtonElId, true);
        Dom.get(showQueryElIdBody).innerHTML = s.join('&');
    }

    function constructUrlSearchParams() {
        var params = [
            {key : UI.util.REQUEST_QUERY_KEY, value : getQueryTerms()},
            {key : UI.util.REQUEST_COLLECTION_KEY,  value : SEARCH.ui.coreName},
            {key : UI.util.REQUEST_SORT_KEY, value : sortBySelect.get("selectedMenuItem").value},
            {key : UI.util.REQUEST_ORDER_KEY, value : getSortOrder()},
            {key : UI.util.REQUEST_START_KEY, value : 0}
        ];
        var fq = getFilterQueryFn();
        if (fq != '') {
            params.push({key : UI.util.REQUEST_FQ_KEY, value : fq});
        }

        return UI.util.constructRequestString([], [], params);
    }

    /* Data table event handlers */
    function dataTableCellMouseoverEvent(e, dataTable) {
        var target = e.target;
        var record = dataTable.getRecord(target),
              data = record.getData(),
            dataId = UI.util.returnEmptyIfUndefined(data.id);

        if (data[thumbnailKey] == undefined) {
            dataTable.updateCell(record, thumbnailKey, docsFromLastSearch[dataId]);
            dataTable.updateCell(record, thumbnailTypeKey, UI.util.CONTENT_TYPES.json);
        }

        setPreviewContainerData(data.title, data[thumbnailKey], data[thumbnailTypeKey]);
    }


    function buildSearchResultDataTableHtml(result) {
        var dataSource = new LocalDS(formatSearchResultFn(result.response), {
            responseSchema : {
                resultsList:'docs',
                fields: dataSourceFields
            }
        });

        var dataTable = new DataTable(searchResultsElId, selectDataColumnDefs, dataSource, { width:"100%" });
        dataTable.subscribe("cellClickEvent", function (e) {
            Event.stopEvent(e.event);

            var data = this.getRecord(e.target).getData();
            var urlParams = "?collection=" + SEARCH.ui.coreName + "&id=" + data.id + "&view=preview";
            window.open(viewDocUrl + urlParams, "_blank");
        });

        dataTable.on('rowMouseoverEvent', function (e) {
            dataTableCellMouseoverEvent(e, dataTable);
        });
    }

    function getIndexFromSearchResultDivId(id)  { return id.match("searchResult([0-9]+)")[1]; }
    function constructSearchResultDivId(i)      { return "searchResult" + i; }
    function constructPreviewDivId(i)           { return "preview" + i; }
    function constructLinkDivId(i)              { return "link" + i; }

    function searchResultListMouseEnter(e) {
        clearPreviewContainerImage();

        var idx     = getIndexFromSearchResultDivId(this.id);
        var href    = Dom.get(constructLinkDivId(idx)).href;
        var file    = href.match("id=([^&]+)(&|$)")[1];
        var segment = href.match("segment=([^&]+)(&|$)")[1];
        var title   = file.substring(file.lastIndexOf("/") + 1);

        currPreviewContainerEl = Dom.get(constructPreviewDivId(idx));

        if (thumbnailData[file] == undefined) {
            setPreviewContainerLoadingImage();
            var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + segment + "&id=" + file;

            Connect.asyncRequest('GET', thumbnailUrl + urlParams, {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    thumbnailData[file] = {};
                    thumbnailData[file][solrDocContentsKey]    = response[solrDocContentsKey];
                    thumbnailData[file][solrDocContentTypeKey] = response[solrDocContentTypeKey];

                    setPreviewContainerData(title, response[solrDocContentsKey], response[solrDocContentTypeKey]);
                }
            });

        } else {
            setPreviewContainerData(title, thumbnailData[file][solrDocContentsKey], thumbnailData[file][solrDocContentTypeKey]);
        }
    }

    function buildSearchResultList(result) {
        var searchResultsEl = Dom.get(searchResultsElId);
        searchResultsEl.innerHTML = "";

        var docs      = UI.util.getSolrResponseDocs(result),
            highlight = UI.util.getSolrResponseHighlighting(result);

        for(var i = 0; i < docs.length; i++) {
            var doc               = docs[i],
                titleHref         = viewDocUrl + "?view=preview&collection=" + SEARCH.ui.coreName + "&id=" + doc[solrDocIdFieldName],
                titleId           = constructLinkDivId(i),
                searchResultDivId = constructSearchResultDivId(i),
                previewDivId      = constructPreviewDivId(i);

            var pdiv   = UI.addDomElementChild("div", searchResultsEl);

            var div   = UI.addDomElementChild("div", pdiv, { id: searchResultDivId },
                { "class" :  searchResultDivCSSClass, width: "56%", float: "left" });
            var title = UI.addDomElementChild("div", div);
            UI.addDomElementChild("a", title, { id: titleId, href: titleHref, innerHTML: doc['title'] }, { color: "blue" });
            UI.addDomElementChild("div", div, { innerHTML: "Author: <b>" + doc['author'] + "</b>"},
                { "class" :  searchResultSubheadingCSSClass });
            UI.addDomElementChild("div", div, { innerHTML: "Content type: <b>" + doc[solrDocContentTypeKey] + "</b>"},
                { "class" :  searchResultSubheadingCSSClass });
            UI.addDomElementChild("div", div, { innerHTML: "Created On: <i>" + doc['creation_date'] + "</i>, " +
                    "Last Modified: <i>" + doc['last_modified'] + "</i>"},
                { "class" :  searchResultSubheadingCSSClass});

            var highlightObj = highlight[doc[solrDocIdFieldName]];
            Object.keys(highlightObj).forEach(function(key) {
                UI.addDomElementChild("div", div, { innerHTML: "Field <b>" + key + "</b>"},
                    { "class" :  searchResultSubheadingCSSClass, color: "darkgreen"});
                highlightObj[key].forEach(function(s) {
                    UI.addDomElementChild("div", div, { innerHTML: "... " + s + "... "}, { "class" : "search_result"});
                })
            });

            UI.addDomElementChild("div", pdiv, { id: previewDivId }, { "class" :  previewDivCSSClass, float: "left" });
            UI.addClearBothDiv(pdiv);

            Event.addListener(titleId, "click", function(e) {
                Event.stopEvent(e);
                window.open(this.href, "_blank");
            });

            Event.addListener(searchResultDivId, "mouseenter", searchResultListMouseEnter);
        }
    }

})();
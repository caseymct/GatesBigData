<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/reports.css"/>" />
    <!--<link rel="stylesheet" href="<c:url value="/static/css/jquery/jquery-ui.css" />" type="text/css" />
    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery-1.9.1.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery-ui.js"/>"></script>    -->

    <script type="text/javascript">
        (function() {

            /* Variables */
            var Dom          = YAHOO.util.Dom,            Button          = YAHOO.widget.Button,
                Connect      = YAHOO.util.Connect,        Event           = YAHOO.util.Event,
                Paginator    = YAHOO.widget.Paginator,    LocalDataSource = YAHOO.util.LocalDataSource,
                Json         = YAHOO.lang.JSON,           DataTable       = YAHOO.widget.ScrollingDataTable,
                Overlay      = YAHOO.widget.Overlay;

            var requestParams           = UI.util.getRequestParameters(),
                reportRequestParamKeys  = [UI.util.REQUEST_COLLECTION_KEY, UI.util.REQUEST_FQ_KEY, UI.util.REQUEST_QUERY_KEY],
                searchRequestParamKeys  = [UI.util.REQUEST_COLLECTION_KEY, UI.util.REQUEST_QUERY_KEY],
                infoDivKeys             = [UI.util.REQUEST_QUERY_KEY, UI.util.REQUEST_FQ_KEY, UI.util.REQUEST_NUM_FOUND_KEY, UI.util.REQUEST_SORT_KEY],
                requestString           = UI.util.constructRequestString(requestParams, reportRequestParamKeys),
                collection              = requestParams[UI.util.REQUEST_COLLECTION_KEY],
                origFq                  = decodeURIComponent(UI.util.specifyReturnValueIfUndefined(requestParams[UI.util.REQUEST_FQ_KEY], '')),
                origNumFound            = UI.util.specifyReturnValueIfUndefined(requestParams[UI.util.REQUEST_NUM_FOUND_KEY], 0),
                origSortField           = UI.util.specifyReturnValueIfUndefined(requestParams[UI.util.REQUEST_SORT_KEY], 'score'),
                origSortOrder           = UI.util.specifyReturnValueIfUndefined(requestParams[UI.util.REQUEST_ORDER_KEY], 'asc');


            var baseUrl             = '<c:url value="/" />',
                exportUrl           = baseUrl + 'export',
                searchUrl           = baseUrl + 'report/search',
                metricsUrl          = baseUrl + 'report/data/metrics',
                reportDataUrl       = baseUrl + 'report/data?collection=' + collection,
                dateRangeUrl        = baseUrl + 'report/data/dateranges?collection=' + collection,
                filtersUrl          = baseUrl + 'report/data/filters' + requestString;

            var infoElId                    = 'info',                       contentContainerElId        = 'content_container',
                searchResultsElId           = 'search_results',             filterDivElId               = 'filter_div',
                rowsInputDivElId            = 'rows_input',
                appliedFiltersElId          = 'applied_filter_div',         filterButtonElId            = 'add_filter',
                filterSelectElId            = 'filter_select',              filterSubSelectElId         = 'filter_subselect',

                currentFiltersElId          = 'current_filters',            currentFiltersContainerElId = 'current_filters_container',

                sortFilterButtonElId        = 'add_sort_filter',            sortFilterElId              = 'sort_filter_div',
                sortFiltersElId             = 'sort_filters',               sortFiltersContainerElId    = 'sort_filters_container',
                sortBySelectElId            = 'sort_by_select',             sortOrderSelectElId         = 'sort_order_select',

                dateFieldFilterElId         = 'date_field_filter',          dateFilterFieldSelectElId   = 'date_filter_field_select',
                dateFilterFromToDivElId     = 'date_filter_from_to_div',    dateFilterFromElId          = 'date_filter_from',
                dateFilterToElId            = 'date_filter_to',             dateFilterButtonElId        = 'date_filter_add',
                accordionDivElId            = 'accordion',                  accordionDivEl              = null,

                metricsElId                 = 'metrics_div',                metricsFieldsetElId         = 'metrics_fieldset',

                applyFiltersButtonElId      = 'apply',                      exportButtonElId            = 'export',
                headerElId                  = 'report_header',
                paginatorElId               = 'pag',                        numFoundElId                = 'num_found',

                infoRowHdrCSSClass      = 'info_row_hdr',
                infoRowCSSClass             = 'info_row',                   infoDivHeaderCSSClass       = 'info_div_header',
                infoDivValueCSSClass        = 'info_div_value',             accordionHdrCSSClass        = 'accordion_hdr',
                buttonDisabledCSSClass      = 'button disabled',            buttonEnabledCSSClass       = 'button small',
                infoDivCSSClass             = 'info_div',                   metricsInfoDivCSSClass      = 'metrics_info_div',
                metricsInfoHeaderCSSClass   = 'metrics_info_div metrics_info_div_header',
                metricsFirstColCSSClass     = 'metrics_info_div metrics_info_div_header metrics_info_div_wide metrics_info_div_first_col',
                metricsRowCSSClass          = 'metrics_info_container_div',

                sortorders                  = ['asc', 'desc'],              rowsPerPage                 = 30,
                contentContainer = Dom.get(contentContainerElId),           contentEl = Dom.get(UI.CONTENT_EL_NAME);

            var displayFields = [], dataSourceFields = [], columnDefs = [], filters = [], datefilters = [], sortfilters = [],
                namesToDisplayNames = {}, dateRangeObject = {}, facets = {}, exportfields = [], reportTitle = '';
            var paginator, solrResponse, reportFq = '', reportData, reportSortInfo = '', numFound = origNumFound;

            /* Code to execute */
            UI.initWait(baseUrl);
            UI.showWait();

            UI.changeFacetContainerVisibility(false);
            buildHtml();

            Event.onDOMReady(function(e) {
                Connect.asyncRequest('GET', reportDataUrl, {
                    success: function(o) {
                        reportData  = Json.parse(o.responseText);
                        reportTitle = UI.util.getSolrResponseTitle(reportData);

                        initializeDisplayFieldVars();
                        initializeHeaderText();
                        initializeNamesToDisplayNames();
                        initializeFilters();
                        initializeDateRangeFilters();
                        search(0);
                        getMetrics();
                    }
                });

                initPaginator();

                Event.addListener(applyFiltersButtonElId, 'click', function(e) {
                    Event.stopEvent(e);
                    search(0);
                    getMetrics();
                });

                Event.addListener(exportButtonElId, "click", function(e) {
                    Event.stopEvent(e);
                    window.open(exportUrl + constructRequestString(0, numFound, true, true));
                    window.focus();
                });
            });

            function initializeHeaderText() {
                Dom.get(headerElId).innerHTML = reportTitle + ' : Collection ' + collection;
            }

            function initializeNamesToDisplayNames() {
                var a = Json.parse(UI.util.getSolrResponseNamesToDisplayNamesFilters(reportData));
                for(var i = 0; i < a.length; i++) {
                    namesToDisplayNames[a[i]['name']] = a[i]['displayname'];
                }
            }

            function initializeDisplayFieldVars() {
                displayFields = UI.util.getSolrResponseDisplayFieldFilters(reportData);

                for(var i = 0; i < displayFields.length; i++) {
                    dataSourceFields.push({ key: displayFields[i], parser : 'text'});
                    columnDefs.push({ key : displayFields[i], label : displayFields[i], sortable : true, formatter: UI.formatString });
                }
            }

            function initializeDateRangeFilters() {
                Connect.asyncRequest('GET', dateRangeUrl, {
                    success: function(o) {
                        dateRangeObject = UI.util.getSolrResponse(Json.parse(o.responseText));
                        $(function() {
                            $("#" + dateFilterFromElId).datepicker( {dateFormat : "M-dd-yy"});
                            $("#" + dateFilterToElId).datepicker( {dateFormat : "M-dd-yy"});
                        });
                        updateDatePickers();
                    }
                });
            }

            function updateDatePickers() {
                var selField = Dom.get(dateFilterFieldSelectElId).value;
                if (dateRangeObject.hasOwnProperty(selField)) {
                    var start = new Date(dateRangeObject[selField].start),
                        end   = new Date(dateRangeObject[selField].end);
                    $("#" + dateFilterFromElId).datepicker("option", "minDate", start);
                    $("#" + dateFilterFromElId).datepicker("setDate", start);
                    $("#" + dateFilterToElId).datepicker("option", "maxDate", end);
                    $("#" + dateFilterToElId).datepicker("setDate", end);
                }
            }

            function initializeFilters() {
                filters      = UI.util.getSolrResponseFilters(reportData);
                datefilters  = UI.util.getSolrResponseDateFilters(reportData);
                sortfilters  = UI.util.getSolrResponseSortFilters(reportData);
                exportfields = UI.util.getSolrResponseExportFieldFilters(reportData);

                UI.populateSelect(filters,     filterSelectElId,          UI.returnNameFn, UI.returnNameFn);
                UI.populateSelect(datefilters, dateFilterFieldSelectElId, UI.returnNameFn, UI.returnNameFn);
                UI.populateSelect(sortfilters, sortBySelectElId,          UI.returnNameFn, UI.returnNameFn);
                UI.populateSelect(sortorders,  sortOrderSelectElId,       UI.returnNameFn, UI.returnNameFn);

                var idx = sortfilters.indexOf(origSortField);
                Dom.get(sortBySelectElId).selectedIndex = idx > -1 ? idx : 0;
                Dom.get(sortOrderSelectElId).selectedIndex = sortorders.indexOf(origSortOrder);

                Event.addListener(filterSelectElId, 'change', function(e) {
                    UI.populateSelect(facets[this.value], filterSubSelectElId, UI.returnNameFn, UI.returnNameFn);
                });

                Event.addListener(filterButtonElId, 'click', function(e) {
                    Event.stopEvent(e);
                    var filterField = Dom.get(filterSelectElId).value,
                        filterValue = Dom.get(filterSubSelectElId).value.match(/(.*?)\s\(\d+\)/)[1];

                    addFilter(filterField, filterValue, currentFiltersElId, getFilterDivId(filterField, filterValue));
                    //getUpdatedFacetCounts();
                });

                Event.addListener(sortFilterButtonElId, 'click', function(e) {
                    Event.stopEvent(e);
                    var filterField = Dom.get(filterSelectElId).value,
                        filterValue = Dom.get(filterSubSelectElId).value;
                    addFilter(filterField, filterValue, sortFiltersElId, getFilterSortId(filterField));
                });

                Event.addListener(dateFilterButtonElId, 'click', function(e) {
                    Event.stopEvent(e);

                    var filterField = Dom.get(dateFilterFieldSelectElId).value,
                        dateFrom    = getDateStringFromDatePicker($( "#" + dateFilterFromElId).datepicker( "getDate")),
                        dateTo      = getDateStringFromDatePicker($( "#" + dateFilterToElId).datepicker( "getDate")),
                        filterValue = dateFrom + ' to ' + dateTo;

                    addFilter(filterField, dateFrom + ' to ' + dateTo, currentFiltersElId, getFilterDateId(filterField));
                    //getUpdatedFacetCounts();
                });
            }

            /* Functions */
            function getDateStringFromDatePicker(datePickerDate) {
                var date = null;
                if (datePickerDate != null) {
                    date = new Date(datePickerDate);
                } else {
                    var selField = Dom.get(dateFilterFieldSelectElId).value;
                    if (dateRangeObject.hasOwnProperty(selField)) {
                        date = new Date(dateRangeObject[selField].start);
                    }
                }
                return date == null ? '*' : UI.date.getDateString(date.getMonth() + 1, date.getDate(), date.getFullYear());
            }

            function initPaginator() {
                paginator = new Paginator( { rowsPerPage : rowsPerPage, containers : [ paginatorElId ] });
                paginator.subscribe('changeRequest', handlePagination);
            }

            function updatePaginatorAfterSearch() {
                paginator.set('totalRecords', numFound);
                paginator.render();
                paginator.setStartIndex(0);
            }

            function handlePagination(newState) {
                UI.showWait();
                search(newState.records[0]);
                paginator.setState(newState);
            }

            function getSelectedValue(elId) {
                return Dom.get(elId).options[Dom.get(elId).selectedIndex].value;
            }

            function getUpdatedFacetCounts() {
                Connect.asyncRequest('GET', searchUrl + constructRequestString(0, 0, false, false), {
                    success: function(o) {
                        var response = o.responseText;
                        if (response != '') {
                            updateFacets(UI.util.getSolrResponseFacets(Json.parse(o.responseText)));
                        }
                    }
                });
            }

            function updateRowsPerPage() {
                var r = Dom.get(rowsInputDivElId).value, m = r.match(/[0-9]+/);
                if (m != null) {
                    rowsPerPage = parseInt(m[0])
                } else {
                    Dom.get(rowsInputDivElId).value = rowsPerPage;
                }
                paginator.set('rowsPerPage', rowsPerPage);
            }

            function search(start) {
                updateRowsPerPage();

                Connect.asyncRequest('GET', searchUrl + constructRequestString(start, rowsPerPage, true, false), {
                    success: function(o) {
                        UI.hideWait();
                        var result = Json.parse(o.responseText);

                        solrResponse = UI.util.getSolrResponse(result);
                        numFound     = UI.util.getSolrResponseNumFound(result);

                        if (start == 0) {
                            updateFacets(UI.util.getSolrResponseFacets(result));
                            updatePaginatorAfterSearch();
                            updateInfoComponents();
                        }
                        updateDataTable();
                        updateNumFound(start);
                    },
                    failure: function(o) {
                        alert("Could not connect to Solr");
                    }
                });
            }

            function getMetrics() {
                Connect.asyncRequest('GET', metricsUrl + constructRequestString(0, 0, false, false), {
                    success: function(o) {
                        var response = UI.util.getSolrResponse(Json.parse(o.responseText));
                        buildMetricsHtml(response);
                    }
                });
            }

            function addMetricRow(div, values, isHeaderRow) {
                var m = UI.addDomElementChild('div', div, {}, {'class' : metricsRowCSSClass});
                var valueClass = isHeaderRow ? metricsInfoHeaderCSSClass : metricsInfoDivCSSClass;

                UI.addDomElementChild('div', m, { innerHTML : values[0] }, {'class' : metricsFirstColCSSClass });
                for(var i = 1; i < values.length; i++) {
                    var value = values[i];
                    if (!isHeaderRow && (values[i] + '').match(/[0-9]+\.[0-9]+/) != null) {
                        value = parseFloat(value).toFixed(2);
                    }
                    UI.addDomElementChild('div', m, { innerHTML : value }, {'class' : valueClass });
                }
                UI.addClearBothDiv(m);
            }

            function buildMetricsHtml(response) {
                UI.removeDivChildNodes(metricsElId);

                var names = Object.keys(response);
                if (names.length == 0) return;

                var f = UI.addDomElementChild('fieldset', Dom.get(metricsElId), { id : metricsFieldsetElId });
                UI.addDomElementChild('legend', f, { innerHTML: 'Metrics'});

                addMetricRow(f, ['Name', 'Value', 'Max', 'Min', 'StdDev', '# Found', '# Missing'], true);

                for(var i = 0; i < names.length; i++) {
                    var n = response[names[i]];

                    for (var j = 0; j < n.length; j++) {
                        var record = n[j];
                        addMetricRow(f, [record['name'], record['sum'], record['max'], record['min'], record['stddev'],
                            record['count'], record['missing']], false);
                    }
                }
            }

            function updateFacets(f) {
                if (f == '' || f == undefined) return;

                facets = [];
                for(var i = 0; i < f.length; i++) {
                    facets[f[i].name] = f[i].values;
                }

                var idx = Dom.get(filterSelectElId).selectedIndex;
                UI.populateSelect(facets[filters[idx]], filterSubSelectElId, UI.returnNameFn, UI.returnNameFn);
            }

            function updateDataTable() {
                var dataSource = new LocalDataSource(solrResponse, {
                    responseSchema : {
                        resultsList: UI.util.SOLR_RESPONSE_KEYS.docs,
                        fields: dataSourceFields
                    }
                });

                new DataTable(searchResultsElId, columnDefs, dataSource, { width:"100%" });
            }

            function updateNumFound(start) {
                var s = "Found " + numFound + " document" + ((numFound != 1) ? "s" : "");
                if (numFound > 0) {
                    s += ", showing items " + start + " through " + Math.min(start + rowsPerPage - 1, numFound);
                }
                Dom.get(numFoundElId).innerHTML = s;
            }

            function buildHtml() {
                UI.addDomElementChild('h2', contentEl, { id : headerElId });
                UI.addClearBothDiv(contentEl);

                accordionDivEl = UI.addDomElementChild('div', contentEl, { id : accordionDivElId });

                buildInfoHtml();
                buildSearchOptionsHTML();
                buildAppliedFiltersHTML();

                $.fn.togglepanels = function(){
                    return this.each(function(){
                        $(this).addClass("ui-accordion ui-accordion-icons ui-widget ui-helper-reset")
                                .find("h3")
                                .addClass("ui-accordion-header ui-helper-reset ui-state-default ui-corner-top ui-corner-bottom")
                                .hover(function() { $(this).toggleClass("ui-state-hover"); })
                                .prepend('<span class="ui-icon ui-icon-triangle-1-e"></span>')
                                .click(function() {
                                    $(this)
                                            .toggleClass("ui-accordion-header-active ui-state-active ui-state-default ui-corner-bottom")
                                            .find("> .ui-icon").toggleClass("ui-icon-triangle-1-e ui-icon-triangle-1-s").end()
                                            .next().slideToggle();
                                    return false;
                                })
                                .next()
                                .addClass("ui-accordion-content ui-helper-reset ui-widget-content ui-corner-bottom")
                                .hide();
                    });
                };

                $("#" + accordionDivElId ).togglepanels();//{ active: 1, collapsible: true, heightStyle: "content" });

                UI.addDomElementChild('div', contentEl, { id : numFoundElId });
                UI.addDomElementChild('div', contentEl, { id : searchResultsElId });
                UI.addClearBothDiv(contentEl);
                UI.addDomElementChild('div', contentEl, { id: paginatorElId });

                UI.addClearBothDiv(contentEl);
                UI.addDomElementChild('div', contentEl, { id: metricsElId });
                UI.addClearBothDiv(contentEl);
                UI.addDomElementChild('a', contentEl, { id: exportButtonElId, innerHTML: 'Export'}, {'class' : 'button small'});
            }

            function updateInfoComponents() {
                var vals = [decodeURIComponent(requestParams[UI.util.REQUEST_QUERY_KEY]),
                            UI.getRequestParamDisplayString(origFq + reportFq), numFound,
                            UI.getRequestParamDisplayString(reportSortInfo)];
                for(var i = 0; i < infoDivKeys.length; i++) {
                    Dom.get(getInfoComponentId(infoDivKeys[i])).innerHTML = vals[i];
                }
            }

            function addRow(div, key, val1, val2) {
                var valueClass = infoDivValueCSSClass, isHeaderRow = key == 'header';
                var rowCSSClass = infoRowCSSClass;
                if (isHeaderRow) {
                    key = '&nbsp';
                    valueClass += ' ' + infoDivHeaderCSSClass;
                    rowCSSClass += ' ' + infoRowHdrCSSClass;
                }

                var m = UI.addDomElementChild('div', div, {}, { 'class' : rowCSSClass });
                UI.addDomElementChild('div', m, { innerHTML : key },  {'class' : infoDivCSSClass });
                UI.addDomElementChild('div', m, { innerHTML : val1 }, {'class' : valueClass});
                UI.addDomElementChild('div', m, { innerHTML : val2, id : getInfoComponentId(key) }, {'class' : valueClass});
                UI.addClearBothDiv(m);
            }

            function getInfoComponentId(r) { return 'info_' + r; }

            function buildInfoHtml() {
                UI.addDomElementChild('h3', accordionDivEl, { innerHTML: "Query information" },
                                        { 'class' : accordionHdrCSSClass });
                var infoEl = UI.addDomElementChild('div', accordionDivEl, { id : infoElId });
                var div    = UI.addDomElementChild('div', infoEl);
                addRow(div, 'header', 'Original search', 'New filters');

                for(var i = 0; i < infoDivKeys.length - 1; i++) {
                    var key = infoDivKeys[i];
                    addRow(div, key, UI.getRequestParamDisplayString(requestParams[key]), '');
                }
                addRow(div, UI.util.REQUEST_SORT_KEY, origSortField + ' ' + origSortOrder, '');
                UI.addClearBothDiv(contentEl);
            }

            function buildSearchOptionsHTML() {
                function buildFilterHTML(filterLabelText, select1LabelText, select2LabelText, filterElId, filterSelectElId,
                                         filterSubSelectElId, filterAddButtonElId) {
                    var f = UI.addDomElementChild('div', filterEl);
                    UI.addDomElementChild('span', f, { innerHTML : filterLabelText });
                    UI.addDomElementChild('label', f, { for : filterSelectElId, innerHTML : select1LabelText });
                    UI.addDomElementChild('select', f, { id : filterSelectElId });

                    UI.addDomElementChild('label', f, { for : filterSubSelectElId, innerHTML : select2LabelText });
                    UI.addDomElementChild('select', f, { id : filterSubSelectElId });
                    UI.addDomElementChild('a', f, { id : filterAddButtonElId, href : '#', innerHTML : 'Add' },
                            { 'class' : 'button small'});
                    UI.addClearBothDiv(f);
                }

                UI.addDomElementChild('h3', accordionDivEl, { innerHTML: "Search options" }, { 'class' : accordionHdrCSSClass });
                var filterEl = UI.addDomElementChild('div', accordionDivEl, { id : filterDivElId });

                var f = UI.addDomElementChild('div', filterEl);
                UI.addDomElementChild('span', f, { innerHTML : 'Rows' });
                UI.addDomElementChild('input', f, { type : 'text', id : rowsInputDivElId, value: rowsPerPage });
                UI.addClearBothDiv(f);

                buildFilterHTML('Filters', 'Field name ', 'Value (#)', filterDivElId, filterSelectElId,
                                filterSubSelectElId, filterButtonElId);
                buildFilterHTML('Sort options', 'Sort by field ', 'Order', sortFilterElId, sortBySelectElId,
                                sortOrderSelectElId, sortFilterButtonElId);
                buildDateFilterHTML();
            }

            function buildAppliedFiltersHTML() {
                UI.addDomElementChild('h3', accordionDivEl, { innerHTML: "Search" }, { 'class' : accordionHdrCSSClass });
                var f = UI.addDomElementChild('div', accordionDivEl, { id : appliedFiltersElId });

                var div = UI.addDomElementChild('div', f, { id : currentFiltersContainerElId });
                UI.addDomElementChild('span', div, { innerHTML: 'Data Filters'});
                UI.addDomElementChild('div', div, { id : currentFiltersElId });
                UI.addClearBothDiv(div);

                div = UI.addDomElementChild('div', f, { id : sortFiltersContainerElId });
                UI.addDomElementChild('span', div, { innerHTML: 'Sort Filters'});
                UI.addDomElementChild('div', div, { id : sortFiltersElId });
                UI.addClearBothDiv(div);
                UI.addClearBothDiv(f);

                UI.addDomElementChild('a', f, { id: applyFiltersButtonElId, innerHTML: 'Apply'},
                        {'class' : buttonEnabledCSSClass });
            }

            function buildDateFilterHTML() {
                var filterEl = UI.addDomElementChild('div', Dom.get(filterDivElId), { id : dateFieldFilterElId });
                UI.addDomElementChild('span', filterEl, { innerHTML : 'Date filter' });
                UI.addDomElementChild('label', filterEl, { for : dateFilterFieldSelectElId, innerHTML : 'Field name' });
                UI.addDomElementChild('select', filterEl, { id : dateFilterFieldSelectElId });

                var d = UI.addDomElementChild('div', filterEl, { id : dateFilterFromToDivElId });
                var d2 = UI.addDomElementChild('div', d);
                UI.addDomElementChild('label', d2, { for : dateFilterFromElId, innerHTML : 'From'});
                UI.addDomElementChild('input', d2, { id : dateFilterFromElId });
                d2 = UI.addDomElementChild('div', d);
                UI.addDomElementChild('label', d2, { for : dateFilterToElId, innerHTML : 'to'});
                UI.addDomElementChild('input', d2, { id : dateFilterToElId });
                UI.addClearBothDiv(d);

                UI.addDomElementChild('a', filterEl, { id : dateFilterButtonElId, href : '#', innerHTML : 'Add' },
                        { 'class' : 'button small'});
                UI.addClearBothDiv(filterEl);
            }

            function getFilterDivId(field, val)  { return 'div_' + field + '_val_' + val; }
            function getFilterSortId(field)      { return 'div_' + field + '_val_sort'; }
            function getFilterDateId(field)      { return 'div_' + field + '_val_date'; }
            function isFilterSortId(id)          { return id.endsWith('_sort'); }

            function addFilter(filterField, filterValue, filtersElId, filterDivId) {
                function getFieldFromFilterDivId(id)    { return Dom.get(id).getElementsByTagName('a')[0].innerHTML.split(" : ")[0]; }
                function getFilterAnchorId(field, val)  { return 'a_' + field + '_val_' + val; }

                var filtersEl      = Dom.get(filtersElId),
                    filterAnchorId = getFilterAnchorId(filterField, filterValue);

                if (Dom.inDocument(filterDivId) == false) {
                    var d = UI.addDomElementChild('div', filtersEl, { id : filterDivId });
                    var a = UI.addDomElementChild('a', d, { id: filterAnchorId }, { 'class' : 'button delete' });
                    a.appendChild(document.createTextNode(filterField + ' : ' + filterValue));

                    Event.addListener(filterDivId, 'click', function(e) {
                        UI.removeElement(this.id);

                        /*if (!isFilterSortId(this.id)) {
                            getUpdatedFacetCounts();
                        } */
                    });
                }
            }

            function constructRequestString(start, rows, includeSortKey, isExportRequest) {
                reportFq       = UI.getFilterOptionsQueryString(currentFiltersElId, UI.combineFacetValues, UI.formatFacetString);
                reportSortInfo = UI.getFilterOptionsQueryString(sortFiltersElId, UI.returnValue, UI.formatSortString);

                var addtlParams = [{key : UI.util.REQUEST_START_KEY, value : start},
                                   {key : UI.util.REQUEST_ROWS_KEY,  value : rows}];
                if (includeSortKey) {
                    addtlParams.push({key : UI.util.REQUEST_SORT_KEY, value : reportSortInfo});
                }
                if (origFq != '' || reportFq != '') {
                    addtlParams.push({key : UI.util.REQUEST_FQ_KEY, value : origFq + reportFq});
                }
                if (isExportRequest) {
                    addtlParams.push({key : UI.util.REQUEST_TYPE_KEY, value : 'csv'});
                    addtlParams.push({key : UI.util.REQUEST_FILE_KEY, value : reportTitle + '.csv'});
                    addtlParams.push({key : UI.util.REQUEST_FL_KEY,   value : exportfields.join(",")})
                }

                return UI.util.constructRequestString(requestParams, searchRequestParamKeys, addtlParams);
            }



        })();
    </script>

</layout:main>


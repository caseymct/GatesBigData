var UI = {};
UI.util = {};
UI.date = {};
UI.request = {};
UI.DATEPICK = {};
UI.EXPORT = {};
UI.SEARCH = {};
UI.FACET = {};

(function() {

    var Dom = YAHOO.util.Dom, Event = YAHOO.util.Event, Connect = YAHOO.util.Connect, Json = YAHOO.lang.JSON;

    // Define universal URL keys
    UI.URLS_KEY                       = 'urls';
    UI.QUERY_BUILDER_AC_URL_KEY       = 'queryBuilderAutoCompleteUrl';
    UI.VIEW_DOC_URL_KEY               = 'viewDocUrl';
    UI.LOADING_IMG_URL_KEY            = 'loadingImgUrl';
    UI.THUMBNAIL_URL_KEY              = 'thumbnailUrl';
    UI.FIELD_NAMES_URL_KEY            = 'fieldNamesUrl';
    UI.TABLE_FIELD_NAMES_URL_KEY      = 'tableFieldNamesUrl';
    UI.AUDIT_FIELD_NAMES_URL_KEY      = 'auditFieldNamesUrl';
    UI.FACET_URL_KEY                  = 'facetUrl';
    UI.SEARCH_URL_KEY                 = 'searchUrl';
    UI.SEARCH_BASE_URL_KEY            = 'searchBaseUrl';
    UI.SUGGEST_URL_KEY                = 'suggestUrl';
    UI.DATE_PICKER_URL_KEY            = 'datePickerUrl';
    UI.EXPORT_URL_KEY                 = 'exportUrl';
    UI.JSP_EXPORT_URL_KEY             = 'jspExportUrl';
    UI.ANALYZE_URL_KEY                = 'analyzeUrl';
    UI.REPORTS_URL_KEY                = 'reportsUrl';

    // Define element name keys
    UI.TAB_LIST_EL_NAME_KEY           = 'tabListElName';
    UI.TAB_CONTENT_EL_NAME_KEY        = 'tabContentElName';
    UI.SNIPPET_DATA_INPUT_EL_NAME     = 'snippetData';
    UI.QUERY_DATA_INPUT_EL_NAME       = 'queryData';
    UI.VIEW_DOC_URL_INPUT_EL_NAME     = 'viewDocUrlData';
    UI.VIEW_FIELDS_DATA_INPUT_EL_NAME = 'viewFieldsData';
    UI.DATA_TYPE_DATA_INPUT_EL_NAME   = 'dataTypeData';
    UI.CONTENT_EL_NAME                = 'content';

    // Define keys
    UI.SELECTED_CORE_KEY              = 'selectedCore';
    UI.CORE_NAMES_KEY                 = 'coreNames';
    UI.TAB_DISPLAY_NAMES_KEY          = 'tabDisplayNames';
    UI.DISPLAY_NAME_KEY               = 'displayName';
    UI.DISPLAY_NAMES_KEY              = 'displayNames';
    UI.BUILD_CORE_TAB_HTML_FN_KEY     = 'buildCoreTabHtmlFn';
    UI.DATE_FIELD_KEY                 = 'dateField';
    UI.DATA_TYPE_KEY                  = 'dataType';
    UI.DATA_TYPE_STRUCTURED           = 'structured';
    UI.DATA_TYPE_UNSTRUCTURED         = 'unstructured';
    UI.THUMBNAIL_KEY                  = 'thumbnail';
    UI.THUMBNAIL_TYPE_KEY             = 'thumbnailType';
    UI.INITIAL_QUERY_KEY              = 'query';
    UI.REQUIRED_FILTERS_KEY           = 'requiredfilters';
    UI.OPTIONAL_FILTERS_KEY           = 'optionalfilters';

    UI.FACET.INSERT_FACET_HTML_AFTER_EL_NAME_KEY        = 'insertFacetHtmlAfterElName';
    UI.FACET.UPDATE_FACET_FN                            = 'updateFacetFn';
    UI.FACET.FACET_OPTIONS_DIV_EL_NAME                  = 'facet_options';
    UI.FACET.FACET_CONTAINER_DIV_EL_NAME                = 'facet_container';

    UI.SEARCH.GET_SEARCH_REQ_PARAMS_FN_KEY              = 'searchRequestParamsFn';
    UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY               = 'selectDataColumnDefs';
    UI.SEARCH.DATA_SOURCE_FIELDS_KEY                    = 'dataSourceFields';
    UI.SEARCH.SELECT_DATA_REGEX_IGNORE_KEY              = 'selectDataRegexIgnore';
    UI.SEARCH.SUBMIT_FN_KEY                             = 'submitFn';
    UI.SEARCH.RESET_FN_KEY                              = 'resetFn';
    UI.SEARCH.SEARCH_FN_KEY                             = 'searchFn';
    UI.SEARCH.ACKEYUP_FN_KEY                            = 'acKeyupFn';
    UI.SEARCH.FORMAT_SEARCH_RESULT_FN_KEY               = 'formatSearchResultFn';
    UI.SEARCH.GET_FILTER_QUERY_FN_KEY                   = 'getFilterQueryFn';
    UI.SEARCH.SEARCH_TAB_EL_NAME_KEY                    = 'searchTabElName';
    UI.SEARCH.SEARCH_HEADER_EL_NAME_KEY                 = 'searchHeaderElName';
    UI.SEARCH.SEARCH_INPUT_ELS_KEY                      = 'searchInputEls';
    UI.SEARCH.INITIAL_SELECT_INDEX_KEY                  = 'initialSelectIndex';
    UI.SEARCH.TOOLTIP_EL_NAME_KEY                       = 'toolTipElName';
    UI.SEARCH.INSERT_SEARCH_RESULTS_AFTER_EL_NAME_KEY   = 'insertSearchResultsAfterElName';
    UI.SEARCH.INSERT_SORT_BUTTONS_AFTER_EL_NAME_KEY     = 'insertSortButtonsAfterElName';
    UI.SEARCH.INSERT_SEARCH_BUTTONS_AFTER_EL_NAME_KEY   = 'insertSearchButtonsAfterElName';

    UI.DATEPICK.DATE_FIELD_KEY                          = 'dateField';
    UI.DATEPICK.DATE_PICK_EL_NAME_KEY                   = 'datePickElName';

    UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY             = 'openSeparateExportPage';
    UI.EXPORT.EXPORT_BUTTON_EL_ID_KEY                   = 'exportButtonElId';
    UI.EXPORT.HTML_SIBLING_NAME_KEY                     = 'htmlSiblingElName';

    UI.STRUCTURED_DATA_EL_ID_KEY                        = 'structured';
    UI.UNSTRUCTURED_DATA_EL_ID_KEY                      = 'unstructured';
    UI.INFO_FIELDS_TABLE_FIELDS_KEY                     = 'TABLEFIELDS';
    UI.INFO_FIELDS_WORD_TREE_FIELDS_KEY                 = 'WORDTREEFIELDS';
    UI.INFO_FIELDS_AUDIT_FIELDS_KEY                     = 'AUDITFIELDS';
    UI.INFO_FIELDS_X_AXIS_FIELDS_KEY                    = 'XAXIS';
    UI.INFO_FIELDS_Y_AXIS_FIELDS_KEY                    = 'YAXIS';
    UI.INFO_FIELDS_SERIES_FIELDS_KEY                    = 'SERIES';
    UI.INFO_FIELDS_REPORT_TITLES_FIELDS_KEY             = 'REPORT_TITLES';
    UI.INFO_FIELDS_REPORT_DATA_FIELDS_KEY               = 'REPORT_DATA';

    UI.FACET_FIELDS_DELIMITER_KEY                       = '<field>';
    UI.FACET_VALUES_DELIMITER_KEY                       = '<values>';
    UI.FACET_VALUE_OPTIONS_DELIMITER_KEY                = '<op>';
    UI.FACET_DATE_RANGE_DELIMITER_KEY                   = '<to>';
    UI.SORT_FIELD_DELIMITER_KEY                         = "<sortfield>";
    UI.SORT_ORDER_DELIMITER_KEY                         = "<sortorder>";

    UI.COL_STRING_MAX_CHARS         = 20;
    UI.COL_DATE_STRING_MAX_CHARS    = 30;

    /* UI and Dom functionality */
    UI.getSolrCoreData = function() {
        var dataTypes = [UI.STRUCTURED_DATA_EL_ID_KEY, UI.UNSTRUCTURED_DATA_EL_ID_KEY];
        var i, j, els, href, cores = {};

        for(j = 0; j < dataTypes.length; j++) {
            var dataType = dataTypes[j];
            els = Dom.get(dataType).getElementsByTagName('a');
            cores[dataType] = {};
            cores[dataType][UI.CORE_NAMES_KEY]     = [];
            cores[dataType][UI.DISPLAY_NAMES_KEY]  = [];

            for(i = 0; i < els.length; i++) {
                href = els[i].href.split("/");
                cores[dataType][UI.CORE_NAMES_KEY].push(href[href.length - 1]);
                cores[dataType][UI.DISPLAY_NAMES_KEY].push(els[i].innerHTML);
            }
        }
        return cores;
    };

    UI.getSolrCoresOfSimilarType = function(coreName) {
        var coreData = UI.getSolrCoreData();
        var structured = coreData[UI.STRUCTURED_DATA_EL_ID_KEY][UI.CORE_NAMES_KEY].indexOf(coreName) > -1;
        var ret = {};
        ret[UI.DATA_TYPE_KEY]  = structured ? UI.DATA_TYPE_STRUCTURED : UI.DATA_TYPE_UNSTRUCTURED;
        ret[UI.CORE_NAMES_KEY] = structured ? coreData[UI.STRUCTURED_DATA_EL_ID_KEY] : coreData[UI.UNSTRUCTURED_DATA_EL_ID_KEY];

        return ret;
    };

    UI.defineURLSObject = function(baseUrl, collection, dateField, structured) {
        var urls = {};
        urls[UI.SEARCH_BASE_URL_KEY]        = baseUrl + 'search/';
        urls[UI.FACET_URL_KEY]              = urls[UI.SEARCH_BASE_URL_KEY] + 'solrfacets?collection=' + collection;
        urls[UI.SEARCH_URL_KEY]             = urls[UI.SEARCH_BASE_URL_KEY] + 'solrquery';
        urls[UI.SUGGEST_URL_KEY]            = urls[UI.SEARCH_BASE_URL_KEY] + 'suggest?core=' + collection;
        urls[UI.QUERY_BUILDER_AC_URL_KEY]   = urls[UI.SEARCH_BASE_URL_KEY] + 'fields/all?core=' + collection;
        urls[UI.TABLE_FIELD_NAMES_URL_KEY]  = urls[UI.SEARCH_BASE_URL_KEY] + 'infofields?core=' + collection + '&infofield=' + UI.INFO_FIELDS_TABLE_FIELDS_KEY;
        urls[UI.ANALYZE_URL_KEY]            = urls[UI.SEARCH_BASE_URL_KEY] + 'analyze';
        urls[UI.REPORTS_URL_KEY]            = urls[UI.SEARCH_BASE_URL_KEY] + 'reports';
        urls[UI.DATE_PICKER_URL_KEY]        = baseUrl + 'core/field/daterange?core=' + collection + '&field=' + dateField;
        urls[UI.EXPORT_URL_KEY]             = baseUrl + 'core/export/structured';
        urls[UI.VIEW_DOC_URL_KEY]           = baseUrl + 'core/document/' + (structured ? 'view' : 'prizmview');
        urls[UI.LOADING_IMG_URL_KEY]        = baseUrl + 'static/images/loading.png';
        urls[UI.THUMBNAIL_URL_KEY]          = baseUrl + 'document/thumbnail/get';
        return urls;
    };

    UI.getDataTabViewParams = function(baseUrl, coreName, dateField, columnDefs, dataSourceFields) {
        var coreData   = UI.getSolrCoresOfSimilarType(coreName),
            structured = (coreData[UI.DATA_TYPE_KEY] == UI.DATA_TYPE_STRUCTURED);

        var params = {};
        params[UI.SELECTED_CORE_KEY]                    = coreName;
        params[UI.URLS_KEY]                             = UI.defineURLSObject(baseUrl, coreName, dateField, structured);
        params[UI.DATE_FIELD_KEY]                       = dateField;
        params[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY]   = columnDefs;
        params[UI.SEARCH.DATA_SOURCE_FIELDS_KEY]        = dataSourceFields;
        params[UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY] = structured;
        params[UI.DATA_TYPE_KEY]                        = coreData[UI.DATA_TYPE_KEY];
        params[UI.CORE_NAMES_KEY]                       = coreData[UI.CORE_NAMES_KEY][UI.CORE_NAMES_KEY];
        params[UI.TAB_DISPLAY_NAMES_KEY]                = coreData[UI.CORE_NAMES_KEY][UI.DISPLAY_NAMES_KEY];

        return params;
    };

    UI.returnKey = function(key) { return key; };
    UI.splitOnUnderscoreAndCamelCase = function(key) {
        return key.replace(/_/g, " ")
                  .replace(/\s(.)/g, function($1) { return $1.toUpperCase(); })
                  .replace(/^(.)/, function($1) { return $1.toUpperCase(); })
    };

    UI.removePrefix = function(key) {
        return key.substring(key.indexOf(".")+1)
                  .replace(/([A-Z])/g, ' $1')
                  .replace(/^./, function(str) { return str.toUpperCase(); }).trim();
    };

    UI.changeVisibility = function(elId, isVisible) {
        UI.setStyleOnElementId(elId, 'visibility', isVisible ? 'visible' : 'hidden');
    };

    UI.changeFacetContainerVisibility = function(visibility) {
        //for IE 8
        UI.changeVisibility(UI.FACET.FACET_CONTAINER_DIV_EL_NAME, visibility);
    };

    UI.constructColumnDefs = function(keys, labelFormatterFn) {
        var i, columnDefObj, key, label;
        var columnDefs = [], dataSourceFields = [];

        for(i = 0; i < keys.length; i++) {
            key = keys[i];
            label = labelFormatterFn(key);
            columnDefObj = { key : key, label : label, sortable : true, formatter: UI.formatLink };
            columnDefs.push(columnDefObj);
            dataSourceFields.push( { key : key, parser : 'text' });
        }

        columnDefs.push({ key : UI.THUMBNAIL_KEY,      label : UI.THUMBNAIL_KEY,      hidden: true });
        columnDefs.push({ key : UI.THUMBNAIL_TYPE_KEY, label : UI.THUMBNAIL_TYPE_KEY, hidden: true });

        dataSourceFields.push({ key: 'id',  parser:'text' });
        dataSourceFields.push({ key: 'url', parser:'text' });

        return { columnDefs : columnDefs, dataSourceFields : dataSourceFields };
    };

    UI.initialize = function(coreName, dateField, baseUrl, labelFormatterFn) {
        UI.initWait(baseUrl);

        Event.onContentReady(UI.STRUCTURED_DATA_EL_ID_KEY, function () {
            var coreData   = UI.getSolrCoresOfSimilarType(SEARCH.ui.coreName),
                structured = (coreData[UI.DATA_TYPE_KEY] == UI.DATA_TYPE_STRUCTURED),
                urls       = UI.defineURLSObject(baseUrl, SEARCH.ui.coreName, dateField, structured);

            Connect.asyncRequest('GET', urls[UI.TABLE_FIELD_NAMES_URL_KEY], {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    var tableData = UI.constructColumnDefs(response[UI.INFO_FIELDS_TABLE_FIELDS_KEY], labelFormatterFn);

                    DATA_TABVIEW.init(UI.getDataTabViewParams(baseUrl, SEARCH.ui.coreName, dateField,
                        tableData.columnDefs, tableData.dataSourceFields));
                },
                failure: function(e) {
                    alert('Could not retrieve table names.');
                }
            });
        });
    };

    UI.getExportOptionsParams = function(baseUrl) {
        var params = {};
        params[UI.URLS_KEY] = {};
        params[UI.URLS_KEY][UI.EXPORT_URL_KEY]            = baseUrl + 'export';
        params[UI.URLS_KEY][UI.FIELD_NAMES_URL_KEY]       = baseUrl + 'core/fieldnames';
        params[UI.URLS_KEY][UI.AUDIT_FIELD_NAMES_URL_KEY] = baseUrl + 'search/infofields?infofield=' +
                                                            UI.INFO_FIELDS_AUDIT_FIELDS_KEY + '&core=';
        return params;
    };

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
        var i, keys, el = document.createElement(type);

        if (attributes != undefined && attributes != null) {
            keys = Object.keys(attributes);
            for(i = 0; i < keys.length; i++) {
                el[keys[i]] = attributes[keys[i]];
            }
        }
        if (styles != undefined && styles != null) {
            keys = Object.keys(styles);
            for(i = 0; i < keys.length; i++) {
                if (keys[i] == "class") {
                    Dom.addClass(el, styles[keys[i]]);
                } else {
                    //$(el).css(keys[i], styles[keys[i]]);
                    UI.setStyleOnElement(el, keys[i], styles[keys[i]]);
                }
            }
        }
        return el;
    }

    UI.setStyleOnElementId = function(elId, key, value) {
        $('#' + elId).css(key, value);
    };

    UI.setStyleOnElement = function(el, key, value) {
        $(el).css(key, value);
    };

    if ( !Array.prototype.indexOf ) {
        Array.prototype.indexOf = function(obj, start) {
            for (var i = (start || 0), j = this.length; i < j; i++) {
                if (this[i] === obj) { return i; }
            }
            return -1;
        }
    }

    if ( !Array.prototype.forEach ) {
        Array.prototype.forEach = function(fn, scope) {
            for(var i = 0, len = this.length; i < len; ++i) {
                fn.call(scope, this[i], i, this);
            }
        }
    }

    Object.keys = Object.keys || (function () {
        var hasOwnProperty = Object.prototype.hasOwnProperty,
            hasDontEnumBug = !{toString:null}.propertyIsEnumerable("toString"),
            DontEnums = [
                'toString',
                'toLocaleString',
                'valueOf',
                'hasOwnProperty',
                'isPrototypeOf',
                'propertyIsEnumerable',
                'constructor'
            ],
            DontEnumsLength = DontEnums.length;

        return function (o) {
            if (typeof o != "object" && typeof o != "function" || o === null)
                throw new TypeError("Object.keys called on a non-object");

            var result = [];
            for (var name in o) {
                if (hasOwnProperty.call(o, name))
                    result.push(name);
            }

            if (hasDontEnumBug) {
                for (var i = 0; i < DontEnumsLength; i++) {
                    if (hasOwnProperty.call(o, DontEnums[i]))
                        result.push(DontEnums[i]);
                }
            }

            return result;
        };
    })();

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

    UI.insertDomElementAsFirstChild = function(type, parentNode, attributes, styles) {
        var el = createDomElement(type, attributes, styles);
        parentNode.insertBefore(el, parentNode.firstChild);
        return el;
    };

    UI.addClearBothDiv = function(parent) {
        return UI.addDivWithClass(parent, "clearboth");
    };

    UI.addDivWithClass = function(parent, classname) {
        return UI.addDomElementChild('div', parent, {}, { "class" : classname });
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


    UI.getRequestParamDisplayString = function(s) {
        s = decodeURIComponent(s);
        if (s.indexOf(UI.FACET_FIELDS_DELIMITER_KEY) != -1) {
            s = s.replace(eval('/(.{1})' + UI.FACET_FIELDS_DELIMITER_KEY + '/g'), '$1,<br>')
                    .replace(eval('/' + UI.FACET_FIELDS_DELIMITER_KEY + '/g'), '')
                    .replace(eval('/' + UI.FACET_VALUES_DELIMITER_KEY + '/g'), ' : ')
                    .replace(eval('/' + UI.FACET_DATE_RANGE_DELIMITER_KEY + '/g'), ' to ');

        } else if (s.indexOf(UI.SORT_FIELD_DELIMITER_KEY) != -1) {
            s = s.replace(eval('/(.{1})' + UI.SORT_FIELD_DELIMITER_KEY + '/g'), '$1, ')
                    .replace(eval('/' + UI.SORT_FIELD_DELIMITER_KEY + '/g'), '')
                    .replace(eval('/' + UI.SORT_ORDER_DELIMITER_KEY + '/g'), ' ')
        }
        return s;
    };

    UI.buildInfoHtml = function(infoEl, requestParams) {
        var div = UI.addDomElementChild('div', infoEl);
        var keys = Object.keys(requestParams);

        for(var i = 0; i < keys.length; i++) {
            var key = keys[i], val = UI.getRequestParamDisplayString(requestParams[keys[i]]),
                m = UI.addDomElementChild('div', div);
            UI.addDomElementChild('div', m, { innerHTML : key }, { 'class' : 'info_div'});
            UI.addDomElementChild('div', m, { innerHTML : val }, { 'class' : 'info_div_value'} );
            UI.addClearBothDiv(m);
        }
    };

    UI.hideDlg = function() { this.hide(); };

    UI.createSimpleDlg = function(containerElName, header, body, handleYesFn, handleYesButtonText) {
        if (header == undefined) header = "Warning!";
        if (body == undefined) body = "Are you sure?";
        if (handleYesButtonText == undefined) handleYesButtonText = "Yes";

        var dlg = new YAHOO.widget.SimpleDialog(containerElName, {
            width: "20em",
            fixedcenter: true,
            modal: true,
            visible: false,
            buttons: [
                { text: "Yes", handler: handleYesFn, isDefault:true},
                { text: "Cancel", handler: UI.hideDlg }]
        });

        dlg.render(document.body);
        dlg.setHeader(header);
        if (body != "none") dlg.setBody(body);
        return dlg;
    };

    UI.formatFacetString = function(key, value) {
        if (value.match(eval('/' + UI.FACET_DATE_RANGE_DELIMITER_KEY + '/')) == null) {
            value = value.encodeForRequest();
        }
        return UI.FACET_FIELDS_DELIMITER_KEY + key + UI.FACET_VALUES_DELIMITER_KEY + value;
    };

    UI.formatSortString = function(key, value) {
        return UI.SORT_FIELD_DELIMITER_KEY + key + UI.SORT_ORDER_DELIMITER_KEY + value;
    };

    UI.combineFacetValues = function(f, field, val) {
        if (f.hasOwnProperty(field)) {
            return f[field] + UI.FACET_VALUE_OPTIONS_DELIMITER_KEY + val;
        }
        return val;
    };

    UI.returnValue = function(f, field, val) {
        return val;
    };

    UI.formatFacetField = function(val) {
        var f = UI.date.formatFacetFieldIfDate(val);
        return f.replace(/"/g, '\\"');
    };

    UI.getChildrenByTagName = function(f) {
        return Dom.get(f).getElementsByTagName('a');
    };

    UI.getFilterOptionsQueryString = function(div, combineValuesFn, formatFn) {
        var i, t, filterOptions = {}, filters = UI.getChildrenByTagName(div);

        for (i = 0; i < filters.length; i++) {
            t = filters[i].innerHTML.split(" : ");
            var field = t[0], val = UI.formatFacetField(t[1]);
            filterOptions[field] = combineValuesFn(filterOptions, field, val);
        }

        var fqStr = "";
        var keys = Object.keys(filterOptions);
        for (i = 0; i < keys.length; i++) {
            fqStr += formatFn(keys[i], filterOptions[keys[i]]);
        }

        return fqStr;
    };

    UI.buildTable = function(result, tableName) {
        var keys = Object.keys(result);
        var containerDiv, childDiv, resultsTable = Dom.get(tableName);

        for(var i = 0; i < keys.length; i++) {
            containerDiv = UI.addDomElementChild("div", resultsTable, null, { "class" : "info_details_results_div" });
            childDiv = UI.addDomElementChild("div", containerDiv, { innerHTML : "<b>" + keys[i] + "</b>: " + result[keys[i]] },
                                             { "class" : "info_details_child_div" });

            if (i < keys.length) {
                childDiv = UI.addDomElementChild("div", containerDiv, { innerHTML : "<b>" + keys[++i] + "</b>: " + result[keys[i]] },
                    { "class" : "info_details_child_div" });
            }

            UI.addDomElementChild("div", containerDiv, null, { "class" : "clearboth"});
        }
    };

    UI.returnLongestStringLength = function(keys) {
        var maxchars = -1;
        for(var i = 0; i < keys.length; i++) {
            maxchars = Math.max(keys[i].length, maxchars);
        }
        return maxchars;
    };

    UI.returnDivLengthBasedOnLongestStringLength = function(keys) {
        return UI.returnLongestStringLength(keys) * 10;
    };

    function getTreeDivEntry(width, key, val, level) {
        var color, fontWeight = "normal", fontStyle = "normal", fieldExists = (val.match('Field does not exist') == null);

        if (fieldExists) {
            switch (level) {
                case 0:  color = "#2F4F4F"; break;
                case 1:  color = "#708090"; break;
                case 2:  color = "#528B8B"; break;
                default: color = "#C1CDCD";
            }
            fontWeight = "bold";
        } else {
            color = "red";
            fontStyle = "italic";
        }

        function style(name, val) { return name + ':' + val + ';' }

        var div1style = style('float', 'left') + style('color', color) + style('width', width + 'px'),
            div2style = style('float', 'left') + style('color', color) + style('width', '600px') +
                        style('font-weight', fontWeight) + style('font-style', fontStyle);

        return "<div style='" + div1style + "'>" + key + "</div>" +
               "<div style='" + div2style + "'>" + val + "</div>";
    }

    UI.buildTreeRecurse = function(doc, keyname, parentNode, maxlength, level) {
        var j, isObject = Object.prototype.toString.call(doc).match("Object") != null;
        var name = getTreeDivEntry(maxlength, keyname, isObject ? "" : doc, level);
        var nameNode = new YAHOO.widget.HTMLNode(name, parentNode, false);

        if (!isObject) {
            nameNode.isLeaf = true;
        } else {
            var keys = Object.keys(doc), maxwidth = UI.returnDivLengthBasedOnLongestStringLength(keys);
            for(j = 0; j < keys.length; j++) {
                UI.buildTreeRecurse(doc[keys[j]], keys[j], nameNode, maxwidth, level + 1);
            }
        }
    };

    UI.buildTreeViewFromJson = function(docs, treeView) {
        var i, j, parent, root = treeView.getRoot();
        treeView.removeChildren(root);

        if (!(docs instanceof Array)) {
            docs = [docs];
        }

        for(i = 0; i < docs.length; i++) {
            parent = (docs[i].hasOwnProperty("name")) ? new YAHOO.widget.TextNode(docs[i].name, root, false) : root;
            var keys = Object.keys(docs[i]), maxwidth = UI.returnDivLengthBasedOnLongestStringLength(keys);
            for(j = 0; j < keys.length; j++) {
                UI.buildTreeRecurse(docs[i][keys[j]], keys[j], parent, maxwidth, 0);
            }
        }
        treeView.render();
        treeView.expandAll();
    };

    UI.initWait = function (baseUrl) {
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
            UI.wait.setBody('<img src=\"' + baseUrl + 'static/images/rel_interstitial_loading.gif\"/>');
            UI.wait.render(document.body);
        }
    };

    UI.showWait = function() { UI.wait.show(); };
    UI.hideWait = function() { UI.wait.hide(); };

    UI.getButtonGroupCheckedButtonValue = function(bg) {
        return bg.get('checkedButton').get('value');
    };

    UI.showOverlayWithButton = function(overlay, showOverlayButtonElName, buttonDownOverlayCSSClass, buttonUpOverlayCSSClass) {
        overlay.show();
        Dom.removeClass(showOverlayButtonElName, buttonDownOverlayCSSClass);
        Dom.addClass(showOverlayButtonElName, buttonUpOverlayCSSClass);
    };

    UI.hideOverlayWithButton = function(overlay, showOverlayButtonElName, buttonDownOverlayCSSClass, buttonUpOverlayCSSClass) {
        overlay.hide();
        Dom.addClass(showOverlayButtonElName, buttonDownOverlayCSSClass);
        Dom.removeClass(showOverlayButtonElName, buttonUpOverlayCSSClass);
    };

    /* Formatters for the search results table */
    function formatDataTableStrings(column, data) {
        var max = (column.label.match(/ Date/) != null) ? UI.COL_DATE_STRING_MAX_CHARS : UI.COL_STRING_MAX_CHARS;
        if (data == undefined) {
            data = "<i>No value</i>";
        } else if (data.length > max) {
            data = data.substring(0, max) + "...";
        }
        return data;
    }

    UI.formatString = function(el, record, column, data) {
        el.innerHTML = formatDataTableStrings(column, data);
    };

    UI.formatDate = function(el, record, column, data) {
        data = (data == undefined) ? "<i>No value</i>" : YAHOO.util.Date.format(data, {format: "%I:%M:%S %p %Y-%m-%d %Z"});
        el.innerHTML = '<a href="#">' + data + '</a>';
    };

    UI.formatLink = function(el, record, column, data) {
        el.innerHTML = '<a href="#">' + formatDataTableStrings(column, data) + '</a>';
    };

    UI.returnNameFn = function(n, i)  { return n; };
    UI.returnIndexFn = function(n, i) { return i; };

    UI.populateSelect = function(nodes, selectElId, nameFn, valueFn) {
        var select = Dom.get(selectElId);
        select.options.length = 0;

        for(var i = 0; i < nodes.length; i++) {
            select.options[select.options.length] = new Option(nameFn(nodes[i], i), valueFn(nodes[i], i));
        }
    };

    /* Utility functions */
    UI.util.REQUEST_CORE_KEY        = 'core';
    UI.util.REQUEST_COLLECTION_KEY  = 'collection';
    UI.util.REQUEST_QUERY_KEY       = 'query';
    UI.util.REQUEST_Q_KEY           = 'q';
    UI.util.REQUEST_FQ_KEY          = 'fq';
    UI.util.REQUEST_SORT_KEY        = 'sort';
    UI.util.REQUEST_ORDER_KEY       = 'order';
    UI.util.REQUEST_START_KEY       = 'start';
    UI.util.REQUEST_ROWS_KEY        = 'rows';
    UI.util.REQUEST_NUM_FOUND_KEY   = 'numfound';
    UI.util.REQUEST_INFO_FIELD_KEY  = 'infofield';
    UI.util.REQUEST_TITLE_KEY       = 'title';
    UI.util.REQUEST_FILTER_KEY      = 'filter';
    UI.util.REQUEST_FILE_KEY        = 'file';
    UI.util.REQUEST_FIELDS_KEY      = 'fields';
    UI.util.REQUEST_FL_KEY          = 'fl';
    UI.util.REQUEST_TYPE_KEY        = 'type';

    UI.util.SOLR_RESPONSE_KEYS = { response : 'response', facets : 'facet_counts', docs : 'docs', numfound : 'num_found',
                                   highlighting : 'highlighting', filters : 'filters', datefilters : 'datefilters', title : 'title',
                                   sortfilters : 'sortfilters', display : 'display', namestodisplaynames : 'namestodisplaynames',
                                   exportfields : 'exportfields' };
    UI.util.SOLR_DOC_FIELD_KEYS = { id : 'id', content : 'content', contentType : 'content_type' };

    UI.util.CONTENT_TYPES = { text : 'text/html', image : 'image/png', json : 'application/json' };

    UI.util.getSolrResponse = function(result) {
        return result[UI.util.SOLR_RESPONSE_KEYS.response];
    };

    UI.util.getSolrResponseDocs = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.docs];
    };

    UI.util.getSolrResponseHighlighting = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.highlighting];
    };

    UI.util.getSolrResponseNumFound = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.numfound];
    };

    UI.util.getSolrResponseFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.filters];
    };

    UI.util.getSolrResponseDateFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.datefilters];
    };

    UI.util.getSolrResponseSortFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.sortfilters];
    };

    UI.util.getSolrResponseDisplayFieldFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.display];
    };

    UI.util.getSolrResponseExportFieldFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.exportfields];
    };

    UI.util.getSolrResponseNamesToDisplayNamesFilters = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.namestodisplaynames];
    };

    UI.util.getSolrResponseFacets = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.facets];
    };

    UI.util.getSolrResponseTitle = function(result) {
        return UI.util.getSolrResponse(result)[UI.util.SOLR_RESPONSE_KEYS.title];
    };

    UI.util.stripBrackets = function(s) {
        return (typeof s == "string") ? s.replace(/^\[|\]$/g, '') : s;
    };

    UI.util.getRequestParamDisplayString = function(requestParams, param) {
        switch (param) {
            case UI.util.REQUEST_QUERY_KEY :
            case UI.util.REQUEST_Q_KEY     : return UI.util.getRequestQueryParam(requestParams); break;
            case UI.util.REQUEST_FQ_KEY    : return UI.util.getRequestFilterQueryParam(requestParams); break;
            case UI.util.REQUEST_SORT_KEY  : return UI.util.getRequestSortAndOrderParams(requestParams); break;
            default: return UI.util.specifyReturnValueIfUndefined(requestParams[param], '');
        }
    };

    UI.util.getRequestQueryParam = function(requestParams) {
        var q1 = requestParams[UI.util.REQUEST_QUERY_KEY],
            q2 = requestParams[UI.util.REQUEST_Q_KEY];
        return q1 != undefined ? decodeURIComponent(q1) : (q2 != undefined ? decodeURIComponent(q2) : '');
    };

    UI.util.getRequestFilterQueryParam = function(requestParams) {
        var fq = requestParams[UI.util.REQUEST_FQ_KEY];
        if (fq == undefined) return '';

        var s = '', fqList = decodeURIComponent(fq).split(UI.FACET_FIELDS_DELIMITER_KEY);
        for(var i = 0; i < fqList.length; i++) {
            if (fqList[i] == '') continue;

            var fqValList = fqList[i].split(UI.FACET_VALUES_DELIMITER_KEY),
                field = fqValList[0], vals = fqValList[1];

            s += '+' + field + ':';
            if (vals.match(UI.FACET_DATE_RANGE_DELIMITER_KEY) != null) {
                var dates = vals.split(UI.FACET_VALUE_OPTIONS_DELIMITER_KEY);
                for(var j = 0; j < dates.length; j++) {
                    s += '[' + dates[j].replace(eval('/' + UI.FACET_DATE_RANGE_DELIMITER_KEY + '/g'), ' to ') + '] ';
                }
            } else {
                s += '(' + fqValList[1].replace(eval('/' + UI.FACET_VALUE_OPTIONS_DELIMITER_KEY + '/g'), ' ') + ') ';
            }
        }
        return s;
    };

    UI.util.getRequestSortAndOrderParams = function(requestParams) {
        var sort = requestParams[UI.util.REQUEST_SORT_KEY],
            order = requestParams[UI.util.REQUEST_ORDER_KEY];
        return sort != undefined && order != undefined ? sort + ' ' + order : '';
    };

    UI.util.getRequestParameters = function() {
        var href = window.location.href;
        var params = href.substring(href.indexOf("?")+1).split("&");
        var i, s, r = {};

        for(i = 0; i < params.length; i++) {
            s = params[i].split("=")[0];
            r[s] = YAHOO.deconcept.util.getRequestParameter(s);
        }

        // TODO: remove
        if (r[UI.util.REQUEST_CORE_KEY] != undefined && r[UI.util.REQUEST_COLLECTION_KEY] == undefined) {
            r[UI.util.REQUEST_COLLECTION_KEY] = r[UI.util.REQUEST_CORE_KEY];
        }
        return r;
    };

    UI.util.constructRequestString = function(requestParams, requestParamKeys, extraParams) {
        var i, rp, rk, p = [];
        if (requestParamKeys == undefined || requestParamKeys.length == 0) {
            requestParamKeys = Object.keys(requestParams);
        }
        for(i = 0; i < requestParamKeys.length; i++) {
            rk = requestParamKeys[i];
            rp = requestParams[rk];

            if (rp == undefined) continue;
            p.push(rk + "=" + rp);
        }

        if (extraParams != undefined) {
            for(i = 0; i < extraParams.length; i++) {
                p.push(extraParams[i]['key'] + "=" + extraParams[i]['value']);
            }
        }
        return "?" + p.join("&");
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

    UI.util.getDomElementsFromDomNames = function(names) {
        var els = [];
        for(var i = 0; i < names.length; i++) {
            els.push(Dom.get(names[i]));
        }
        return els;
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

    Array.prototype.applyFn = function(fn) {
        var a = this;
        for(var i = 0; i < a.length; i++) {
            a[i][fn]();
        }
    };

    UI.util.returnIfDefined = function(origval, newval) {
        return (newval != undefined) ? newval : origval;
    };

    UI.util.specifyReturnValueIfUndefined = function(s, retval) {
        return (s == undefined) ? retval : s;
    };

    UI.util.returnEmptyIfUndefined = function(s) {
        return (s == undefined) ? '' : s;
    };

    UI.util.getNPixels = function(pxStr) {
        return parseInt(pxStr.replace("px", ""));
    };

    UI.util.jsonSyntaxHighlight = function (json) {
        json = YAHOO.lang.JSON.stringify(json, undefined, 3);

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

    UI.util.lengthOfLongestStringInArray = function(arr) {
        return arr.sort(function(a, b) { return b.length - a.length; })[0].length;
    };

    UI.util.clone = function(obj) {
        // Handle the 3 simple types, and null or undefined
        if (obj == null || "object" != typeof obj) return obj;

        if (obj instanceof Date) {
            var dateCopy = new Date();
            dateCopy.setTime(obj.getTime());
            return dateCopy;
        }

        if (obj instanceof Array) {
            var arrCopy = [];
            for (var i = 0, len = obj.length; i < len; i++) {
                arrCopy[i] = UI.util.clone(obj[i]);
            }
            return arrCopy;
        }

        if (obj instanceof Object) {
            var copy = {};
            for (var attr in obj) {
                if (obj.hasOwnProperty(attr)) copy[attr] = UI.util.clone(obj[attr]);
            }
            return copy;
        }

        throw new Error("Unable to copy obj! Its type isn't supported.");
    };

    /* Date functions
     */
    UI.date.MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

    function checkMonth(m)  { return UI.date.MONTHS.indexOf((m + '').toUpperCase()) != -1 || (!isNaN(m) && m >= 1 && m <= 12); }
    function checkDay(d)    { return !isNaN(d) && d >= 1 && d <= 31; }
    function checkYear(y)   { return !isNaN(y) && y >= 1900; }

    function getMonthString(month) {
        var m = UI.date.MONTHS.indexOf((month + '').toUpperCase());
        return UI.date.MONTHS[(m != -1) ? m : parseInt(month) - 1];
    }

    UI.date.getDateString = function(m, d, y) {
        return checkMonth(m) && checkDay(d) && checkYear(y) ? getMonthString(m) + '-' + d + '-' + y : null;
    };

    UI.date.formatFacetFieldIfDate = function(fieldVal) {
        var f = fieldVal.match(/(\w{3}-\d{1,2}-\d{4}) to (\w{3}-\d{1,2}-\d{4})/);
        return f == null ? fieldVal : f[1] + UI.FACET_DATE_RANGE_DELIMITER_KEY + f[2];
    };
})();
   
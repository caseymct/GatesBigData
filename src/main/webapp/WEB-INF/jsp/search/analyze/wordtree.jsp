<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/wordtree.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/raphael.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/word-tree.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/word-tree-layout.js"/>" ></script>

    <script type="text/javascript">
    (function() {
        var Dom  = YAHOO.util.Dom,    Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,     Event = YAHOO.util.Event;

        var infoElId             = 'info',              contentEl               = Dom.get(UI.CONTENT_EL_NAME),
            graphElId            = 'graph',             graphContentElId        = 'graph_content',
            queryInfoElId        = 'query_info',        searchClickElId         = 'search_click',
            selectWordTreeElId   = 'select_word_tree',  selectWordTreeDivElId   = 'select_word_tree_div',
            searchInputElId      = 'search_input',      searchResultsElId       = 'search_results',
            searchOnlySelElId    = 'search_onlysel',    recenterClickElId       = 'recenter',
            resetClickElId       = 'reset',
            changeFieldElId      = 'change_field',      searchLblCtrCSSClass    = 'search_label_container_div',
            searchLabelCSSClass  = 'search_label_div',  searchHdrLblCSSClass    = 'search_header_label',
            onlySelLabelCSSClass = 'only_sel_label',    searchResultsLabelElId  = 'search_results_lbl',

            PREFIX_KEY           = 'prefix',            SUFFIX_KEY              = 'suffix',
            ALT_QUERIES_KEY      = 'alternate_queries', CORE_KEY                = 'core',
            ANALYSIS_FIELD_KEY   = 'analysisfield',     FQ_KEY                  = 'fq',
            QUERY_KEY            = 'query',             ROWS_KEY                = 'rows',
            NUM_FOUND_KEY        = 'numfound',          HL_KEY                  = 'hl',
            SENTENCE_KEY         = 'sentence',          COUNT_KEY               = 'count',
            PAPER_HEIGHT         = 2000,                PAPER_WIDTH             = 2000;

        var requestParams       = UI.util.getRequestParameters(),
            structured          = requestParams[UI.STRUCTURED_DATA_EL_ID_KEY] == 'true',
            coreName            = requestParams[CORE_KEY],
            analysisField       = requestParams[ANALYSIS_FIELD_KEY],
            requestFields       = [QUERY_KEY, CORE_KEY, ANALYSIS_FIELD_KEY, FQ_KEY],
            extraFields         = [ { key : HL_KEY, value : !structured }, { key : ROWS_KEY, value : requestParams[NUM_FOUND_KEY] }],
            urlRequestParams    = UI.util.constructRequestString(requestParams, requestFields, extraFields),
            responseObject      = null,
            fields              = [];

        var BASE_URL            = '<c:url value="/" />',
            ANALYZE_ALL_URL     = BASE_URL + 'analyze/snippets/all' + urlRequestParams,
            ANALYZE_FIRST_URL   = BASE_URL + 'analyze/snippets/firstpage' + urlRequestParams,
            SEARCH_URL          = BASE_URL + 'search/' + coreName + '?query=';

        buildHtml();

        Connect.asyncRequest('GET', ANALYZE_ALL_URL, {
            success: function(o) {
                responseObject = Json.parse(o.responseText);
                fields = Object.keys(responseObject).sort();

                UI.populateSelect(fields, selectWordTreeElId, getFieldSelectName, getFieldSelectValue);
            }
        });

        Event.addListener(changeFieldElId, 'click', function(o) {
            Event.stopEvent(o);
            buildWordTreeHtml(Dom.get(selectWordTreeElId).value);
        });

        function buildWordTreeHtml(fieldName) {
            buildResponseHtml(fieldName);

            Event.addListener(searchClickElId, 'click', function(e) {
                Event.stopEvent(e);
                var searchResults = WORDTREE.search(Dom.get(searchInputElId).value, Dom.get(searchOnlySelElId).checked);
                UI.populateSelect(searchResults, searchResultsElId, getSearchSelectName, getSearchSelectValue);
            });

            Event.addListener(searchResultsElId, 'focus', function(e) {
                this.selectedIndex = -1;
            });

            Event.addListener(searchResultsElId, 'change', function(e) {
                var val = this.options[this.selectedIndex].value.match(/(R|L)_(\d+)/);
                WORDTREE.selectNode(val[1], val[2]);
                WORDTREE.recenterOnSelected();
            });

            Event.addListener(recenterClickElId, 'click', function(e) {
                Event.stopEvent(e);
                WORDTREE.recenterOnSelected();
            });

            Event.addListener(resetClickElId, 'click', function(e) {
                Event.stopEvent(e);
                WORDTREE.selectNode('R', 0);
                WORDTREE.recenterOnSelected();
            });

            var paper = Raphael(graphElId, PAPER_WIDTH, PAPER_HEIGHT);
            WORDTREE.makeWordTrees({
                paper         : paper,
                searchUrl     : SEARCH_URL + (structured ? analysisField : fieldName) + ':',
                prefixes      : responseObject[fieldName][PREFIX_KEY],
                suffixes      : responseObject[fieldName][SUFFIX_KEY],
                altQueries    : responseObject[fieldName][ALT_QUERIES_KEY]
            });

            formatSvgBorder();
        }

        function formatSvgBorder() {
            var svg = getSvg();
            Dom.setStyle(svg, "border", "1px solid #97381B");
            Dom.setStyle(svg, "border-radius", "5px");
        }

        function getSvg() {
            return Dom.getChildrenBy(Dom.get(graphElId), function(node) { return node.tagName == "svg"; })[0];
        }

        function getFieldSelectName(field, i) {
            if (structured) {
                var p = responseObject[field][PREFIX_KEY];
                return p[SENTENCE_KEY].split(" ")[0] + ' (' + p[COUNT_KEY] + ')';
            }
            return field;
        }
        function getFieldSelectValue(field, i) { return field; }
        function getSearchSelectName(node, i)  { return node.dsc; }
        function getSearchSelectValue(node, i) { return (node.prefixTree ? 'L' : 'R') + '_' + node.id; }

        function buildResponseHtml(field) {
            var graphContentEl = Dom.get(graphContentElId);
            UI.removeDivChildNodes(graphContentEl);

            var fieldset = UI.addDomElementChild('fieldset', graphContentEl);
            var legend = UI.addDomElementChild('legend', fieldset, { innerHTML: "Sentence results from field " + field });

            var parentDiv = UI.addDomElementChild('div', fieldset);
            var headerDiv = UI.addDomElementChild('div', parentDiv);

            var r = UI.addDomElementChild('div', headerDiv, {}, { 'class' : searchLblCtrCSSClass } );
            UI.addDomElementChild('a', r, { innerHTML: 'Recenter on selected', id : recenterClickElId }, { "class" : 'button small' });
            UI.addDomElementChild('a', r, { innerHTML: 'Reset to original position', id : resetClickElId }, { "class" : 'button small' });

            var searchDiv = UI.addDomElementChild('div', headerDiv);
            var searchInputsDiv = UI.addDomElementChild('div', searchDiv, {}, { "class" : searchLabelCSSClass } );

            var div = UI.addDomElementChild('div', searchInputsDiv);
            UI.addDomElementChild('a', div, { id : searchClickElId, innerHTML: 'Search'}, { 'class' : 'button small'});
            UI.addDomElementChild('input', div, { id : searchInputElId, value : '', size: '20', maxlength : '100'});


            div = UI.addDomElementChild('div', searchInputsDiv);
            UI.addDomElementChild('input', div, { id : searchOnlySelElId, type : 'checkbox' }, { float : 'left'});
            UI.addDomElementChild('label', div, { innerHTML : 'Only children of selected '}, { 'class' : onlySelLabelCSSClass });
            UI.addClearBothDiv(searchInputsDiv);

            var searchResultsDiv = UI.addDomElementChild('div', searchDiv, {}, { "class" : searchLabelCSSClass, width : '500px' } );
            UI.addDomElementChild('label', searchResultsDiv, { id : searchResultsLabelElId, innerHTML : 'Search Results '});
            UI.addDomElementChild('select', searchResultsDiv, { id : searchResultsElId }, { float : 'left'});
            UI.addClearBothDiv(searchResultsDiv);

            UI.addClearBothDiv(headerDiv);
            UI.addDomElementChild('div', parentDiv, { id : graphElId });

            UI.addClearBothDiv(fieldset);
        }

        function buildHtml() {
            var infoEl = UI.addDomElementChild('div', contentEl, { id : infoElId });
            var fieldset = UI.addDomElementChild('fieldset', infoEl);
            var legend = UI.addDomElementChild('legend', fieldset, { innerHTML: "Query information" });
            var div = UI.addDomElementChild('div', fieldset, { id : queryInfoElId });

            Object.keys(requestParams).forEach(function(key) {
                var midDiv = UI.addDomElementChild('div', div, {}, { 'class' : searchLblCtrCSSClass });
                UI.addDomElementChild('span', midDiv, { innerHTML : key });
                UI.addDomElementChild('div',  midDiv, { innerHTML : decodeURIComponent(requestParams[key]) });
                UI.addClearBothDiv(midDiv);
            });

            UI.addClearBothDiv(infoEl);

            var selDiv = UI.addDomElementChild('div', contentEl, { id : selectWordTreeDivElId });
            var t = structured ? 'results for field ' + analysisField + ' starting with word:' : ' field ';
            UI.addDomElementChild('input', selDiv, { type : 'button', value: 'Show', id : changeFieldElId });
            UI.addDomElementChild('label', selDiv, { innerHTML : t});
            UI.addDomElementChild('select', selDiv, { id : selectWordTreeElId });
            UI.addClearBothDiv(selDiv);

            UI.addDomElementChild('div', contentEl, { id : graphContentElId });
            UI.addClearBothDiv(contentEl);
        }

    })();
</script>
</layout:main>

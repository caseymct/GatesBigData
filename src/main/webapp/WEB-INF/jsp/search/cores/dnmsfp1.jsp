<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/search/search.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/queryBuilder.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/datepick.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/facet.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/searchAutoComplete.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/export.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/search/dataTabview.js" />"></script>

    <script type="text/javascript">
    (function() {

        var dateField = "last_modified",
                core = SEARCH.ui.coreName;

        var coreUrlReqStr  = "core=" + core,
                fieldUrlReqStr = "field=" + dateField,
                searchBaseUrl  = '<c:url value="/search/" />';

        var urls = {};
        urls[UI.SEARCH_BASE_URL_KEY]        = searchBaseUrl;
        urls[UI.DATE_PICKER_URL_KEY]        = '<c:url value="/core/field/daterange?" />' + coreUrlReqStr + "&" + fieldUrlReqStr;
        urls[UI.FACET_URL_KEY]              = searchBaseUrl + "solrfacets?" + coreUrlReqStr;
        urls[UI.SEARCH_URL_KEY]             = searchBaseUrl + "solrquery";
        urls[UI.SUGGEST_URL_KEY]            = searchBaseUrl + "suggest?" + coreUrlReqStr;
        urls[UI.QUERY_BUILDER_AC_URL_KEY]   = searchBaseUrl + "fields/all?" + coreUrlReqStr;
        urls[UI.EXPORT_URL_KEY]             = '<c:url value="/export" />';
        urls[UI.VIEW_DOC_URL_KEY]           = '<c:url value="/core/document/prizmview" />';
        urls[UI.LOADING_IMG_URL_KEY]        = '<c:url value="/static/images/loading.png" />';
        urls[UI.THUMBNAIL_URL_KEY]          = '<c:url value="/document/thumbnail/get" />';

        var columnDefs = [
            {key:'title', label:'Title', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'author', label:'Author', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'last_modified', label:'Last Modified Date', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'creation_date', label:'Creation Date', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'application_name', label:'Application', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'company', label:'Company', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'last_author', label:'Last Author', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'content_type', label:'Content Type', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'thumbnail', label:'thumbnail', hidden: true},
            {key:'thumbnailType', label:'thumbnail', hidden: true}
        ];
        var dataSourceFields = [
            {key:'title', parser:'text'},
            {key:'author', parser:'text'},
            {key:'last_modified', parser:'text'},
            {key:'creation_date', parser:'text'},
            {key:'application_name', parser:'text'},
            {key:'company', parser:'text'},
            {key:'last_author', parser:'text'},
            {key:'content_type', parser:'text'},
            {key:'url', parser:'text'},
            {key:'HDFSKey', parser:'text'},
            {key:'HDFSSegment', parser:'text'}
        ];

        UI.initWait();

        var params = {};
        params[UI.SELECTED_CORE_KEY] = core;
        params[UI.URLS_KEY] = urls;
        params[UI.DATE_FIELD_KEY] = dateField;
        params[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY] = columnDefs;
        params[UI.SEARCH.DATA_SOURCE_FIELDS_KEY] = dataSourceFields;
        params[UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY] = false;
        params[UI.DATA_TYPE_KEY] = UI.DATA_TYPE_UNSTRUCTURED;
        params[UI.CORE_NAMES_KEY] = ['test2_data', 'dnmsfp1'];
        params[UI.TAB_DISPLAY_NAMES_KEY] = ['Unstructured data test 2', 'dnmsfp1 crawl'];
        params[UI.DATA_TYPE_KEY] = UI.DATA_TYPE_UNSTRUCTURED;
        DATA_TABVIEW.init(params);

    })();
    </script>
</layout:main>

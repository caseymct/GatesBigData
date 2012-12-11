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
                fieldUrlReqStr = "field=" + dateField;

            var datePickerUrl     = '<c:url value="/core/field/daterange?" />' + coreUrlReqStr + "&" + fieldUrlReqStr,
                searchBaseUrl     = '<c:url value="/search/" />',
                facetUrl          = searchBaseUrl + "solrfacets?" + coreUrlReqStr,
                searchUrl         = searchBaseUrl + "solrquery",
                suggestUrl        = searchBaseUrl + "suggest?" + coreUrlReqStr,
                queryBuilderACUrl = searchBaseUrl + "fields/all?" + coreUrlReqStr,
                exportUrl         = '<c:url value="/core/export/" />' + core,
                viewDocUrl        = '<c:url value="/core/document/prizmview" />',
                loadingImgUrl     = '<c:url value="/static/images/loading.png" />',
                thumbnailUrl      = '<c:url value="/document/thumbnail/get" />';

            var urls = { datePickerUrl : datePickerUrl, searchBaseUrl : searchBaseUrl, facetUrl : facetUrl, suggestUrl : suggestUrl,
                searchUrl : searchUrl, queryBuilderAutoCompleteUrl : queryBuilderACUrl, exportUrl : exportUrl, viewDocUrl : viewDocUrl,
                loadingImgUrl : loadingImgUrl, thumbnailUrl : thumbnailUrl };

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

            DATA_TABVIEW.init({
                selectedCore : core,
                urls : urls,
                dateField : dateField,
                selectDataColumnDefs : columnDefs,
                dataSourceFields : dataSourceFields,
                openSeparateExportPage: false,
                dataType: "unstructured",
                coreNames: ["test2_data", "test_data"]
            });

        })();
    </script>
</layout:main>
